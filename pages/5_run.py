"""
Page 5: Run Migration

Features:
- Full migration pipeline orchestration
- Validation checks with pass/fail/warning display
- Resume from table functionality
- Real-time log viewer
- Migration report generation
"""
import os
import json
import time
from datetime import datetime
import streamlit as st
import pandas as pd

from utils.config import SessionKeys
from utils.db import ConnectionConfig, run_pg_dump
from utils.extraction import ExtractionEngine
from utils.transformation import TransformationEngine
from utils.loader import DataLoader, LOAD_ORDER
from utils.validation import DataValidator, generate_migration_report

# Page config
st.set_page_config(page_title="Run Migration", page_icon="🚀", layout="wide")
st.title("🚀 Run Migration")

# Directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXTRACT_DIR = os.path.join(BASE_DIR, "output", "extract")
TRANSFORM_DIR = os.path.join(BASE_DIR, "output", "transform")
BACKUP_DIR = os.path.join(BASE_DIR, "backups")


def check_prerequisites():
    """Check if all prerequisites are met."""
    issues = []
    
    if "source_config" not in st.session_state:
        issues.append("Source database not connected")
    
    if SessionKeys.SELECTED_USERS not in st.session_state or not st.session_state[SessionKeys.SELECTED_USERS]:
        issues.append("No users selected for migration")
    
    return issues


def render_status_overview():
    """Render current status overview."""
    st.subheader("📊 Migration Status Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        source_connected = "source_config" in st.session_state
        st.metric("Source DB", "✅ Connected" if source_connected else "❌ Not Connected")
    
    with col2:
        users_selected = st.session_state.get(SessionKeys.SELECTED_USERS, [])
        st.metric("Users Selected", len(users_selected))
    
    with col3:
        extracted = st.session_state.get(SessionKeys.EXTRACTED_DATA)
        st.metric("Extracted", f"✅ {extracted.get('timestamp', '')[:8]}" if extracted else "❌ Not Done")
    
    with col4:
        transformed = st.session_state.get(SessionKeys.TRANSFORMED_DATA)
        st.metric("Transformed", f"✅ {transformed.get('timestamp', '')[:8]}" if transformed else "❌ Not Done")


def render_run_options():
    """Render migration run options."""
    st.markdown("---")
    st.subheader("⚙️ Run Options")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        dry_run = st.toggle("🔍 Dry Run Mode", value=True, help="Preview without making changes to target")
    
    with col2:
        backup_before = st.toggle("💾 Backup Before Migration", value=False, help="Create pg_dump before starting")
    
    with col3:
        stop_on_validation_fail = st.toggle("⚠️ Stop on Validation Failure", value=True)
    
    # Resume option
    resume_from = st.selectbox(
        "Resume from step",
        options=["Start Fresh", "Extract", "Transform", "Validate", "Load"],
        index=0,
        help="Skip earlier steps if they've already been completed"
    )
    
    return {
        "dry_run": dry_run,
        "backup_before": backup_before,
        "stop_on_validation_fail": stop_on_validation_fail,
        "resume_from": resume_from,
    }


def render_validation_results(validation_results: Dict):
    """Render validation results."""
    st.subheader("✅ Validation Results")
    
    # Summary metrics
    summary = validation_results.get("summary", {})
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Passed", summary.get("passed", 0), delta=None)
    with col2:
        st.metric("Failed", summary.get("failed", 0), delta=None, delta_color="inverse")
    with col3:
        st.metric("Warnings", summary.get("warnings", 0), delta=None)
    with col4:
        st.metric("Skipped", summary.get("skipped", 0), delta=None)
    
    # Detailed results
    results_data = []
    for result in validation_results.get("results", []):
        status_icon = {
            "pass": "✅",
            "fail": "❌",
            "warning": "⚠️",
            "skipped": "⏭️",
        }.get(result.get("status"), "❓")
        
        results_data.append({
            "Status": status_icon,
            "Check": result.get("name", ""),
            "Message": result.get("message", ""),
        })
    
    st.dataframe(pd.DataFrame(results_data), hide_index=True, use_container_width=True)


def run_full_migration(options: Dict, log_container):
    """
    Run the full migration pipeline.
    
    Args:
        options: Migration options dict
        log_container: Streamlit container for log output
        
    Returns:
        Migration report dict
    """
    start_time = time.time()
    logs = []
    
    def log(message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        logs.append(f"[{timestamp}] {message}")
        log_container.text_area("Migration Log", "\n".join(logs), height=300)
    
    results = {
        "extraction": None,
        "transformation": None,
        "validation": None,
        "load": None,
    }
    
    # Get configuration
    source_config = st.session_state.get("source_config")
    prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
    user_emails = st.session_state.get(SessionKeys.SELECTED_USERS, [])
    filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
    mapping_config = st.session_state.get(SessionKeys.MAPPING_CONFIG, {})
    constant_columns = st.session_state.get("constant_columns", {})
    
    # Backup if requested
    if options["backup_before"] and options["resume_from"] == "Start Fresh":
        log("Creating backup...")
        success, message, _ = run_pg_dump(source_config, BACKUP_DIR)
        if success:
            log(f"✅ Backup created: {message}")
        else:
            log(f"⚠️ Backup failed: {message}")
    
    # Step 1: Extract
    if options["resume_from"] in ["Start Fresh", "Extract"]:
        log("Starting extraction...")
        
        engine = ExtractionEngine(
            config=source_config,
            prefix=prefix,
            output_dir=EXTRACT_DIR,
            progress_callback=lambda t, c, tot: log(f"  Extracting {t}... ({c}/{tot})")
        )
        
        results["extraction"] = engine.run_full_extraction(
            user_emails=user_emails,
            date_from=filters.get("date_from"),
            date_to=filters.get("date_to"),
            max_doc_size=filters.get("max_size")
        )
        
        st.session_state[SessionKeys.EXTRACTED_DATA] = results["extraction"]
        
        if results["extraction"].get("errors"):
            log(f"❌ Extraction failed: {results['extraction']['errors']}")
            return results
        
        log(f"✅ Extraction complete: {sum(results['extraction'].get('summary', {}).values())} total rows")
    else:
        results["extraction"] = st.session_state.get(SessionKeys.EXTRACTED_DATA, {})
        log("⏭️ Skipping extraction (using existing data)")
    
    # Step 2: Transform
    if options["resume_from"] in ["Start Fresh", "Extract", "Transform"]:
        log("Starting transformation...")
        
        engine = TransformationEngine(
            mapping_config=mapping_config,
            input_dir=EXTRACT_DIR,
            output_dir=TRANSFORM_DIR,
            constant_columns=constant_columns,
            progress_callback=lambda t, c, tot: log(f"  Transforming {t}... ({c}/{tot})")
        )
        
        results["transformation"] = engine.run_full_transformation()
        st.session_state[SessionKeys.TRANSFORMED_DATA] = results["transformation"]
        
        if results["transformation"].get("errors"):
            log(f"❌ Transformation failed: {results['transformation']['errors']}")
            return results
        
        log(f"✅ Transformation complete: {sum(results['transformation'].get('summary', {}).values())} total rows")
    else:
        results["transformation"] = st.session_state.get(SessionKeys.TRANSFORMED_DATA, {})
        log("⏭️ Skipping transformation (using existing data)")
    
    # Step 3: Validate
    if options["resume_from"] in ["Start Fresh", "Extract", "Transform", "Validate"]:
        log("Running validation checks...")
        
        validator = DataValidator(EXTRACT_DIR, TRANSFORM_DIR)
        results["validation"] = validator.run_all_validations()
        
        summary = results["validation"].get("summary", {})
        log(f"✅ Validation complete: {summary.get('passed', 0)} passed, {summary.get('failed', 0)} failed, {summary.get('warnings', 0)} warnings")
        
        if results["validation"].get("overall_status") == "fail" and options["stop_on_validation_fail"]:
            log("❌ Stopping due to validation failures")
            return results
    else:
        log("⏭️ Skipping validation")
    
    # Step 4: Load (only if not dry run and target configured)
    target_config = st.session_state.get("target_config")
    
    if target_config and not options["dry_run"]:
        if options["resume_from"] in ["Start Fresh", "Extract", "Transform", "Validate", "Load"]:
            log("Starting data load...")
            
            load_modes = st.session_state.get("load_modes", {name: "truncate" for name in LOAD_ORDER})
            schema_mode = st.session_state.get("target_schema_mode", "schemas")
            
            loader = DataLoader(
                config=target_config,
                input_dir=TRANSFORM_DIR,
                schema_mode=schema_mode,
                progress_callback=lambda t, c, tot, s: log(f"  Loading {t}... ({c}/{tot}) {s}")
            )
            
            results["load"] = loader.load_all(
                load_modes=load_modes,
                dry_run=False,
                strict_mode=True
            )
            
            if results["load"].get("errors"):
                log(f"⚠️ Load completed with errors: {results['load']['errors']}")
            else:
                log(f"✅ Load complete: {results['load']['summary'].get('total_loaded', 0)} rows loaded")
    elif options["dry_run"]:
        log("⏭️ Skipping load (dry run mode)")
    else:
        log("⏭️ Skipping load (target not configured)")
    
    duration = time.time() - start_time
    log(f"\n🎉 Migration completed in {duration:.1f} seconds")
    
    return results


def render_migration_report(report: Dict):
    """Render the migration report."""
    st.markdown("---")
    st.subheader("📋 Migration Report")
    
    # Overall status
    status = report.get("overall_status", "unknown")
    status_colors = {
        "success": "green",
        "partial": "orange",
        "failed": "red",
    }
    st.markdown(f"**Overall Status:** :{status_colors.get(status, 'gray')}[{status.upper()}]")
    
    if report.get("duration_seconds"):
        st.caption(f"Duration: {report['duration_seconds']:.1f} seconds")
    
    # Per-stage summary
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("**Extraction**")
        ext = report.get("extraction", {})
        for table, count in ext.get("tables", {}).items():
            st.caption(f"• {table}: {count}")
    
    with col2:
        st.markdown("**Transformation**")
        trans = report.get("transformation", {})
        for table, count in trans.get("tables", {}).items():
            st.caption(f"• {table}: {count}")
    
    with col3:
        st.markdown("**Validation**")
        val = report.get("validation", {})
        summary = val.get("summary", {})
        st.caption(f"✅ Passed: {summary.get('passed', 0)}")
        st.caption(f"❌ Failed: {summary.get('failed', 0)}")
        st.caption(f"⚠️ Warnings: {summary.get('warnings', 0)}")
    
    with col4:
        st.markdown("**Load**")
        load = report.get("load", {})
        if load:
            summary = load.get("summary", {})
            st.caption(f"Loaded: {summary.get('total_loaded', 0)}")
            st.caption(f"Failed: {summary.get('total_failed', 0)}")
        else:
            st.caption("Not executed")
    
    # Download report
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        report_json = json.dumps(report, indent=2, default=str)
        st.download_button(
            label="📥 Download Report (JSON)",
            data=report_json,
            file_name=f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )
    
    with col2:
        # Generate HTML report
        html_report = f"""
        <html>
        <head><title>Migration Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            .success {{ color: green; }}
            .failed {{ color: red; }}
            .warning {{ color: orange; }}
            table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f4f4f4; }}
        </style>
        </head>
        <body>
        <h1>Migration Report</h1>
        <p>Generated: {report.get('generated_at', 'N/A')}</p>
        <p>Status: <span class="{status}">{status.upper()}</span></p>
        <h2>Summary</h2>
        <pre>{json.dumps(report, indent=2, default=str)}</pre>
        </body>
        </html>
        """
        st.download_button(
            label="📥 Download Report (HTML)",
            data=html_report,
            file_name=f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
            mime="text/html"
        )


def main():
    """Main page function."""
    # Check prerequisites
    issues = check_prerequisites()
    if issues:
        st.warning("⚠️ Prerequisites not met:")
        for issue in issues:
            st.error(f"• {issue}")
        st.info("Please complete the earlier steps before running the migration.")
        return
    
    # Status overview
    render_status_overview()
    
    # Run options
    options = render_run_options()
    
    st.markdown("---")
    
    # Individual stage buttons
    st.subheader("🎮 Run Controls")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        run_extract = st.button("📥 Extract Only", use_container_width=True)
    with col2:
        run_transform = st.button("🔄 Transform Only", use_container_width=True)
    with col3:
        run_validate = st.button("✅ Validate Only", use_container_width=True)
    with col4:
        run_load = st.button("📤 Load Only", use_container_width=True, disabled=options["dry_run"])
    with col5:
        pass
    
    # Full migration button
    st.markdown("")
    run_full = st.button("🚀 Run Full Migration", type="primary", use_container_width=True)
    
    # Log container
    log_container = st.empty()
    
    # Execute based on button clicked
    if run_full:
        with st.spinner("Running migration..."):
            results = run_full_migration(options, log_container)
        
        # Show validation results if available
        if results.get("validation"):
            render_validation_results(results["validation"])
        
        # Generate and show report
        report = generate_migration_report(
            extraction_results=results.get("extraction", {}),
            transformation_results=results.get("transformation", {}),
            validation_results=results.get("validation", {}),
            load_results=results.get("load"),
        )
        render_migration_report(report)
    
    elif run_extract:
        source_config = st.session_state.get("source_config")
        prefix = st.session_state.get(SessionKeys.TABLE_PREFIX, "jeen_dev")
        user_emails = st.session_state.get(SessionKeys.SELECTED_USERS, [])
        filters = st.session_state.get(SessionKeys.DOCUMENT_FILTERS, {})
        
        with st.spinner("Extracting..."):
            engine = ExtractionEngine(
                config=source_config,
                prefix=prefix,
                output_dir=EXTRACT_DIR
            )
            results = engine.run_full_extraction(
                user_emails=user_emails,
                date_from=filters.get("date_from"),
                date_to=filters.get("date_to"),
                max_doc_size=filters.get("max_size")
            )
            st.session_state[SessionKeys.EXTRACTED_DATA] = results
        
        st.success(f"✅ Extraction complete: {sum(results.get('summary', {}).values())} rows")
    
    elif run_transform:
        mapping_config = st.session_state.get(SessionKeys.MAPPING_CONFIG, {})
        constant_columns = st.session_state.get("constant_columns", {})
        
        with st.spinner("Transforming..."):
            engine = TransformationEngine(
                mapping_config=mapping_config,
                input_dir=EXTRACT_DIR,
                output_dir=TRANSFORM_DIR,
                constant_columns=constant_columns
            )
            results = engine.run_full_transformation()
            st.session_state[SessionKeys.TRANSFORMED_DATA] = results
        
        st.success(f"✅ Transformation complete: {sum(results.get('summary', {}).values())} rows")
    
    elif run_validate:
        with st.spinner("Validating..."):
            validator = DataValidator(EXTRACT_DIR, TRANSFORM_DIR)
            results = validator.run_all_validations()
        
        render_validation_results(results)
    
    elif run_load and not options["dry_run"]:
        target_config = st.session_state.get("target_config")
        if not target_config:
            st.error("Target database not configured")
        else:
            load_modes = st.session_state.get("load_modes", {name: "truncate" for name in LOAD_ORDER})
            schema_mode = st.session_state.get("target_schema_mode", "schemas")
            
            with st.spinner("Loading..."):
                loader = DataLoader(
                    config=target_config,
                    input_dir=TRANSFORM_DIR,
                    schema_mode=schema_mode
                )
                results = loader.load_all(
                    load_modes=load_modes,
                    dry_run=False,
                    strict_mode=True
                )
            
            st.success(f"✅ Load complete: {results['summary'].get('total_loaded', 0)} rows loaded")


if __name__ == "__main__":
    main()
