"""
PDF Export for Pre-Migration Audit Reports.
Generates a structured, multi-section PDF from run_full_audit() results.
"""
import io
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    HRFlowable,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# ── Colour palette ──────────────────────────────────────────────────────────
_C_HEADER_BG  = colors.HexColor('#2c3e50')
_C_HEADER_FG  = colors.white
_C_ROW_ODD    = colors.white
_C_ROW_EVEN   = colors.HexColor('#eef2f7')
_C_SECTION_BG = colors.HexColor('#2980b9')
_C_SECTION_FG = colors.white
_C_WARN       = colors.HexColor('#c0392b')
_C_OK         = colors.HexColor('#27ae60')
_C_BORDER     = colors.HexColor('#bdc3c7')
_C_PAGE_BG    = colors.white

_PAGE_W, _PAGE_H = A4                        # 595.27 x 841.89 pt
_MARGIN       = 1.5 * cm
_BODY_WIDTH   = _PAGE_W - 2 * _MARGIN        # ~510 pt ≈ 18 cm


# ── Style helpers ────────────────────────────────────────────────────────────

def _build_styles() -> Dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        'title': ParagraphStyle(
            'DocTitle', parent=base['Normal'],
            fontSize=22, fontName='Helvetica-Bold',
            textColor=_C_HEADER_BG, spaceAfter=4, leading=26,
        ),
        'subtitle': ParagraphStyle(
            'DocSubtitle', parent=base['Normal'],
            fontSize=10, textColor=colors.HexColor('#7f8c8d'), spaceAfter=12,
        ),
        'section': ParagraphStyle(
            'Section', parent=base['Normal'],
            fontSize=12, fontName='Helvetica-Bold',
            textColor=_C_SECTION_FG, backColor=_C_SECTION_BG,
            spaceBefore=14, spaceAfter=6, leftIndent=6, leading=18,
        ),
        'subsection': ParagraphStyle(
            'Subsection', parent=base['Normal'],
            fontSize=10, fontName='Helvetica-Bold',
            textColor=_C_HEADER_BG, spaceBefore=8, spaceAfter=4,
        ),
        'body': ParagraphStyle(
            'Body', parent=base['Normal'],
            fontSize=9, spaceAfter=4,
        ),
        'warn': ParagraphStyle(
            'Warn', parent=base['Normal'],
            fontSize=9, textColor=_C_WARN, spaceAfter=4,
        ),
        'ok': ParagraphStyle(
            'OK', parent=base['Normal'],
            fontSize=9, textColor=_C_OK, spaceAfter=4,
        ),
        'note': ParagraphStyle(
            'Note', parent=base['Normal'],
            fontSize=8, textColor=colors.HexColor('#7f8c8d'),
            spaceAfter=4, leftIndent=8,
        ),
    }


# ── DataFrame → Table ────────────────────────────────────────────────────────

def _df_table(
    df: pd.DataFrame,
    max_rows: int = 40,
    col_widths: Optional[List[float]] = None,
) -> Optional[Tuple[Table, bool]]:
    """
    Convert a DataFrame to a styled reportlab Table.

    Returns (Table, truncated) or None if df is empty.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None

    truncated = len(df) > max_rows
    display = df.head(max_rows).fillna('').astype(str)
    headers = list(display.columns)
    data = [headers] + display.values.tolist()

    n = len(headers)
    if col_widths is None:
        col_widths = [_BODY_WIDTH / n] * n

    t = Table(data, colWidths=col_widths, repeatRows=1)

    style_cmds = [
        # Header
        ('BACKGROUND',    (0, 0), (-1, 0),  _C_HEADER_BG),
        ('TEXTCOLOR',     (0, 0), (-1, 0),  _C_HEADER_FG),
        ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0, 0), (-1, 0),  8),
        # Data rows
        ('FONTNAME',      (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE',      (0, 1), (-1, -1), 7.5),
        # Grid / padding
        ('GRID',          (0, 0), (-1, -1), 0.3, _C_BORDER),
        ('TOPPADDING',    (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('LEFTPADDING',   (0, 0), (-1, -1), 5),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 5),
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('WORDWRAP',      (0, 0), (-1, -1), True),
    ]
    # Alternating row colours
    for i in range(1, len(data)):
        bg = _C_ROW_EVEN if i % 2 == 0 else _C_ROW_ODD
        style_cmds.append(('BACKGROUND', (0, i), (-1, i), bg))

    t.setStyle(TableStyle(style_cmds))
    return t, truncated


# ── Story-building helpers ────────────────────────────────────────────────────

def _section_header(title: str, styles: Dict) -> List:
    """Return flowables for a coloured section banner."""
    return [
        Spacer(1, 0.3 * cm),
        Paragraph(f'  {title}', styles['section']),
    ]


def _subsection_header(title: str, styles: Dict) -> List:
    return [Paragraph(title, styles['subsection'])]


def _add_df(
    story: List,
    label: str,
    df: pd.DataFrame,
    styles: Dict,
    max_rows: int = 40,
) -> None:
    """Append a labelled DataFrame table to the story."""
    result = _df_table(df, max_rows=max_rows)
    if result is None:
        story.append(Paragraph(f'<i>No data for {label}</i>', styles['note']))
        return
    t, truncated = result
    elems = _subsection_header(label, styles) + [t]
    if truncated:
        elems.append(Paragraph(
            f'(showing first {max_rows} of {len(df):,} rows)',
            styles['note'],
        ))
    story.extend(elems)
    story.append(Spacer(1, 0.2 * cm))


def _stat_line(label: str, value: Any, styles: Dict, warn: bool = False) -> Paragraph:
    key = 'warn' if warn else 'body'
    prefix = '⚠ ' if warn else '• '
    return Paragraph(f'{prefix}<b>{label}:</b> {value}', styles[key])


# ── Footer / page number callback ─────────────────────────────────────────────

def _add_page_number(canvas, doc):
    canvas.saveState()
    canvas.setFont('Helvetica', 7)
    canvas.setFillColor(colors.HexColor('#95a5a6'))
    canvas.drawRightString(
        _PAGE_W - _MARGIN, _MARGIN * 0.6,
        f'Page {doc.page}  •  Generated {datetime.now().strftime("%Y-%m-%d %H:%M")}',
    )
    canvas.restoreState()


# ── Main export function ──────────────────────────────────────────────────────

def generate_audit_pdf(
    results: Dict[str, Any],
    prefix: str,
    source_info: str = '',
    table_counts: Optional[List[Dict]] = None,
) -> bytes:
    """
    Build and return a PDF byte-string for the full audit report.

    Args:
        results:      Output of run_full_audit().
        prefix:       Table prefix (e.g. 'jeen_dev').
        source_info:  Human-readable source DB string for the cover page.
        table_counts: Optional list of {'table': str, 'count': int} for Section 1.

    Returns:
        Raw PDF bytes ready for st.download_button.
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=_MARGIN,
        rightMargin=_MARGIN,
        topMargin=_MARGIN,
        bottomMargin=_MARGIN * 1.5,
        title='Pre-Migration Audit Report',
        author='DB Migrator',
    )
    styles = _build_styles()
    story: List = []

    # ── Cover ──────────────────────────────────────────────────────────────
    story.append(Spacer(1, 1 * cm))
    story.append(Paragraph('Pre-Migration Audit Report', styles['title']))
    story.append(Paragraph(
        f'Prefix: <b>{prefix}</b>  |  Source: {source_info or "—"}  |  '
        f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
        styles['subtitle'],
    ))
    story.append(HRFlowable(width='100%', thickness=1.5, color=_C_SECTION_BG, spaceAfter=10))

    # ── Section 1: Overall Counts ──────────────────────────────────────────
    story.extend(_section_header('Section 1: Overall Table Counts', styles))
    if table_counts:
        counts_data = [['Table', 'Row Count']] + [
            [item['table'], f"{item['count']:,}"] for item in table_counts
        ]
        col_w = _BODY_WIDTH / 2
        t = Table(counts_data, colWidths=[col_w, col_w])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0, 0), (-1, 0),  _C_HEADER_BG),
            ('TEXTCOLOR',     (0, 0), (-1, 0),  colors.white),
            ('FONTNAME',      (0, 0), (-1, 0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0, 0), (-1, -1), 9),
            ('GRID',          (0, 0), (-1, -1), 0.3, _C_BORDER),
            ('TOPPADDING',    (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LEFTPADDING',   (0, 0), (-1, -1), 8),
            ('ALIGN',         (1, 1), (1, -1),  'RIGHT'),
        ] + [
            ('BACKGROUND', (0, i), (-1, i), _C_ROW_EVEN if i % 2 == 0 else _C_ROW_ODD)
            for i in range(1, len(counts_data))
        ]))
        story.append(t)
    else:
        story.append(Paragraph('Table counts not available.', styles['note']))
    story.append(Spacer(1, 0.3 * cm))

    # ── Section 2: User Analytics ─────────────────────────────────────────
    story.extend(_section_header('Section 2: User Analytics', styles))
    users = results.get('users', {})
    if 'error' in users:
        story.append(Paragraph(f'Error: {users["error"]}', styles['warn']))
    else:
        _add_df(story, 'Top 10 Users by Chat Activity',   users.get('top_by_logs'),      styles)
        _add_df(story, 'Top 10 Users by Documents',       users.get('top_by_documents'), styles)
        _add_df(story, 'Top 10 Users by Chunks',          users.get('top_by_chunks'),    styles)

        without_email = users.get('without_email', pd.DataFrame())
        if isinstance(without_email, pd.DataFrame) and not without_email.empty:
            story.append(_stat_line('Users without email (will be skipped)',
                                    f'{len(without_email):,}', styles, warn=True))
            _add_df(story, 'Users Without Email', without_email, styles, max_rows=20)
        else:
            story.append(Paragraph('✓ All users have email addresses', styles['ok']))

        collisions = users.get('username_collisions', pd.DataFrame())
        if isinstance(collisions, pd.DataFrame) and not collisions.empty:
            story.append(_stat_line('Username prefix collisions',
                                    f'{len(collisions):,}', styles, warn=True))
            _add_df(story, 'Username Collisions', collisions, styles, max_rows=20)
        else:
            story.append(Paragraph('✓ No username collisions detected', styles['ok']))

    # ── Section 3: Folder Analytics ───────────────────────────────────────
    story.extend(_section_header('Section 3: Folder Analytics', styles))
    folders = results.get('folders', {})
    if 'error' in folders:
        story.append(Paragraph(f'Error: {folders["error"]}', styles['warn']))
    else:
        _add_df(story, 'Folder Hierarchy Depth',    folders.get('hierarchy_depth'),   styles)
        _add_df(story, 'Folder Type Distribution',  folders.get('type_distribution'), styles)
        orphaned_f = folders.get('orphaned', pd.DataFrame())
        if isinstance(orphaned_f, pd.DataFrame) and not orphaned_f.empty:
            story.append(_stat_line('Orphaned folders', f'{len(orphaned_f):,}', styles, warn=True))
            _add_df(story, 'Orphaned Folders', orphaned_f, styles, max_rows=30)
        else:
            story.append(Paragraph('✓ No orphaned folders', styles['ok']))

    # ── Section 4: Document Analytics ────────────────────────────────────
    story.extend(_section_header('Section 4: Document Analytics', styles))
    docs = results.get('documents', {})
    if 'error' in docs:
        story.append(Paragraph(f'Error: {docs["error"]}', styles['warn']))
    else:
        _add_df(story, 'Document Type Distribution',  docs.get('type_distribution'),       styles)
        _add_df(story, 'Problematic Document Types',  docs.get('problematic_types'),        styles)
        _add_df(story, 'Blob Source Distribution',    docs.get('blob_source_distribution'), styles)

        oc = docs.get('orphaned_count', 0)
        mf = docs.get('missing_folders_count', 0)
        story.append(_stat_line('Documents without valid owner',    f'{oc:,}', styles, warn=oc > 0))
        story.append(_stat_line('Documents referencing missing folder', f'{mf:,}', styles, warn=mf > 0))

        dups = docs.get('duplicate_ids', pd.DataFrame())
        if isinstance(dups, pd.DataFrame) and not dups.empty:
            story.append(_stat_line('Duplicate doc_ids', f'{len(dups):,}', styles, warn=True))
            _add_df(story, 'Duplicate doc_ids', dups, styles, max_rows=20)
        else:
            story.append(Paragraph('✓ No duplicate doc_ids', styles['ok']))

    # ── Section 5: Chunks & Embeddings ───────────────────────────────────
    story.extend(_section_header('Section 5: Chunks & Embeddings Analytics', styles))
    chunks = results.get('chunks_embeddings', {})
    if 'error' in chunks:
        story.append(Paragraph(f'Error: {chunks["error"]}', styles['warn']))
    else:
        _add_df(story, 'Top Documents by Chunk Count', chunks.get('per_document'),     styles, max_rows=20)
        _add_df(story, 'Chunk Type Distribution',      chunks.get('type_distribution'), styles)
        _add_df(story, 'Embedding Vector Dimensions',  chunks.get('dimensions'),        styles)
        _add_df(story, 'Embeddings by Model',          chunks.get('by_model'),          styles)

        orphaned_c = chunks.get('orphaned', {})
        oc_cnt = orphaned_c.get('orphaned_chunks', 0) if isinstance(orphaned_c, dict) else 0
        story.append(_stat_line('Orphaned chunks', f'{oc_cnt:,}', styles, warn=oc_cnt > 0))

        we = chunks.get('without_embeddings', 0)
        if we:
            story.append(_stat_line('Chunks with NULL embeddings', f'{we:,}', styles))

    # ── Section 6: Conversation Analytics ────────────────────────────────
    story.extend(_section_header('Section 6: Conversation Analytics', styles))
    convs = results.get('conversations', {})
    if 'error' in convs:
        story.append(Paragraph(f'Error: {convs["error"]}', styles['warn']))
    else:
        _add_df(story, 'Top 10 Users by Conversations',  convs.get('top_users'),          styles)
        _add_df(story, 'Conversation Size Distribution', convs.get('size_distribution'),  styles)
        _add_df(story, 'Model Usage Distribution',       convs.get('model_usage'),         styles)
        _add_df(story, 'Bot / Agent Usage',              convs.get('bot_usage'),           styles, max_rows=20)
        _add_df(story, 'Token Statistics',               convs.get('token_stats'),         styles)

        wu = convs.get('without_user', {})
        if isinstance(wu, dict):
            n = wu.get('logs_without_user', 0)
            story.append(_stat_line('Logs without user_id (skipped)', f'{n:,}', styles, warn=n > 0))

        wc = convs.get('without_chat_id', 0)
        if wc:
            story.append(_stat_line('Logs without chat_id (skipped)', f'{wc:,}', styles, warn=True))

        orphaned_l = convs.get('orphaned', {})
        ol = orphaned_l.get('orphaned_logs', 0) if isinstance(orphaned_l, dict) else 0
        story.append(_stat_line('Orphaned logs (user not in users table)', f'{ol:,}', styles, warn=ol > 0))

    # ── Section 7: Cross-Table Integrity ─────────────────────────────────
    story.extend(_section_header('Section 7: Cross-Table Integrity – Data Loss Risk', styles))
    cross = results.get('cross_table', {})
    if 'error' in cross:
        story.append(Paragraph(f'Error: {cross["error"]}', styles['warn']))
    else:
        risk_df = cross.get('data_loss_risk', pd.DataFrame())
        if isinstance(risk_df, pd.DataFrame) and not risk_df.empty:
            total_risk = risk_df['rows_at_risk'].sum() if 'rows_at_risk' in risk_df.columns else 0
            level = 'warn' if total_risk > 0 else 'ok'
            icon  = '⚠ ' if total_risk > 0 else '✓ '
            story.append(Paragraph(
                f'{icon}Total rows at risk: <b>{total_risk:,}</b>', styles[level]))
            _add_df(story, 'Data Loss Risk Summary', risk_df, styles)
        else:
            story.append(Paragraph('✓ No data loss risk detected', styles['ok']))

        _add_df(story, 'Missing User References by Table', cross.get('missing_users'), styles, max_rows=30)

    # ── Build ─────────────────────────────────────────────────────────────
    doc.build(
        story,
        onFirstPage=_add_page_number,
        onLaterPages=_add_page_number,
    )
    return buf.getvalue()
