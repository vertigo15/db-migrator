#!/usr/bin/env python3
"""
List all agents that use Langflow workflows.

READ-ONLY: this script only runs SELECT queries against the SOURCE database.
It never inserts, updates, deletes, or alters anything.

A "Workflow" agent is a row in public.playground_bot_generator_config whose
    character_prompts->>'model' = 'Workflow'
The Langflow flow(s) it uses are stored inside the agent row itself at:
    character_prompts->'agentFlow'->'flows'  -> array of { "id": <flow uuid>, "name": ... }

The same flow can be referenced by several agents, so the script also reports
which flows are shared by more than one agent.

The universe of all known workflows is taken from
    public.jeen_dev_langflow_user_permissions.flow_permissions[].flowId
(the Langflow flow definitions live in a separate Langflow DB; this permissions
table is the authoritative list of provisioned flows available in this source
DB). A workflow that exists there but is referenced by no agent is "dead weight".

Excluded from the agent output:
  - soft-deleted agents (deleted_at IS NOT NULL)
  - agents tagged 'Workflow' that reference no flow (empty flows array)

Usage:
    python list_workflow_agents.py            # print report + write CSV to output/
    python list_workflow_agents.py --no-csv   # print report only
"""
import os
import sys
import csv
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional; env vars may already be set


def get_source_config():
    """Build the source DB connection kwargs from environment (.env)."""
    return {
        "host": os.getenv("SOURCE_DB_HOST", "localhost"),
        "port": int(os.getenv("SOURCE_DB_PORT", "5432")),
        "dbname": os.getenv("SOURCE_DB_DATABASE", "postgres"),
        "user": os.getenv("SOURCE_DB_USERNAME", ""),
        "password": os.getenv("SOURCE_DB_PASSWORD", ""),
        "sslmode": os.getenv("SOURCE_DB_SSLMODE", "require"),
    }


# One row per (agent, flow). LEFT JOIN LATERAL keeps Workflow agents that have
# NO flow assigned (empty array) so we can flag them.
AGENT_FLOW_QUERY = """
SELECT
    c.bot_id                              AS agent_id,
    c.user_id,
    c.character_prompts->>'title'         AS agent_title,
    f.flow->>'id'                         AS flow_id,
    f.flow->>'name'                       AS flow_name
FROM public.playground_bot_generator_config c
LEFT JOIN LATERAL jsonb_array_elements(c.character_prompts->'agentFlow'->'flows')
        AS f(flow) ON TRUE
WHERE c.character_prompts->>'model' = 'Workflow'
  AND c.deleted_at IS NULL                                          -- exclude soft-deleted agents
  AND COALESCE(jsonb_array_length(c.character_prompts->'agentFlow'->'flows'), 0) > 0  -- must reference >=1 flow
ORDER BY c.bot_id, flow_name;
"""


# Universe of all known/provisioned workflows (distinct flow ids in the Langflow
# permissions table). Used to find workflows that no agent references.
ALL_FLOWS_QUERY = """
SELECT DISTINCT (p->>'flowId') AS flow_id
FROM public.jeen_dev_langflow_user_permissions,
     jsonb_array_elements(flow_permissions) AS p
WHERE NULLIF(p->>'flowId', '') IS NOT NULL;
"""


def main():
    write_csv = "--no-csv" not in sys.argv
    cfg = get_source_config()

    print(f"Connecting to source DB {cfg['user']}@{cfg['host']}:{cfg['port']}/{cfg['dbname']} ...")
    # Read-only connection: open a read-only transaction as an extra safety net.
    conn = psycopg2.connect(**cfg)
    conn.set_session(readonly=True, autocommit=False)
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(AGENT_FLOW_QUERY)
        rows = cur.fetchall()
        cur.execute(ALL_FLOWS_QUERY)
        all_flow_ids = {r["flow_id"] for r in cur.fetchall()}
        cur.close()
    finally:
        conn.rollback()  # nothing to commit; read-only
        conn.close()

    # ---- Aggregate -------------------------------------------------------
    agents = {}                  # agent_id -> {title, user_id, flows: [(id, name)]}
    flow_to_agents = {}          # flow_id  -> {name, agents: set()}

    for r in rows:
        aid = r["agent_id"]
        a = agents.setdefault(aid, {
            "title": r["agent_title"],
            "user_id": r["user_id"],
            "flows": [],
        })
        if r["flow_id"]:
            a["flows"].append((r["flow_id"], r["flow_name"]))
            fa = flow_to_agents.setdefault(r["flow_id"], {"name": r["flow_name"], "agents": set()})
            fa["agents"].add(aid)

    total_agents = len(agents)
    referenced_flow_ids = set(flow_to_agents)
    distinct_flows = len(referenced_flow_ids)
    shared_flows = {fid: v for fid, v in flow_to_agents.items() if len(v["agents"]) > 1}

    # Workflow universe vs. usage.
    total_workflows = len(all_flow_ids)
    unused_flow_ids = all_flow_ids - referenced_flow_ids            # dead weight
    orphan_flow_ids = referenced_flow_ids - all_flow_ids            # referenced but not provisioned

    # ---- Report ----------------------------------------------------------
    line = "=" * 78
    print(f"\n{line}\n1. OVERALL COUNTS\n{line}")
    print(f"Workflows (total provisioned)          : {total_workflows}")
    print(f"Agents that use workflows              : {total_agents}")
    print(f"Workflows in use (referenced by agents): {distinct_flows}")
    print(f"Workflows NOT used by any agent (dead) : {len(unused_flow_ids)}")
    print(f"Referenced flows not in permissions    : {len(orphan_flow_ids)}")

    print(f"\n{line}\nSUMMARY (agent scope)\n{line}")
    print("Scope: model='Workflow', NOT soft-deleted, referencing >=1 Langflow flow.")
    print(f"Agents that actually use a workflow : {total_agents}")
    print(f"Distinct Langflow flows referenced  : {distinct_flows}")
    print(f"Flows shared by >1 agent            : {len(shared_flows)}")

    print(f"\n{line}\nFLOWS SHARED BY MULTIPLE AGENTS (flow -> agents)\n{line}")
    for fid, v in sorted(shared_flows.items(), key=lambda kv: len(kv[1]["agents"]), reverse=True):
        print(f"\n[{len(v['agents'])} agents] flow {fid}  \"{v['name']}\"")
        for aid in sorted(v["agents"]):
            print(f"    - {aid}  ({agents[aid]['title']})")

    print(f"\n{line}\nALL WORKFLOW AGENTS (agent -> flow(s))\n{line}")
    for aid in sorted(agents):
        a = agents[aid]
        flows_str = "; ".join(f"{name} [{fid}]" for fid, name in a["flows"])
        print(f"{aid:24}  {str(a['title'])[:30]:30}  ->  {flows_str}")

    # ---- Clean ID list: agents that actually use a workflow -------------
    active_ids = sorted(agents)
    print(f"\n{line}\nAGENT IDS THAT ACTUALLY USE A WORKFLOW ({len(active_ids)})\n{line}")
    for aid in active_ids:
        print(aid)

    print(f"\nTotal: {len(active_ids)} non-deleted agents reference a Langflow flow.")

    # ---- Dead workflows: provisioned but referenced by no agent ----------
    dead_ids = sorted(unused_flow_ids)
    print(f"\n{line}\nUNUSED / DEAD WORKFLOWS ({len(dead_ids)})\n{line}")
    print("Provisioned in langflow permissions but referenced by no agent.")
    for fid in dead_ids:
        print(fid)

    # ---- CSV -------------------------------------------------------------
    if write_csv:
        out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
        os.makedirs(out_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(out_dir, f"workflow_agents_{ts}.csv")
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["agent_id", "agent_title", "user_id", "flow_id", "flow_name",
                        "agents_sharing_this_flow"])
            for r in rows:
                share = len(flow_to_agents[r["flow_id"]]["agents"]) if r["flow_id"] else 0
                w.writerow([r["agent_id"], r["agent_title"], r["user_id"],
                            r["flow_id"] or "", r["flow_name"] or "", share])
        print(f"\nCSV written: {out_path}")

        # Plain list of agent IDs that actually use a workflow (one per line).
        ids_path = os.path.join(out_dir, f"workflow_agent_ids_{ts}.txt")
        with open(ids_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(active_ids) + "\n")
        print(f"Agent IDs written: {ids_path}")

        # Plain list of unused/dead workflow ids (one per line).
        dead_path = os.path.join(out_dir, f"unused_workflow_ids_{ts}.txt")
        with open(dead_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(dead_ids) + "\n")
        print(f"Unused workflow IDs written: {dead_path}")


if __name__ == "__main__":
    main()
