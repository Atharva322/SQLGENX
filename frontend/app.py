import os
import json
from uuid import uuid4
from pathlib import Path
import sys

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Text-to-SQL Interface", layout="wide")
st.title("Text-to-SQL Interface with Guardrails and Hallucination Detection")

api_url = os.getenv("API_URL", "http://localhost:8000")
project_root = Path(__file__).resolve().parents[1]
eval_snapshot_path = project_root / "evals" / "latest_eval_snapshot.json"

if "session_id" not in st.session_state:
    st.session_state["session_id"] = f"sess_{uuid4().hex[:8]}"
if "last_payload" not in st.session_state:
    st.session_state["last_payload"] = None
if "connection_id" not in st.session_state:
    st.session_state["connection_id"] = "default"
if "eval_snapshot" not in st.session_state:
    st.session_state["eval_snapshot"] = None

st.caption(f"Session: `{st.session_state['session_id']}`")

connection_map: dict[str, str] = {"default": "default"}
try:
    conn_resp = requests.get(f"{api_url}/v1/connections", timeout=10)
    conn_resp.raise_for_status()
    connection_map = conn_resp.json().get("connections", connection_map)
except requests.RequestException:
    pass

connection_ids = list(connection_map.keys()) or ["default"]
default_index = (
    connection_ids.index(st.session_state["connection_id"])
    if st.session_state["connection_id"] in connection_ids
    else 0
)
selected_connection = st.selectbox(
    "Database connection",
    options=connection_ids,
    index=default_index,
    help="Choose a configured connection_id (maps to a database URL on backend).",
)
st.session_state["connection_id"] = selected_connection

question = st.text_input(
    "Ask a natural language database question",
    placeholder="e.g. Total sales by department this quarter",
)

limit = st.number_input("Row limit", min_value=1, max_value=5000, value=1000, step=50)
run = st.button("Generate and Run")

if run and question.strip():
    with st.spinner("Generating SQL, validating, and executing..."):
        try:
            response = requests.post(
                f"{api_url}/v1/query",
                json={
                    "question": question,
                    "connection_id": st.session_state["connection_id"],
                    "session_id": st.session_state["session_id"],
                    "options": {"row_limit": int(limit)},
                },
                timeout=60,
            )
            response.raise_for_status()
            st.session_state["last_payload"] = response.json()
        except requests.RequestException as exc:
            st.error(f"API request failed: {exc}")

payload = st.session_state.get("last_payload")
if payload:
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("Generated SQL")
        st.code(payload.get("sql", ""), language="sql")
        edited_sql = st.text_area(
            "Edit SQL (power users)",
            value=payload.get("sql", ""),
            height=140,
            key="edited_sql",
        )
        rerun = st.button("Run Edited SQL")
        if rerun and edited_sql.strip():
            try:
                rerun_resp = requests.post(
                    f"{api_url}/v1/query",
                    json={
                        "question": question or "User edited SQL",
                        "connection_id": st.session_state["connection_id"],
                        "session_id": st.session_state["session_id"],
                        "sql_override": edited_sql.strip(),
                        "options": {"row_limit": int(limit)},
                    },
                    timeout=60,
                )
                rerun_resp.raise_for_status()
                st.session_state["last_payload"] = rerun_resp.json()
                payload = st.session_state["last_payload"]
            except requests.RequestException as exc:
                st.error(f"Edited SQL run failed: {exc}")

    with col2:
        st.subheader("Confidence")
        st.metric("Overall", f"{payload.get('confidence', 0):.2f}")
        st.json(payload.get("signals", {}))

    with st.expander("Observability (Optional)", expanded=False):
        if st.checkbox(
            "Show per-request latency/token breakdown",
            value=False,
            key="show_observability_panel",
            help="Displays observability for the latest response only; not persisted to history.",
        ):
            execution_meta = payload.get("execution_meta", {}) or {}
            reasoning = payload.get("reasoning", {}) or {}
            token_usage = execution_meta.get("llm_token_usage", {}) or {}
            stage_latencies = execution_meta.get("stage_latencies_ms", {}) or {}

            st.markdown("**LLM Usage**")
            usage_col1, usage_col2, usage_col3, usage_col4 = st.columns(4)
            usage_col1.metric("Prompt Tokens", int(token_usage.get("prompt_tokens", 0) or 0))
            usage_col2.metric(
                "Completion Tokens", int(token_usage.get("completion_tokens", 0) or 0)
            )
            usage_col3.metric("Total Tokens", int(token_usage.get("total_tokens", 0) or 0))
            usage_col4.metric("LLM Calls", int(token_usage.get("calls", 0) or 0))
            st.caption(
                f"Provider: {token_usage.get('provider', 'n/a')} | "
                f"Model: {token_usage.get('model', 'n/a')}"
            )

            st.markdown("**Latency Breakdown**")
            lat_col1, lat_col2, lat_col3 = st.columns(3)
            lat_col1.metric("Total Pipeline", f"{int(stage_latencies.get('total_pipeline_ms', 0) or 0)} ms")
            lat_col2.metric(
                "Generation + Selection",
                f"{int(stage_latencies.get('generation_and_selection_ms', 0) or 0)} ms",
            )
            lat_col3.metric("Execution", f"{int(stage_latencies.get('execute_ms', 0) or 0)} ms")

            st.markdown("**Key Diagnostics**")
            diag_col1, diag_col2, diag_col3 = st.columns(3)
            diag_col1.metric(
                "Failure Class", str(execution_meta.get("failure_classification", "none"))
            )
            diag_col2.metric(
                "Reasoning Strategy", str(reasoning.get("strategy", "single_pass"))
            )
            diag_col3.metric(
                "Selected Candidate", str(reasoning.get("selected_candidate", "primary"))
            )

    st.subheader("Results")
    rows = payload.get("results", [])
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.info("No rows returned.")

    warnings = payload.get("warnings", [])
    if warnings:
        st.subheader("Guardrail / Validation Warnings")
        for warning in warnings:
            st.warning(warning)

    st.subheader("Feedback Loop")
    feedback_col1, feedback_col2 = st.columns(2)
    notes = st.text_input("Optional feedback notes", key="feedback_notes")
    with feedback_col1:
        if st.button("Mark Correct"):
            try:
                fb = requests.post(
                    f"{api_url}/v1/feedback",
                    json={
                        "query_id": payload.get("query_id"),
                        "session_id": payload.get("session_id"),
                        "verdict": "correct",
                        "notes": notes,
                    },
                    timeout=20,
                )
                fb.raise_for_status()
                st.success("Stored as correct. Added to few-shot feedback store.")
            except requests.RequestException as exc:
                st.error(f"Feedback submit failed: {exc}")
    with feedback_col2:
        if st.button("Mark Incorrect"):
            try:
                fb = requests.post(
                    f"{api_url}/v1/feedback",
                    json={
                        "query_id": payload.get("query_id"),
                        "session_id": payload.get("session_id"),
                        "verdict": "incorrect",
                        "notes": notes,
                    },
                    timeout=20,
                )
                fb.raise_for_status()
                st.success("Stored as incorrect. Added to eval feedback cases.")
            except requests.RequestException as exc:
                st.error(f"Feedback submit failed: {exc}")

st.subheader("Session History")
try:
    history_resp = requests.get(
        f"{api_url}/v1/history",
        params={"session_id": st.session_state["session_id"]},
        timeout=20,
    )
    history_resp.raise_for_status()
    items = history_resp.json().get("items", [])
    if items:
        summary = pd.DataFrame(
            [
                {
                    "query_id": item.get("query_id"),
                    "connection_id": item.get("connection_id"),
                    "question": item.get("question"),
                    "confidence": item.get("confidence"),
                    "rows": item.get("execution_meta", {}).get("rows_returned", 0),
                    "feedback": (item.get("feedback") or {}).get("verdict"),
                }
                for item in items
            ]
        )
        st.dataframe(summary, use_container_width=True)

        with st.expander("History Details"):
            for item in items:
                st.markdown(f"**{item.get('query_id')}** - {item.get('question')}")
                st.code(item.get("sql", ""), language="sql")
                res = item.get("results", [])
                if res:
                    st.dataframe(pd.DataFrame(res), use_container_width=True)
                st.caption(f"Warnings: {len(item.get('warnings', []))}")
    else:
        st.caption("No history yet for this session.")
except requests.RequestException:
    st.caption("History unavailable while API is offline.")

st.subheader("Evaluation Snapshot")
eval_col1, eval_col2 = st.columns(2)
with eval_col1:
    run_eval = st.button("Run Quick Eval")
with eval_col2:
    load_eval = st.button("Load Last Snapshot")

if run_eval:
    try:
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from evals.run_evals import run_eval_suite

        with st.spinner("Running retrieval-focused evaluation..."):
            output = run_eval_suite(
                dataset_path=project_root / "evals" / "golden_queries.jsonl",
                limit=20,
                retrieval_only=True,
            )
        st.session_state["eval_snapshot"] = output
        eval_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        eval_snapshot_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
        st.success("Evaluation snapshot updated.")
    except Exception as exc:
        st.error(f"Eval run unavailable in this environment: {exc}")

if load_eval:
    try:
        if eval_snapshot_path.exists():
            st.session_state["eval_snapshot"] = json.loads(
                eval_snapshot_path.read_text(encoding="utf-8")
            )
            st.success("Loaded latest eval snapshot.")
        else:
            st.warning("No saved eval snapshot found yet.")
    except Exception as exc:
        st.error(f"Failed to load eval snapshot: {exc}")

eval_snapshot = st.session_state.get("eval_snapshot")
if eval_snapshot:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("SQL Exact Match", f"{float(eval_snapshot.get('sql_exact_match', 0.0)):.3f}")
    m2.metric("Execution Match", f"{float(eval_snapshot.get('execution_match', 0.0)):.3f}")
    m3.metric(
        "Hallucination Detection",
        f"{float(eval_snapshot.get('hallucination_detection_rate', 0.0)):.3f}",
    )
    m4.metric(
        "Guardrail Effectiveness",
        f"{float(eval_snapshot.get('guardrail_effectiveness', 0.0)):.3f}",
    )

    retrieval_label = next(
        (key for key in eval_snapshot.keys() if key.startswith("schema_recall_at_")),
        "schema_recall_at_k",
    )
    ndcg_label = next(
        (key for key in eval_snapshot.keys() if key.startswith("schema_ndcg_at_")),
        "schema_ndcg_at_k",
    )

    r1, r2, r3 = st.columns(3)
    r1.metric("Schema Recall@K", f"{float(eval_snapshot.get(retrieval_label, 0.0)):.3f}")
    r2.metric("Schema nDCG@K", f"{float(eval_snapshot.get(ndcg_label, 0.0)):.3f}")
    r3.metric("Retrieval Cases", int(eval_snapshot.get("retrieval_eval_cases", 0)))

    st.caption(
        f"Retrieval context available: {bool(eval_snapshot.get('retrieval_context_available', False))} | "
        f"Mode: {'retrieval-only' if bool(eval_snapshot.get('retrieval_only_mode', False)) else 'full'} | "
        f"Cases: {int(eval_snapshot.get('limited_cases', eval_snapshot.get('total_cases', 0)))}"
    )
