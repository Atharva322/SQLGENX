import os
from uuid import uuid4

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Text-to-SQL Interface", layout="wide")
st.title("Text-to-SQL Interface with Guardrails and Hallucination Detection")

api_url = os.getenv("API_URL", "http://localhost:8000")

if "session_id" not in st.session_state:
    st.session_state["session_id"] = f"sess_{uuid4().hex[:8]}"
if "last_payload" not in st.session_state:
    st.session_state["last_payload"] = None
if "connection_id" not in st.session_state:
    st.session_state["connection_id"] = "default"

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
