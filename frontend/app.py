import os

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title='Text-to-SQL Interface', layout='wide')
st.title('Text-to-SQL Interface with Guardrails')

api_url = os.getenv('API_URL', 'http://localhost:8000')

question = st.text_input('Ask a database question', placeholder='e.g. Total sales by department this quarter')
run = st.button('Run Query')

if run and question.strip():
    with st.spinner('Generating SQL and executing...'):
        try:
            response = requests.post(
                f'{api_url}/v1/query',
                json={'question': question},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()

            col1, col2 = st.columns([2, 1])
            with col1:
                st.subheader('Generated SQL')
                st.code(payload.get('sql', ''), language='sql')
            with col2:
                st.subheader('Confidence')
                st.metric('Overall', f"{payload.get('confidence', 0):.2f}")
                st.json(payload.get('signals', {}))

            st.subheader('Results')
            rows = payload.get('results', [])
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
            else:
                st.info('No rows returned.')

            warnings = payload.get('warnings', [])
            if warnings:
                st.subheader('Guardrail Warnings')
                for warning in warnings:
                    st.warning(warning)

        except requests.RequestException as exc:
            st.error(f'API request failed: {exc}')

st.subheader('Query History')
try:
    history_resp = requests.get(f'{api_url}/v1/history', timeout=10)
    history_resp.raise_for_status()
    items = history_resp.json().get('items', [])
    if items:
        st.dataframe(pd.DataFrame(items), use_container_width=True)
    else:
        st.caption('No history yet.')
except requests.RequestException:
    st.caption('History unavailable while API is offline.')
