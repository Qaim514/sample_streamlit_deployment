import streamlit as st
from datetime import datetime, date, time,timedelta
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import pandas as pd
from typing import Dict, Any

load_dotenv()

# --- MongoDB connection ---
@st.cache_resource
def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URL")
    client = MongoClient(
        mongo_uri,
        maxPoolSize=50,
        serverSelectionTimeoutMS=3000,
        readPreference='secondaryPreferred'
    )
    return client

def fetch_paginated_data(collection, query: Dict[str, Any], skip: int, limit: int):
    cursor = collection.find(query).sort("timestamp", 1).skip(skip).limit(limit).hint([("timestamp", 1)])
    docs = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        docs.append(doc)
    return docs

def get_total_count(collection, query: Dict[str, Any]) -> int:
    return collection.count_documents(query, maxTimeMS=10000)

# --- Streamlit UI ---
st.set_page_config(page_title="Navy_Dashboard", page_icon="⚡", layout="wide")
st.title("⚡ Navy Data Filter")

# Hide Streamlit UI
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}  
    footer {visibility: hidden;}     
    header {visibility: hidden;}     
    a.anchor-link {display: none;}
    </style>
""", unsafe_allow_html=True)

# --- Session state ---
if "current_page" not in st.session_state:
    st.session_state.current_page = 0
if "records_per_page" not in st.session_state:
    st.session_state.records_per_page = 100
if "total_records" not in st.session_state:
    st.session_state.total_records = 0
if "query" not in st.session_state:
    st.session_state.query = None
if "query_executed" not in st.session_state:
    st.session_state.query_executed = False

# --- Date inputs ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    start_date = st.date_input("Start Date", date.today())
with col2:
    start_time = st.time_input("Start Time", time(0, 0),step=timedelta(minutes=5))
with col3:
    end_date = st.date_input("End Date", date.today())  
with col4:
    end_time = st.time_input("End Time", time(23, 59),step=timedelta(minutes=5))

start_datetime = datetime.combine(start_date, start_time)
end_datetime = datetime.combine(end_date, end_time)

if start_datetime >= end_datetime:
    st.error("❌ End date-time must be greater than start date-time")
    st.stop()

# --- Buttons ---
col_a, col_b = st.columns([5, 5])
with col_b:
    if st.button("📊 Fetch Data", width="stretch"):
        st.session_state.query = {
            "timestamp": {
                "$gte": start_datetime.isoformat(),
                "$lt": end_datetime.isoformat()
            }
        }
        st.session_state.current_page = 0
        st.session_state.query_executed = True
        st.session_state.total_records = 0  # reset count
        st.rerun()

with col_a:
    if st.button("🔍 Check Data", width="stretch"):
        st.session_state.query = {
            "timestamp": {
                "$gte": start_datetime.isoformat(),
                "$lt": end_datetime.isoformat()
            },
            "Genset_Run_SS": {"$gte": 1, "$lte": 6}
        }
        st.session_state.current_page = 0
        st.session_state.query_executed = True
        st.session_state.total_records = 0  # reset count
        st.rerun()

# --- Data processing ---
if st.session_state.query_executed and st.session_state.query:
    try:
        client = get_mongo_client()
        db = client["iotdb"]
        collection = db["navy"]

        # Count only once
        if st.session_state.total_records == 0:
            with st.spinner("Counting records..."):
                st.session_state.total_records = get_total_count(collection, st.session_state.query)

        total_records = st.session_state.total_records
        if total_records == 0:
            st.warning("⚠️ No data found in the specified date range")
            st.stop()

        # Pagination
        total_pages = (total_records - 1) // 100 + 1
        current_page = st.session_state.current_page

        st.info(f"📊 Found {total_records:,} records | Page {current_page + 1} of {total_pages}")

        col_prev,col_space, col_info,col_space2, col_next = st.columns([1,2, 4, 2,1])
        with col_prev:
            if st.button("◀ Previous", disabled=(current_page == 0)):
                st.session_state.current_page = max(0, current_page - 1)
                st.rerun()
        with col_info:
            # Jump to page
            target_page = st.number_input( 
                "Go to page",
                min_value=1, 
                max_value=total_pages,
                value=current_page + 1
            ) - 1
            if target_page != current_page:
                st.session_state.current_page = target_page
                st.rerun()
            
        with col_next:
            if st.button("Next ▶", disabled=(current_page >= total_pages - 1)):
                st.session_state.current_page = min(total_pages - 1, current_page + 1)
                st.rerun()

        # Fetch page data
        skip = current_page * 100
        with st.spinner(f"Loading page {current_page + 1}..."):
            start_fetch = datetime.now()
            records = fetch_paginated_data(collection, st.session_state.query, skip, 100)
            fetch_time = (datetime.now() - start_fetch).total_seconds()

        if records:
            st.success(f"✅ Loaded {len(records)} records in {fetch_time:.2f}s")
            df = pd.DataFrame(records)
            st.dataframe(df, width="stretch", height=600)
        else:
            st.warning("⚠️ No records found on this page")

    except Exception as e:
        st.error(f"❌ Database error: {str(e)}")
