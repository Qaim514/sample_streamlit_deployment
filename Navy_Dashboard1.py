import streamlit as st
from datetime import datetime, date, time
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import csv
import io
import pandas as pd
from typing import Dict, Any, Optional

load_dotenv()

# Initialize MongoDB connection with optimizations
@st.cache_resource
def get_mongo_client():
    """Cache MongoDB client to avoid reconnection overhead"""
    mongo_uri = os.getenv("MONGO_URL")
    # Add connection pooling and timeout optimizations
    client = MongoClient(
        mongo_uri,
        maxPoolSize=50,  # Increase connection pool
        serverSelectionTimeoutMS=5000,  # 5 second timeout
        socketTimeoutMS=30000,  # 30 second socket timeout
        connectTimeoutMS=5000,  # 5 second connection timeout
        maxIdleTimeMS=45000,  # Keep connections alive longer
        waitQueueTimeoutMS=5000
    )
    return client

def build_optimized_query(start_datetime: datetime, end_datetime: datetime, is_check: bool) -> Dict[str, Any]:
    """Build optimized MongoDB query with proper indexing hints"""
    query = {
        "timestamp": {
            "$gte": start_datetime.isoformat(), 
            "$lte": end_datetime.isoformat()
        }
    }
    
    if is_check:
        # Combine filters for better index utilization
        query["Genset_Run_SS"] = {"$gte": 0, "$lte": 2}
    
    return query

def get_projection_fields() -> Optional[Dict[str, int]]:
    """Define projection to reduce data transfer - customize based on your needs"""
    # Return None to fetch all fields, or specify fields to reduce network transfer
    # Example: return {"timestamp": 1, "Genset_Run_SS": 1, "field1": 1, "field2": 1}
    return None

def stream_data_to_csv(collection, query: Dict[str, Any], projection: Optional[Dict[str, int]] = None) -> tuple[str, int]:
    """Optimized streaming data extraction with better memory management"""
    buffer = io.StringIO()
    writer = None
    count = 0
    headers_written = False
    
    try:
        # Optimized cursor with larger batch size and projection
        cursor = collection.find(
            query, 
            projection,
            no_cursor_timeout=True
        ).batch_size(10000)  # Increased batch size
        
        # Hint to use timestamp index (adjust index name as needed)
        cursor.hint([("timestamp", 1)])
        
        # Process in chunks for better memory management
        batch = []
        batch_size = 1000
        
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            
            if not headers_written:
                headers = list(doc.keys())
                writer = csv.DictWriter(buffer, fieldnames=headers)
                writer.writeheader()
                headers_written = True
            
            batch.append(doc)
            count += 1
            
            # Write batch when it reaches batch_size
            if len(batch) >= batch_size:
                writer.writerows(batch)
                batch.clear()
        
        # Write remaining batch
        if batch and writer:
            writer.writerows(batch)
        
        cursor.close()
        
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return "", 0
    
    return buffer.getvalue(), count

# --- Streamlit UI ---
st.set_page_config(page_title="NAVY_DashBoard", page_icon="🚀")
st.title("Date Range Filter")

# Add performance mode selection
performance_mode = st.selectbox(
    "Performance Mode",
    ["Standard (Streaming)", "Auto"],
    help="Standard: Better for very large datasets. Fast: Better for medium datasets. Auto: Automatically choose based on estimated size."
)

# --- Inputs ---
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", date.today())
with col2:
    start_time = st.time_input("Start Time", datetime.today())

col3, col4 = st.columns(2)
with col3:
    end_date = st.date_input("End Date", date.today())
with col4:
    end_time = st.time_input("End Time", datetime.today())

col5, col6 = st.columns([5, 5])
with col5:
    check_btn = st.button("Check Data", use_container_width=True)
with col6:
    fetch_btn = st.button("Fetch Data", use_container_width=True)

st.write("Selected range:", start_date, "→", end_date)

# --- Processing ---
if check_btn or fetch_btn:
    if end_date < start_date:
        st.error("❌ End Date must be greater than or equal to Start Date.")
    else:
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        
        if start_datetime >= end_datetime:
            st.error("❌ End Date-Time must be greater than Start Date-Time.")
        else:
            # Show progress
            with st.spinner('Connecting to database...'):
                client = get_mongo_client()
                db = client['iotdb']
                collection = db['navy']
            
            # Build optimized query
            query = build_optimized_query(start_datetime, end_datetime, check_btn)
            projection = get_projection_fields()
            
            # Estimate data size for auto mode
            if performance_mode == "Auto":
                with st.spinner('Estimating data size...'):
                    estimated_count = collection.count_documents(query)
                    use_pandas = estimated_count < 50000  # Use pandas for smaller datasets
            else:
                use_pandas = performance_mode == "Fast (Pandas)"
            
            # Process data
            with st.spinner(f'Processing data using {"Streaming"} mode...'):
                csv_data, count = stream_data_to_csv(collection, query, projection)
            
            if count > 0:
                csv_bytes = csv_data.encode("utf-8")
                filename = f"navy_data_{'Check' if check_btn else 'Fetch'}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

                st.download_button(
                    label=f"📥 Download CSV ({count:,} records)",
                    data=csv_bytes,
                    file_name=filename,
                    mime="text/csv"
                )
                st.success(f"✅ {count:,} records processed successfully")
                    
            else:
                st.warning("⚠️ No data found in the specified date range.")
