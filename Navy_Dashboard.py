import streamlit as st
from datetime import datetime, date, time
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import csv

load_dotenv()

st.title("Date Range Filter")

st.set_page_config(page_title="NAVY_DashBoard",page_icon="🚀")


col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", "2025-08-28")
    print(start_date)
with col2:
    start_time = st.time_input("Start Time", "2025-08-28")
    print(start_time)

col3, col4 = st.columns(2)
with col3:
    end_date = st.date_input("End Date", date.today())
    print(end_date)
with col4:
    end_time = st.time_input("End Time", datetime.today())
    print(end_time)


# --- Buttons ---
col5, col6 = st.columns([5,5])
with col5:
    check_btn = st.button("Check Data",use_container_width=True)
with col6:
    fetch_btn = st.button("Fetch Data",use_container_width=True)

st.write("Selected range:",start_date, "→", end_date)



if check_btn or fetch_btn:
    if end_date < start_date:
        st.error("❌ End Date must be greater than or equal to Start Date.")
    else:
        start_datetime = datetime.combine(start_date, start_time)
        end_datetime = datetime.combine(end_date, end_time)
        if start_datetime >= end_datetime:
            st.error("❌ End Date-Time must be greater than Start Date-Time.")
        else:            
            Mongo_uri=os.getenv("MONGO_URL")
            client = MongoClient(Mongo_uri)
            db = client['iotdb']
            collection = db['navy']

            if check_btn:
                query={
                    "timestamp": {"$gte": start_datetime.isoformat(),"$lte": end_datetime.isoformat()},
                     "Genset_Run_SS": {"$gte": 0, "$lte": 2}
                }
                print(query)
                documents = list(collection.find(query))
                if documents:
                    headers = list(documents[0].keys())

                    filename = f"navy_data_Check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        for doc in documents:
                            doc['_id'] = str(doc['_id'])
                            writer.writerow(doc)

                    st.success("✅ Data saved")
                else:
                    st.warning("⚠️ No data found in the specified date range.")

            if fetch_btn:
                query={
                    "timestamp": {"$gte": start_datetime.isoformat(),"$lte": end_datetime.isoformat()},
                }
                documents = list(collection.find(query))
                if documents:
                    headers = list(documents[0].keys())

                    filename = f"navy_data_Fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        for doc in documents:
                            doc['_id'] = str(doc['_id'])
                            writer.writerow(doc)

                    st.success("✅ Data saved")
                else:
                    st.warning("⚠️ No data found in the specified date range.")

            

            

            





