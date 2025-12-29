import streamlit as st
import redis
import json
import pandas as pd
import subprocess
import sys
import os
import signal
import time
import datetime

# --- CONFIGURATION ---
st.set_page_config(
    page_title="🛡️ Fraud Command Center",
    layout="wide",
    page_icon="🚨"
)

PID_FILE = "producer.pid"
LOG_FILE = "producer.log"

# --- HELPER FUNCTIONS ---
@st.cache_resource
def get_redis_connection():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=1)
        r.ping()
        return r
    except Exception:
        return None

def is_producer_running():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                pid = int(f.read())
            os.kill(pid, 0)
            return True
        except:
            return False
    return False

def start_producer():
    if is_producer_running():
        st.toast("⚠️ Producer is already running!")
        return
    with open(LOG_FILE, "w") as log:
        # Start producer in background
        process = subprocess.Popen(
            [sys.executable, "python_producer.py"],
            stdout=log, 
            stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )
    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))
    st.toast("🚀 Simulation Started!")
    time.sleep(1)
    st.rerun()

def stop_producer():
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                pid = int(f.read())
            os.kill(pid, signal.SIGTERM)
            st.toast("🛑 Simulation Stopped.")
        except:
            pass
        finally:
            if os.path.exists(PID_FILE):
                os.remove(PID_FILE)
            time.sleep(1)
            st.rerun()

# --- UI SIDEBAR ---
st.sidebar.title("🎮 Control Panel")
if is_producer_running():
    st.sidebar.success("🟢 Producer: **RUNNING**")
    if st.sidebar.button("🛑 Stop Simulation", type="primary"):
        stop_producer()
else:
    st.sidebar.error("🔴 Producer: **STOPPED**")
    if st.sidebar.button("🚀 Start Simulation"):
        start_producer()

st.sidebar.divider()
st.sidebar.write("Debugging Log:")
if os.path.exists(LOG_FILE):
    with open(LOG_FILE, "r") as f:
        st.sidebar.code("".join(f.readlines()[-5:]), language="text")

# --- MAIN PAGE ---
st.title("🚨 Live Fraud Monitor")

r = get_redis_connection()
if not r:
    st.error("❌ Redis is DOWN. Run: `docker start redis`")
    st.stop()

# 1. FETCH DATA
keys = r.keys("fraud:*")
alerts = []
for key in keys:
    try:
        val = r.get(key)
        if val:
            alerts.append(json.loads(val.decode("utf-8")))
    except:
        continue

# 2. CALCULATE METRICS
total_count = len(alerts)
total_amount = sum([item['amount'] for item in alerts])
latest_fraud = alerts[0] if alerts else None

# Sort alerts so newest is always first
if alerts:
    df_temp = pd.DataFrame(alerts)
    if 'timestamp' in df_temp.columns:
        df_temp = df_temp.sort_values(by='timestamp', ascending=False)
        alerts = df_temp.to_dict('records')
        latest_fraud = alerts[0]

# 3. DISPLAY METRICS (4 Columns now)
col1, col2, col3, col4 = st.columns(4)

# Col 1: Total Count
col1.metric("Total Alerts", total_count)

# Col 2: Total Money Lost
col2.metric("Total Loss", f"${total_amount:,.2f}")

# Col 3: Latest Alert
if latest_fraud:
    amt = latest_fraud['amount']
    name = latest_fraud.get('user_name', 'Unknown')
    merchant = latest_fraud.get('merchant', 'Unknown')
    
    col3.metric(
        label="Latest Fraud",
        value=f"${amt:,.2f}",
        delta=f"{name}",
        delta_color="inverse"
    )
else:
    col3.metric("Latest Fraud", "$0.00", "None")

# Col 4: Status
col4.metric("System Status", "Active", delta=datetime.datetime.now().strftime("%H:%M:%S"))

st.divider()

# 4. DISPLAY TABLE
st.subheader("🔴 Recent Alerts")
if alerts:
    df = pd.DataFrame(alerts)
    
    # Define columns (No space in "user_name"!)
    cols_to_show = ["timestamp", "user_name", "user_id", "amount", "merchant", "reason"]
    final_cols = [c for c in cols_to_show if c in df.columns]
    
    st.dataframe(
        df[final_cols].head(20),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No fraud detected yet. Waiting for incoming data...")

# Auto-refresh every 2 seconds
time.sleep(2)
st.rerun()