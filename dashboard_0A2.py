import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import matplotlib.colors as mcolors
import logging
import json
import time
import sys
from typing import Dict, Any, List, Optional, Tuple
import threading
import queue
import asyncio
import uuid
import warnings
import math

try:
    from streamlit_autorefresh import st_autorefresh
    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False

try:
    from streamlit_echarts import st_echarts
    ECHARTS_AVAILABLE = True
except ImportError:
    ECHARTS_AVAILABLE = False
    st.error("❌ Missing dependency: pip install streamlit-echarts")
    st.stop()

try:
    from pyecharts.commons.utils import JsCode
    PYECHARTS_AVAILABLE = True
except ImportError:
    PYECHARTS_AVAILABLE = False
    st.error("❌ Missing dependency: pip install pyecharts")
    st.stop()

try:
    from ably import AblyRealtime, AblyRest
    ABLY_AVAILABLE = True
except ImportError:
    ABLY_AVAILABLE = False
    st.error("❌ Ably library not available. Please install: pip install ably")
    st.stop()

try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    st.error("❌ Supabase library not available. Please install: pip install supabase")
    st.stop()

warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*tracemalloc.*")

# Configuration
DASHBOARD_ABLY_API_KEY = "DxuYSw.fQHpug:sa4tOcqWDkYBW9ht56s7fT0G091R1fyXQc6mc8WthxQ"
DASHBOARD_CHANNEL_NAME = "telemetry-dashboard-channel"
SUPABASE_URL = "https://dsfmdziehhgmrconjcns.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRzZm1kemllaGhnbXJjb25qY25zIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MDEyOTIsImV4cCI6MjA2NzQ3NzI5Mn0.P41bpLkP0tKpTktLx6hFOnnyrAB9N_yihQP1v6zTRwc"
SUPABASE_TABLE_NAME = "telemetry"

SUPABASE_MAX_ROWS_PER_REQUEST = 1000
MAX_DATAPOINTS_PER_SESSION = 1000000

st.set_page_config(
    page_title="🏎️ Shell Eco-marathon Telemetry Dashboard",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/your-repo",
        "Report a bug": "https://github.com/your-repo/issues",
        "About": "Shell Eco-marathon Telemetry Dashboard",
    },
)

# ----------------------------
# Liquid Glass Grayscale Theme
# ----------------------------
def get_theme_aware_css():
    return """
<style>
:root {
  color-scheme: light dark;
  /* Neutral grayscale palette */
  --brand-1: 0 0% 56%;
  --brand-2: 0 0% 62%;
  --accent-1: 0 0% 52%;

  --primary: hsl(var(--brand-1));
  --accent: hsl(var(--brand-2));
  --ok: hsl(var(--accent-1));

  --bg: Canvas;
  --text: CanvasText;
  --text-muted: color-mix(in oklab, CanvasText 55%, Canvas);
  --text-subtle: color-mix(in oklab, CanvasText 40%, Canvas);

  --border-weak: color-mix(in oklab, CanvasText 8%, Canvas);
  --border: color-mix(in oklab, CanvasText 14%, Canvas);
  --border-strong: color-mix(in oklab, CanvasText 26%, Canvas);

  --glass: color-mix(in oklab, Canvas 65%, transparent);
  --glass-strong: color-mix(in oklab, Canvas 55%, transparent);
  --glass-border: color-mix(in oklab, CanvasText 24%, transparent);

  --shadow-1: 0 6px 20px color-mix(in oklab, CanvasText 10%, transparent);
  --shadow-2: 0 14px 35px color-mix(in oklab, CanvasText 14%, transparent);
}

@media (prefers-color-scheme: dark) {
  :root {
    --glass: color-mix(in oklab, Canvas 58%, transparent);
    --glass-strong: color-mix(in oklab, Canvas 48%, transparent);
    --shadow-1: 0 8px 26px rgba(0,0,0,0.35);
    --shadow-2: 0 18px 42px rgba(0,0,0,0.45);
  }
}

[data-testid="stAppViewContainer"] {
  background:
    radial-gradient(1200px 600px at 10% -10%, color-mix(in oklab, hsl(var(--brand-2)) 8%, transparent), transparent 60%),
    radial-gradient(1300px 700px at 110% 110%, color-mix(in oklab, hsl(var(--brand-1)) 6%, transparent), transparent 60%),
    radial-gradient(900px 520px at 50% 50%, color-mix(in oklab, hsl(var(--accent-1)) 6%, transparent), transparent 65%),
    linear-gradient(180deg, color-mix(in oklab, hsl(var(--brand-1)) 3%, var(--bg)) 0%, var(--bg) 60%);
  background-attachment: fixed;
}

[data-testid="stHeader"] {
  background: linear-gradient(90deg,
              color-mix(in oklab, hsl(var(--brand-1)) 12%, transparent),
              color-mix(in oklab, hsl(var(--brand-2)) 12%, transparent))
              , var(--glass);
  backdrop-filter: blur(18px) saturate(140%);
  border-bottom: 1px solid var(--glass-border);
}

html, body { color: var(--text); }
.main-header {
  font-size: 2.25rem; font-weight: 800; letter-spacing: .2px;
  background: linear-gradient(90deg,
              color-mix(in oklab, hsl(var(--brand-1)) 65%, var(--text)),
              color-mix(in oklab, hsl(var(--brand-2)) 65%, var(--text)));
  -webkit-background-clip: text; background-clip: text; color: transparent;
  text-align: center; margin: .25rem 0 1rem;
}

/* Status pill */
.status-indicator {
  display:flex; align-items:center; justify-content:center;
  padding:.55rem .9rem; border-radius:999px; font-weight:700; font-size:.9rem;
  border:1px solid var(--glass-border); background: var(--glass-strong);
  backdrop-filter: blur(10px) saturate(130%); box-shadow: var(--shadow-1);
}

/* Cards */
.card { border-radius:18px; padding:1.1rem; border:1px solid var(--glass-border);
  background:
    radial-gradient(120% 130% at 85% 15%, color-mix(in oklab, hsl(var(--brand-2)) 5%, transparent), transparent 60%),
    radial-gradient(130% 120% at 15% 85%, color-mix(in oklab, hsl(var(--brand-1)) 5%, transparent), transparent 60%),
    var(--glass);
  backdrop-filter: blur(18px) saturate(140%); box-shadow: var(--shadow-1);
  transition: transform .25s ease, box-shadow .25s ease, border-color .25s ease;
}
.card:hover { transform: translateY(-3px); box-shadow: var(--shadow-2); border-color: var(--border-strong); }
.card-strong { background: var(--glass-strong); border:1px solid var(--border); }
.session-info h3 { color: hsl(var(--brand-1)); margin:0 0 .5rem; font-weight:800; }
.session-info p { margin:.25rem 0; color: var(--text-muted); }

/* Notifications */
.historical-notice,.pagination-info { border-radius:14px; padding:.9rem 1rem; font-weight:700;
  border:1px solid var(--border); background: var(--glass);
}

/* Gauges grid */
.widget-grid { display:grid; grid-template-columns: repeat(6, 1fr); gap:1rem; margin-top: .75rem; }
.gauge-container { text-align:center; padding:.75rem; border-radius:16px; border:1px solid var(--glass-border);
  background:
    radial-gradient(120% 120% at 85% 15%, color-mix(in oklab, hsl(var(--brand-1)) 4%, transparent), transparent 60%),
    radial-gradient(120% 130% at 20% 80%, color-mix(in oklab, hsl(var(--brand-2)) 4%, transparent), transparent 60%),
    var(--glass);
  backdrop-filter: blur(10px); transition: transform .2s ease, border-color .2s ease, background .2s ease; }
.gauge-container:hover { transform: translateY(-2px); border-color: var(--border); }
.gauge-title { font-size:.85rem; font-weight:600; color:var(--text-subtle); margin-bottom:.25rem; }

/* Chart wrappers */
.chart-wrap { border-radius:18px; border:1px solid var(--glass-border);
  background:
    radial-gradient(110% 120% at 85% 10%, color-mix(in oklab, hsl(var(--brand-1)) 3%, transparent), transparent 60%),
    var(--glass);
  padding:.75rem; box-shadow: var(--shadow-1); }

/* Buttons */
.stButton > button, div[data-testid="stDownloadButton"] > button {
  border-radius:12px !important; font-weight:700 !important; color:var(--text) !important;
  background: linear-gradient(135deg,
              color-mix(in oklab, hsl(var(--brand-1)) 28%, var(--bg)),
              color-mix(in oklab, hsl(var(--brand-2)) 28%, var(--bg))) !important;
  border: 1px solid color-mix(in oklab, hsl(var(--brand-1)) 20%, var(--border-strong)) !important;
  box-shadow: 0 6px 16px color-mix(in oklab, hsl(var(--brand-1)) 15%, transparent) !important;
  transition: transform .15s ease, box-shadow .2s ease !important;
}
.stButton > button:hover, div[data-testid="stDownloadButton"] > button:hover {
  transform: translateY(-2px);
  box-shadow: 0 10px 22px color-mix(in oklab, hsl(var(--brand-2)) 18%, transparent) !important;
}
.stButton > button:active, div[data-testid="stDownloadButton"] > button:active { transform: translateY(0); }

/* Sidebar */
[data-testid="stSidebar"] > div { background: var(--glass-strong); border-right:1px solid var(--glass-border); backdrop-filter: blur(18px) saturate(140%); }

/* Inputs */
label, .stTextInput, .stSelectbox, .stNumberInput, .stSlider { color: var(--text); }
div[data-baseweb="input"] > div { background: var(--glass); border-radius:10px; border:1px solid var(--border); }

/* Scrollbars */
::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 6px; }
::-webkit-scrollbar-thumb:hover { background: color-mix(in oklab, hsl(var(--brand-1)) 50%, var(--border-strong)); }

/* Focus ring */
*:focus-visible { outline: 2px solid color-mix(in oklab, hsl(var(--brand-1)) 55%, var(--text)); outline-offset:2px; border-radius:4px; }

/* streamlit-echarts iframe sizing guard */
iframe[title="streamlit_echarts.st_echarts"] {
  min-height: 160px !important;
  width: 100% !important;
  display: block !important;
}

/* Segmented "Tabs" styling for the Sections radio */
div[role="radiogroup"][aria-label="Sections"] {
  display:flex; flex-wrap:wrap; gap:.4rem; margin:.25rem 0 1rem 0;
}
div[role="radiogroup"][aria-label="Sections"] > label {
  background: var(--glass);
  border:1px solid var(--glass-border);
  border-radius:12px;
  padding:.45rem .75rem;
  font-weight:700;
  color: var(--text-muted);
  box-shadow: var(--shadow-1);
  transition: transform .15s ease, box-shadow .2s ease, color .2s ease, background .2s ease, border-color .2s ease;
}
div[role="radiogroup"][aria-label="Sections"] > label[aria-checked="true"]{
  color: color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text));
  background: var(--glass-strong);
  border-color: var(--border-strong);
  box-shadow: var(--shadow-2);
  transform: translateY(-2px);
}
/* hide default bullets in that radio only */
div[role="radiogroup"][aria-label="Sections"] [data-baseweb="radio"] > div:first-child { display:none !important; }
</style>
"""
st.markdown(get_theme_aware_css(), unsafe_allow_html=True)

# Logger (keep minimal, no extra chart event logs)
def setup_terminal_logging():
    logger = logging.getLogger("TelemetryDashboard")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

setup_terminal_logging()

def first_load_splash():
    if st.session_state.get("_first_load", True):
        with st.spinner("Preparing charts and layout..."):
            time.sleep(0.6)
        st.session_state["_first_load"] = False
        st.rerun()

# ---------------------------
# Telemetry Manager (unchanged core behavior)
# ---------------------------
class EnhancedTelemetryManager:
    def __init__(self):
        self.realtime_subscriber = None
        self.supabase_client = None
        self.is_connected = False
        self.message_queue = queue.Queue()
        self.connection_thread = None
        self.stats = {
            "messages_received": 0,
            "last_message_time": None,
            "connection_attempts": 0,
            "errors": 0,
            "last_error": None,
            "data_sources": [],
            "pagination_stats": {
                "total_requests": 0,
                "total_rows_fetched": 0,
                "largest_session_size": 0,
                "sessions_paginated": 0,
            },
        }
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._should_run = False
        self.logger = logging.getLogger("TelemetryDashboard")

    def connect_supabase(self) -> bool:
        try:
            self.supabase_client = create_client(SUPABASE_URL, SUPABASE_API_KEY)
            self.logger.info("✅ Connected to Supabase")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Supabase: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            return False

    def connect_realtime(self) -> bool:
        try:
            with self._lock:
                self.stats["connection_attempts"] += 1

            if self._should_run:
                self.disconnect()

            self._stop_event.clear()
            self._should_run = True

            self.connection_thread = threading.Thread(
                target=self._connection_worker, daemon=True
            )
            self.connection_thread.start()

            time.sleep(3)
            return self.is_connected

        except Exception as e:
            self.logger.error(f"❌ Real-time connection failed: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            return False

    def _connection_worker(self):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._async_connection_handler())
        except Exception as e:
            self.logger.error(f"💥 Connection worker error: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            self.is_connected = False

    async def _async_connection_handler(self):
        try:
            self.realtime_subscriber = AblyRealtime(DASHBOARD_ABLY_API_KEY)

            def on_connected(state_change):
                self.is_connected = True
                self.logger.info("✅ Connected to Ably")

            def on_disconnected(state_change):
                self.is_connected = False
                self.logger.warning("❌ Disconnected from Ably")

            def on_failed(state_change):
                self.is_connected = False
                self.logger.error(f"💥 Connection failed: {state_change}")

            self.realtime_subscriber.connection.on("connected", on_connected)
            self.realtime_subscriber.connection.on("disconnected", on_disconnected)
            self.realtime_subscriber.connection.on("failed", on_failed)

            await self.realtime_subscriber.connection.once_async("connected")

            channel = self.realtime_subscriber.channels.get(DASHBOARD_CHANNEL_NAME)
            await channel.subscribe("telemetry_update", self._on_message_received)

            while self._should_run and not self._stop_event.is_set():
                await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"💥 Async connection error: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            self.is_connected = False

    def _on_message_received(self, message):
        try:
            data = message.data
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as e:
                    self.logger.error(f"❌ JSON decode error: {e}")
                    return
            if not isinstance(data, dict):
                self.logger.warning(f"⚠️ Invalid data type: {type(data)}")
                return

            with self._lock:
                if self.message_queue.qsize() > 500:
                    while self.message_queue.qsize() > 250:
                        try:
                            self.message_queue.get_nowait()
                        except queue.Empty:
                            break

                data["data_source"] = "realtime"
                self.message_queue.put(data)
                self.stats["messages_received"] += 1
                self.stats["last_message_time"] = datetime.now()

        except Exception as e:
            self.logger.error(f"❌ Message handling error: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)

    def get_realtime_messages(self) -> List[Dict[str, Any]]:
        messages = []
        with self._lock:
            while not self.message_queue.empty():
                try:
                    message = self.message_queue.get_nowait()
                    messages.append(message)
                except queue.Empty:
                    break
        return messages

    def _paginated_fetch(self, session_id: str, data_source: str = "supabase_current") -> pd.DataFrame:
        try:
            if not self.supabase_client:
                self.logger.error("❌ Supabase client not initialized")
                return pd.DataFrame()

            all_data = []
            offset = 0
            total_fetched = 0
            request_count = 0

            self.logger.info(f"🔄 Starting paginated fetch for session {session_id[:8]}...")

            while offset < MAX_DATAPOINTS_PER_SESSION:
                try:
                    range_end = offset + SUPABASE_MAX_ROWS_PER_REQUEST - 1

                    response = (
                        self.supabase_client.table(SUPABASE_TABLE_NAME)
                        .select("*")
                        .eq("session_id", session_id)
                        .order("timestamp", desc=False)
                        .range(offset, range_end)
                        .execute()
                    )

                    request_count += 1

                    if not response.data:
                        break

                    batch_size = len(response.data)
                    all_data.extend(response.data)
                    total_fetched += batch_size

                    if batch_size < SUPABASE_MAX_ROWS_PER_REQUEST:
                        break

                    offset += SUPABASE_MAX_ROWS_PER_REQUEST
                    time.sleep(0.1)

                except Exception as e:
                    self.logger.error(f"❌ Error in pagination request {request_count}: {e}")
                    offset += SUPABASE_MAX_ROWS_PER_REQUEST
                    continue

            with self._lock:
                self.stats["pagination_stats"]["total_requests"] += request_count
                self.stats["pagination_stats"]["total_rows_fetched"] += total_fetched
                self.stats["pagination_stats"]["largest_session_size"] = max(
                    self.stats["pagination_stats"]["largest_session_size"], total_fetched
                )
                if request_count > 1:
                    self.stats["pagination_stats"]["sessions_paginated"] += 1

            if all_data:
                df = pd.DataFrame(all_data)
                df["data_source"] = data_source
                self.logger.info(
                    f"✅ Successfully fetched {len(df)} total rows for session {session_id[:8]}..."
                )
                return df
            else:
                self.logger.warning(f"⚠️ No data found for session {session_id}")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"❌ Error in paginated fetch for session {session_id}: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            return pd.DataFrame()

    def get_current_session_data(self, session_id: str) -> pd.DataFrame:
        self.logger.info(f"🔄 Fetching current session data for {session_id[:8]}...")
        return self._paginated_fetch(session_id, "supabase_current")

    def get_historical_sessions(self) -> List[Dict[str, Any]]:
        try:
            if not self.supabase_client:
                self.logger.error("❌ Supabase client not initialized")
                return []

            all_records = []
            offset = 0

            while True:
                try:
                    range_end = offset + SUPABASE_MAX_ROWS_PER_REQUEST - 1
                    response = (
                        self.supabase_client.table(SUPABASE_TABLE_NAME)
                        .select("session_id, session_name, timestamp")
                        .order("timestamp", desc=True)
                        .range(offset, range_end)
                        .execute()
                    )
                    if not response.data:
                        break
                    all_records.extend(response.data)
                    if len(response.data) < SUPABASE_MAX_ROWS_PER_REQUEST:
                        break
                    offset += SUPABASE_MAX_ROWS_PER_REQUEST

                except Exception as e:
                    self.logger.error(f"❌ Error fetching session records at offset {offset}: {e}")
                    break

            if not all_records:
                self.logger.warning("⚠️ No session records found")
                return []

            sessions = {}
            for record in all_records:
                session_id = record["session_id"]
                timestamp = record["timestamp"]
                session_name = record.get("session_name")
                if session_id not in sessions:
                    sessions[session_id] = {
                        "session_id": session_id,
                        "session_name": session_name,
                        "start_time": timestamp,
                        "end_time": timestamp,
                        "record_count": 1,
                    }
                else:
                    sessions[session_id]["record_count"] += 1
                    if session_name and not sessions[session_id].get("session_name"):
                        sessions[session_id]["session_name"] = session_name
                    if timestamp < sessions[session_id]["start_time"]:
                        sessions[session_id]["start_time"] = timestamp
                    if timestamp > sessions[session_id]["end_time"]:
                        sessions[session_id]["end_time"] = timestamp

            session_list = []
            for session_info in sessions.values():
                try:
                    start_dt = datetime.fromisoformat(session_info["start_time"].replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(session_info["end_time"].replace("Z", "+00:00"))
                    duration = end_dt - start_dt
                    session_list.append(
                        {
                            "session_id": session_info["session_id"],
                            "session_name": session_info.get("session_name"),
                            "start_time": start_dt,
                            "end_time": end_dt,
                            "duration": duration,
                            "record_count": session_info["record_count"],
                        }
                    )
                except Exception as e:
                    self.logger.error(f"❌ Error processing session {session_info['session_id']}: {e}")

            sorted_sessions = sorted(session_list, key=lambda x: x["start_time"], reverse=True)
            self.logger.info(f"✅ Found {len(sorted_sessions)} unique sessions")
            return sorted_sessions

        except Exception as e:
            self.logger.error(f"❌ Error fetching historical sessions: {e}")
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            return []

    def get_historical_data(self, session_id: str) -> pd.DataFrame:
        self.logger.info(f"🔄 Fetching historical data for session {session_id[:8]}...")
        return self._paginated_fetch(session_id, "supabase_historical")

    def disconnect(self):
        try:
            self._should_run = False
            self._stop_event.set()
            self.is_connected = False

            if self.realtime_subscriber:
                try:
                    self.realtime_subscriber.close()
                except Exception as e:
                    self.logger.warning(f"⚠️ Error closing Ably: {e}")

            if self.connection_thread and self.connection_thread.is_alive():
                self.connection_thread.join(timeout=5)

            self.logger.info("🔚 Disconnected from services")

        except Exception as e:
            self.logger.error(f"❌ Disconnect error: {e}")
        finally:
            self.realtime_subscriber = None

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return self.stats.copy()

# ---------------------------
# Data helpers
# ---------------------------
def merge_telemetry_data(realtime_data: List[Dict], supabase_data: pd.DataFrame, streamlit_history: pd.DataFrame) -> pd.DataFrame:
    try:
        all_data = []
        if realtime_data:
            all_data.extend(realtime_data)
        if not supabase_data.empty:
            all_data.extend(supabase_data.to_dict("records"))
        if not streamlit_history.empty:
            all_data.extend(streamlit_history.to_dict("records"))
        if not all_data:
            return pd.DataFrame()

        df = pd.DataFrame(all_data)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df.dropna(subset=["timestamp"], inplace=True)
        else:
            return df

        dedup_columns = ["timestamp"]
        if "message_id" in df.columns:
            dedup_columns.append("message_id")

        df = df.drop_duplicates(subset=dedup_columns, keep="last")
        df = df.sort_values("timestamp", ascending=True).reset_index(drop=True)
        return df

    except Exception as e:
        st.error(f"Error merging telemetry data: {e}")
        return pd.DataFrame()

def initialize_session_state():
    defaults = {
        "telemetry_manager": None,
        "telemetry_data": pd.DataFrame(),
        "last_update": datetime.now(),
        "auto_refresh": True,
        "dynamic_charts": [],
        "data_source_mode": "realtime_session",
        "selected_session": None,
        "historical_sessions": [],
        "current_session_id": None,
        "is_viewing_historical": False,
        "pagination_info": {"is_loading": False, "current_session": None, "total_requests": 0, "total_rows": 0},
        "chart_info_initialized": False,
        "data_quality_notifications": [],
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

def calculate_roll_and_pitch(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df_calc = df.copy()
    accel_cols = ["accel_x", "accel_y", "accel_z"]
    if not all(col in df_calc.columns for col in accel_cols):
        return df_calc
    try:
        for col in accel_cols:
            df_calc[col] = pd.to_numeric(df_calc[col], errors="coerce")
        denominator_roll = np.sqrt(df_calc["accel_x"] ** 2 + df_calc["accel_z"] ** 2)
        denominator_roll = np.where(denominator_roll == 0, 1e-10, denominator_roll)
        df_calc["roll_rad"] = np.arctan2(df_calc["accel_y"], denominator_roll)
        df_calc["roll_deg"] = np.degrees(df_calc["roll_rad"])

        denominator_pitch = np.sqrt(df_calc["accel_y"] ** 2 + df_calc["accel_z"] ** 2)
        denominator_pitch = np.where(denominator_pitch == 0, 1e-10, denominator_pitch)
        df_calc["pitch_rad"] = np.arctan2(df_calc["accel_x"], denominator_pitch)
        df_calc["pitch_deg"] = np.degrees(df_calc["pitch_rad"])

        df_calc[["roll_rad", "roll_deg", "pitch_rad", "pitch_deg"]] = (
            df_calc[["roll_rad", "roll_deg", "pitch_rad", "pitch_deg"]]
            .replace([np.inf, -np.inf, np.nan], 0)
            .astype(float)
        )
    except Exception as e:
        st.warning(f"⚠️ Error calculating Roll and Pitch: {e}")
        df_calc["roll_rad"] = 0.0
        df_calc["roll_deg"] = 0.0
        df_calc["pitch_rad"] = 0.0
        df_calc["pitch_deg"] = 0.0
    return df_calc

def calculate_kpis(df: pd.DataFrame) -> Dict[str, float]:
    default_kpis = {
        "current_speed_ms": 0.0,
        "total_distance_km": 0.0,
        "max_speed_ms": 0.0,
        "avg_speed_ms": 0.0,
        "current_speed_kmh": 0.0,
        "max_speed_kmh": 0.0,
        "avg_speed_kmh": 0.0,
        "total_energy_kwh": 0.0,
        "avg_power_w": 0.0,
        "c_current_a": 0.0,
        "efficiency_km_per_kwh": 0.0,
        "battery_voltage_v": 0.0,
        "battery_percentage": 0.0,
        "avg_current_a": 0.0,
        "current_roll_deg": 0.0,
        "current_pitch_deg": 0.0,
        "max_roll_deg": 0.0,
        "max_pitch_deg": 0.0,
    }
    if df.empty:
        return default_kpis

    try:
        df = calculate_roll_and_pitch(df)
        numeric_cols = [
            "energy_j",
            "speed_ms",
            "distance_m",
            "power_w",
            "voltage_v",
            "c_current_a",
            "latitude",
            "longitude",
            "altitude",
            "roll_deg",
            "pitch_deg",
            "current_a",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        kpis = default_kpis.copy()
        if "speed_ms" in df.columns:
            speed_data = df["speed_ms"].dropna()
            if not speed_data.empty:
                kpis["current_speed_ms"] = max(0, float(speed_data.iloc[-1]))
                kpis["max_speed_ms"] = max(0, float(speed_data.max()))
                kpis["avg_speed_ms"] = max(0, float(speed_data.mean()))

        kpis["current_speed_kmh"] = kpis["current_speed_ms"] * 3.6
        kpis["max_speed_kmh"] = kpis["max_speed_ms"] * 3.6
        kpis["avg_speed_kmh"] = kpis["avg_speed_ms"] * 3.6

        if "distance_m" in df.columns and not df["distance_m"].dropna().empty:
            kpis["total_distance_km"] = max(0, float(df["distance_m"].dropna().iloc[-1]) / 1000.0)

        if "energy_j" in df.columns and not df["energy_j"].dropna().empty:
            kpis["total_energy_kwh"] = max(0, float(df["energy_j"].dropna().iloc[-1]) / 3_600_000.0)

        if "power_w" in df.columns:
            power_data = df["power_w"].dropna()
            if not power_data.empty:
                kpis["avg_power_w"] = max(0, float(power_data.mean()))

        if kpis["total_energy_kwh"] > 0:
            kpis["efficiency_km_per_kwh"] = kpis["total_distance_km"] / kpis["total_energy_kwh"]

        if "voltage_v" in df.columns:
            voltage_data = df["voltage_v"].dropna()
            if not voltage_data.empty:
                kpis["battery_voltage_v"] = max(0, float(voltage_data.iloc[-1]))
                nominal_voltage = 50.4
                max_voltage = 58.5
                min_voltage = 50.4
                cv = kpis["battery_voltage_v"]
                if cv > min_voltage:
                    kpis["battery_percentage"] = min(
                        100.0, max(0.0, ((cv - min_voltage) / (max_voltage - min_voltage)) * 100.0)
                    )

        if "current_a" in df.columns:
            curr_data = df["current_a"].dropna()
            if not curr_data.empty:
                kpis["avg_current_a"] = max(0.0, float(curr_data.mean()))
                kpis["c_current_a"] = max(0.0, float(curr_data.iloc[-1]))
            else:
                kpis["c_current_a"] = 0.0

        if "roll_deg" in df.columns:
            roll_data = df["roll_deg"].dropna()
            if not roll_data.empty:
                kpis["current_roll_deg"] = float(roll_data.iloc[-1])
                kpis["max_roll_deg"] = float(roll_data.abs().max())

        if "pitch_deg" in df.columns:
            pitch_data = df["pitch_deg"].dropna()
            if not pitch_data.empty:
                kpis["current_pitch_deg"] = float(pitch_data.iloc[-1])
                kpis["max_pitch_deg"] = float(pitch_data.abs().max())

        return kpis

    except Exception as e:
        st.error(f"Error calculating KPIs: {e}")
        return default_kpis

# ---------------------------
# ECharts utilities
# ---------------------------
def _rgb_tuple(hex_color: str) -> Tuple[int, int, int]:
    try:
        r, g, b = [int(x * 255) for x in mcolors.to_rgb(hex_color)]
        return r, g, b
    except Exception:
        return 31, 119, 180

def _ts_to_iso_list(ts: pd.Series) -> List[str]:
    s = pd.to_datetime(ts, errors="coerce", utc=True)
    return s.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("").astype(str).tolist()

def _echarts_base_opts(title: str = "") -> Dict[str, Any]:
    # Larger top grid to prevent overlap of title/legend with plot area.
    return {
        "title": {"text": title, "left": "center", "top": 0, "textStyle": {"fontSize": 14}},
        "legend": {"top": 30},
        "grid": {"left": "4%", "right": "4%", "top": 70, "bottom": 50, "containLabel": True},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "time", "axisLine": {"lineStyle": {"color": "#888"}}},
        "yAxis": {"type": "value", "axisLine": {"lineStyle": {"color": "#888"}}},
        # Smooth update, no number value animations
        "animation": True,
        "animationDuration": 200,
        "animationDurationUpdate": 200,
        "animationEasing": "cubicOut",
        "animationEasingUpdate": "cubicOut",
        "useDirtyRect": True,
        "progressive": 2000,
        "progressiveThreshold": 4000,
        # Handy toolbox for users (zoom/restore/image)
        "toolbox": {
            "right": 10,
            "feature": {"dataZoom": {"yAxisIndex": "none"}, "restore": {}, "saveAsImage": {}},
        },
    }

def _num_or_none(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if pd.isna(x):
            return None
        v = float(x)
        if np.isnan(v) or np.isinf(v):
            return None
        return v
    except Exception:
        return None

def _echarts_responsive_events() -> Dict[str, str]:
    js = (
        "function(){try{var chart=this;var el=chart.getDom();"
        "function safe(){try{chart.resize();}catch(e){}}"
        "if(!el.__t3_hooks__){"
        "  window.addEventListener('resize', safe, {passive:true});"
        "  if(typeof ResizeObserver!=='undefined'){"
        "    var ro=new ResizeObserver(function(){safe()});"
        "    try{ro.observe(el);}catch(e){}"
        "    var p=el.parentElement;var n=0;"
        "    while(p&&n<6){try{ro.observe(p);}catch(e){} p=p.parentElement;n++;}"
        "  }"
        "  if(typeof IntersectionObserver!=='undefined'){"
        "    try{var io=new IntersectionObserver(function(es){"
        "      for(var i=0;i<es.length;i++){if(es[i].isIntersecting){setTimeout(safe,60);setTimeout(safe,180);}}"
        "    },{root:null,threshold:0}); io.observe(el);}catch(e){}"
        "  }"
        "  if(typeof MutationObserver!=='undefined'){"
        "    try{var tablist=document.querySelector('[data-baseweb=\"tab-list\"]');"
        "    if(tablist){var mo=new MutationObserver(function(){setTimeout(safe,60);setTimeout(safe,180);});"
        "      mo.observe(tablist,{attributes:true,subtree:true,attributeFilter:['aria-selected','aria-hidden','style']});}"
        "    }catch(e){}"
        "  }"
        "  document.addEventListener('visibilitychange', function(){setTimeout(safe,60)});"
        "  var tries=0; var iv=setInterval(function(){var r=el.getBoundingClientRect();"
        "    if(r.width>0&&r.height>0){safe();clearInterval(iv);} if(++tries>60){clearInterval(iv);}},150);"
        "  el.__t3_hooks__=true;"
        "}"
        "return null;}catch(e){return null;}"
    )
    return {"rendered": js, "finished": js}

def _st_echarts_render(options: Dict[str, Any], height_px: int, key: str):
    try:
        st_echarts(
            options=options,
            height=f"{height_px}px",
            width="100%",
            renderer="canvas",
            key=key,
            events=_echarts_responsive_events(),
        )
    except Exception as e:
        logging.getLogger("TelemetryDashboard").error(f"ECharts render error [{key}]: {e}")

# ---------------------------
# Gauges (value only, no value animation)
# ---------------------------
def create_small_gauge_option(
    value: float,
    max_val: Optional[float],
    title: str,
    color_hex: str,
    suffix: str = "",
    avg_ref: Optional[float] = None,
    thresh_val: Optional[float] = None,
) -> Dict[str, Any]:
    if max_val is None or max_val <= 0:
        max_val = value * 1.2 if value > 0 else 1.0
    v = float(value or 0.0)
    mx = float(max_val)
    r, g, b = _rgb_tuple(color_hex)
    color = f"rgb({r},{g},{b})"

    main_series = {
        "type": "gauge",
        "min": 0,
        "max": mx,
        "startAngle": 220,
        "endAngle": -40,
        "progress": {"show": True, "width": 10, "itemStyle": {"color": color}},
        "axisLine": {
            "lineStyle": {
                "width": 10,
                "color": [[0.6, f"rgba({r},{g},{b},0.15)"], [1.0, f"rgba({r},{g},{b},0.3)"]],
            }
        },
        "axisTick": {"show": False},
        "splitLine": {"length": 10, "lineStyle": {"width": 2, "color": "#999"}},
        "axisLabel": {"show": False},
        "anchor": {"show": True, "showAbove": True, "size": 8, "itemStyle": {"color": "#fff"}},
        "pointer": {"length": "60%", "width": 4, "itemStyle": {"color": color}},
        "title": {"show": False},
        "detail": {
            "valueAnimation": False,  # no numeric animation
            "offsetCenter": [0, "60%"],
            "fontSize": 16,
            "fontWeight": "bold",
            "formatter": JsCode("function(v){return Number(v).toFixed(1);}").js_code,
            "color": "#333",
        },
        "data": [{"value": v, "name": title}],
    }

    option = {
        "title": {"text": title, "left": "center", "top": 0, "textStyle": {"fontSize": 12}},
        "tooltip": {"show": False},
        "series": [main_series],
        "animation": True,
        "useDirtyRect": True,
    }
    return option

def render_live_gauges(kpis: Dict[str, float], unique_ns: str = "gauges"):
    st.markdown("##### 📊 Live Performance Gauges")
    st.markdown('<div class="widget-grid">', unsafe_allow_html=True)
    cols = st.columns(6)

    with cols[0]:
        st.markdown('<div class="gauge-container"><div class="gauge-title">🚀 Speed (km/h)</div>', unsafe_allow_html=True)
        opt = create_small_gauge_option(
            kpis["current_speed_kmh"],
            max_val=max(100, kpis["max_speed_kmh"] + 5),
            title="Speed",
            color_hex="#444",
        )
        _st_echarts_render(opt, 140, key=f"{unique_ns}_gauge_speed")
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[1]:
        st.markdown('<div class="gauge-container"><div class="gauge-title">🔋 Battery (%)</div>', unsafe_allow_html=True)
        opt = create_small_gauge_option(kpis["battery_percentage"], 100, "Battery", "#4a4a4a")
        _st_echarts_render(opt, 140, key=f"{unique_ns}_gauge_battery")
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[2]:
        st.markdown('<div class="gauge-container"><div class="gauge-title">💡 Power (W)</div>', unsafe_allow_html=True)
        opt = create_small_gauge_option(
            kpis["avg_power_w"], max_val=max(1000, kpis["avg_power_w"] * 2 + 1), title="Power", color_hex="#5a5a5a"
        )
        _st_echarts_render(opt, 140, key=f"{unique_ns}_gauge_power")
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[3]:
        st.markdown('<div class="gauge-container"><div class="gauge-title">♻️ Efficiency (km/kWh)</div>', unsafe_allow_html=True)
        eff_val = kpis["efficiency_km_per_kwh"]
        opt = create_small_gauge_option(
            eff_val, max_val=max(100, eff_val * 1.5) if eff_val > 0 else 100, title="Efficiency", color_hex="#6a6a6a"
        )
        _st_echarts_render(opt, 140, key=f"{unique_ns}_gauge_eff")
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[4]:
        st.markdown('<div class="gauge-container"><div class="gauge-title">🔄 Roll (°)</div>', unsafe_allow_html=True)
        roll_max = max(45, abs(kpis["current_roll_deg"]) + 10) if kpis["current_roll_deg"] != 0 else 45
        opt = create_small_gauge_option(kpis["current_roll_deg"], roll_max, "Roll", "#7a7a7a")
        _st_echarts_render(opt, 140, key=f"{unique_ns}_gauge_roll")
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[5]:
        st.markdown('<div class="gauge-container"><div class="gauge-title">📐 Pitch (°)</div>', unsafe_allow_html=True)
        pitch_max = max(45, abs(kpis["current_pitch_deg"]) + 10) if kpis["current_pitch_deg"] != 0 else 45
        opt = create_small_gauge_option(kpis["current_pitch_deg"], pitch_max, "Pitch", "#8a8a8a")
        _st_echarts_render(opt, 140, key=f"{unique_ns}_gauge_pitch")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

def render_kpi_header(kpis: Dict[str, float], unique_ns: str = "kpiheader", show_gauges: bool = True):
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("📏 Distance", f"{kpis['total_distance_km']:.2f} km")
        st.metric("🏃 Max Speed", f"{kpis['max_speed_kmh']:.1f} km/h")
    with col2:
        st.metric("⚡ Avg Speed", f"{kpis['avg_speed_kmh']:.1f} km/h")
        st.metric("🔋 Energy", f"{kpis['total_energy_kwh']:.2f} kWh")
    with col3:
        st.metric("⚡ Voltage", f"{kpis['battery_voltage_v']:.1f} V")
        st.metric("🔄 Current", f"{kpis['c_current_a']:.1f} A")
    with col4:
        st.metric("💡 Avg Power", f"{kpis['avg_power_w']:.1f} W")
        st.metric("🌊 Avg Current ", f"{kpis['avg_current_a']:.1f} A")

    if show_gauges:
        render_live_gauges(kpis, unique_ns)

# ---------------------------
# Charts with zoom + spacing + smooth updates
# ---------------------------
def create_speed_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "speed_ms" not in df.columns:
        return {"title": {"text": "No speed data available"}, "animation": False}
    opt = _echarts_base_opts("🚗 Vehicle Speed Over Time")
    ts_iso = _ts_to_iso_list(df["timestamp"])
    spd_raw = pd.to_numeric(df["speed_ms"], errors="coerce")
    spd = [0.0 if _num_or_none(v) is None else float(v) for v in spd_raw]
    opt.update(
        {
            "dataset": {"source": [[t, s] for t, s in zip(ts_iso, spd)]},
            "series": [
                {
                    "type": "line",
                    "name": "Speed (m/s)",
                    "encode": {"x": 0, "y": 1},
                    "showSymbol": False,
                    "lineStyle": {"width": 2, "color": "#4c4c4c"},
                    "sampling": "lttb",
                    "smooth": 0.2,
                }
            ],
            "yAxis": {"name": "m/s"},
            "dataZoom": [
                {"type": "inside", "xAxisIndex": [0]},
                {"type": "slider", "xAxisIndex": [0], "bottom": 10, "height": 16},
            ],
        }
    )
    return opt

def create_power_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    need = {"voltage_v", "current_a", "power_w"}
    if df.empty or not need.issubset(df.columns):
        return {"title": {"text": "No power data available"}, "animation": False}

    ts = _ts_to_iso_list(df["timestamp"])
    volt = [_num_or_none(v) for v in pd.to_numeric(df["voltage_v"], errors="coerce")]
    curr = [_num_or_none(v) for v in pd.to_numeric(df["current_a"], errors="coerce")]
    pwr = [_num_or_none(v) for v in pd.to_numeric(df["power_w"], errors="coerce")]

    src_top = [[t, v, c] for t, v, c in zip(ts, volt, curr)]
    src_bot = [[t, w] for t, w in zip(ts, pwr)]

    return {
        "title": {"text": "⚡ Electrical System Performance"},
        "legend": {"top": 30},
        "tooltip": {"trigger": "axis"},
        "grid": [
            {"left": "6%", "right": "4%", "top": 70, "height": 180, "containLabel": True},
            {"left": "6%", "right": "4%", "top": 280, "height": 180, "containLabel": True},
        ],
        "xAxis": [{"type": "time", "gridIndex": 0}, {"type": "time", "gridIndex": 1}],
        "yAxis": [{"type": "value", "gridIndex": 0, "name": "V / A"}, {"type": "value", "gridIndex": 1, "name": "W"}],
        "dataset": [{"id": "top", "source": src_top}, {"id": "bot", "source": src_bot}],
        "series": [
            {
                "type": "line",
                "datasetId": "top",
                "name": "Voltage (V)",
                "encode": {"x": 0, "y": 1},
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#4d4d4d"},
                "sampling": "lttb",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "smooth": 0.15,
            },
            {
                "type": "line",
                "datasetId": "top",
                "name": "Current (A)",
                "encode": {"x": 0, "y": 2},
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#707070"},
                "sampling": "lttb",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "smooth": 0.15,
            },
            {
                "type": "line",
                "datasetId": "bot",
                "name": "Power (W)",
                "encode": {"x": 0, "y": 1},
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#8a8a8a"},
                "sampling": "lttb",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "smooth": 0.15,
            },
        ],
        "axisPointer": {"link": [{"xAxisIndex": "all"}]},
        "animation": True,
        "useDirtyRect": True,
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0, 1]},
            {"type": "slider", "xAxisIndex": [0, 1], "bottom": 10, "height": 16},
        ],
        "toolbox": {"right": 10, "feature": {"dataZoom": {"yAxisIndex": "none"}, "restore": {}, "saveAsImage": {}}},
    }

def create_imu_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    need = {"gyro_x", "gyro_y", "gyro_z", "accel_x", "accel_y", "accel_z"}
    if df.empty or not need.issubset(df.columns):
        return {"title": {"text": "No IMU data available"}, "animation": False}

    df2 = calculate_roll_and_pitch(df)
    ts = _ts_to_iso_list(df2["timestamp"])

    def col(v):
        return [_num_or_none(x) for x in pd.to_numeric(df2[v], errors="coerce")]

    gx, gy, gz = col("gyro_x"), col("gyro_y"), col("gyro_z")
    ax, ay, az = col("accel_x"), col("accel_y"), col("accel_z")
    roll, pitch = col("roll_deg"), col("pitch_deg")

    src_gyro = [[t, a, b, c] for t, a, b, c in zip(ts, gx, gy, gz)]
    src_acc = [[t, a, b, c] for t, a, b, c in zip(ts, ax, ay, az)]
    src_rp = [[t, r, p] for t, r, p in zip(ts, roll, pitch)]

    opt = {
        "title": {"text": "⚡ IMU System Performance with Roll & Pitch"},
        "legend": {"top": 30},
        "tooltip": {"trigger": "axis"},
        "grid": [
            {"left": "6%", "right": "4%", "top": 70, "height": 160, "containLabel": True},
            {"left": "6%", "right": "4%", "top": 260, "height": 160, "containLabel": True},
            {"left": "6%", "right": "4%", "top": 450, "height": 160, "containLabel": True},
        ],
        "xAxis": [{"type": "time", "gridIndex": i} for i in range(3)],
        "yAxis": [{"type": "value", "gridIndex": 0, "name": "deg/s"}, {"type": "value", "gridIndex": 1, "name": "m/s²"}, {"type": "value", "gridIndex": 2, "name": "°"}],
        "dataset": [{"id": "gyro", "source": src_gyro}, {"id": "acc", "source": src_acc}, {"id": "rp", "source": src_rp}],
        "series": [
            {"type": "line", "datasetId": "gyro", "name": "Gyro X", "encode": {"x": 0, "y": 1}, "xAxisIndex": 0, "yAxisIndex": 0, "showSymbol": False, "lineStyle": {"width": 2, "color": "#a44"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "gyro", "name": "Gyro Y", "encode": {"x": 0, "y": 2}, "xAxisIndex": 0, "yAxisIndex": 0, "showSymbol": False, "lineStyle": {"width": 2, "color": "#4a4"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "gyro", "name": "Gyro Z", "encode": {"x": 0, "y": 3}, "xAxisIndex": 0, "yAxisIndex": 0, "showSymbol": False, "lineStyle": {"width": 2, "color": "#49c"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "acc", "name": "Accel X", "encode": {"x": 0, "y": 1}, "xAxisIndex": 1, "yAxisIndex": 1, "showSymbol": False, "lineStyle": {"width": 2, "color": "#777"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "acc", "name": "Accel Y", "encode": {"x": 0, "y": 2}, "xAxisIndex": 1, "yAxisIndex": 1, "showSymbol": False, "lineStyle": {"width": 2, "color": "#999"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "acc", "name": "Accel Z", "encode": {"x": 0, "y": 3}, "xAxisIndex": 1, "yAxisIndex": 1, "showSymbol": False, "lineStyle": {"width": 2, "color": "#bbb"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "rp", "name": "Roll (°)", "encode": {"x": 0, "y": 1}, "xAxisIndex": 2, "yAxisIndex": 2, "showSymbol": False, "lineStyle": {"width": 3, "color": "#666"}, "sampling": "lttb", "smooth": 0.15},
            {"type": "line", "datasetId": "rp", "name": "Pitch (°)", "encode": {"x": 0, "y": 2}, "xAxisIndex": 2, "yAxisIndex": 2, "showSymbol": False, "lineStyle": {"width": 3, "color": "#888"}, "sampling": "lttb", "smooth": 0.15},
        ],
        "axisPointer": {"link": [{"xAxisIndex": "all"}]},
        "animation": True,
        "useDirtyRect": True,
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0, 1, 2]},
            {"type": "slider", "xAxisIndex": [0, 1, 2], "bottom": 10, "height": 16},
        ],
        "toolbox": {"right": 10, "feature": {"dataZoom": {"yAxisIndex": "none"}, "restore": {}, "saveAsImage": {}}},
    }
    return opt

def create_imu_detail_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    need = {"gyro_x", "gyro_y", "gyro_z", "accel_x", "accel_y", "accel_z"}
    if df.empty or not need.issubset(set(df.columns)):
        return {"title": {"text": "No IMU data available"}, "animation": False}

    df2 = calculate_roll_and_pitch(df)
    ts = _ts_to_iso_list(df2["timestamp"])

    def col(v):
        return pd.to_numeric(df2[v], errors="coerce").fillna(np.nan).astype(float).tolist()

    gx, gy, gz = col("gyro_x"), col("gyro_y"), col("gyro_z")
    ax, ay, az = col("accel_x"), col("accel_y"), col("accel_z")
    roll, pitch = col("roll_deg"), col("pitch_deg")

    grids = []
    x_axes = []
    y_axes = []
    series = []

    top_offsets = [70, 280, 490]
    left_offsets = ["6%", "36%", "66%"]
    height = 180

    col_map = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: None}

    grid_idx = 0
    for r in range(3):
        for c in range(3):
            grids.append({"left": left_offsets[c], "top": top_offsets[r], "width": "28%", "height": height, "containLabel": True})
            x_axes.append({"type": "time", "gridIndex": grid_idx})
            y_axes.append({"type": "value", "gridIndex": grid_idx})
            grid_idx += 1

    titles = ["🌀 Gyro X", "🌀 Gyro Y", "🌀 Gyro Z", "📊 Accel X", "📊 Accel Y", "📊 Accel Z", "🔄 Roll (°)", "📐 Pitch (°)", "🎯 R&P Combined"]

    for i in range(9):
        if i == 8:
            series.append(
                {"type": "line", "name": "Roll", "encode": {"x": 0, "y": 7}, "xAxisIndex": i, "yAxisIndex": i, "showSymbol": False, "lineStyle": {"width": 2, "color": "#666"}, "sampling": "lttb", "smooth": 0.15}
            )
            series.append(
                {"type": "line", "name": "Pitch", "encode": {"x": 0, "y": 8}, "xAxisIndex": i, "yAxisIndex": i, "showSymbol": False, "lineStyle": {"width": 2, "color": "#888"}, "sampling": "lttb", "smooth": 0.15}
            )
        else:
            color = (
                "#555" if i == 0 else "#666" if i == 1 else "#777" if i == 2 else "#888" if i == 3 else "#999" if i == 4 else "#aaa" if i == 5 else "#666" if i == 6 else "#888"
            )
            series.append(
                {
                    "type": "line",
                    "name": titles[i].split()[1],
                    "encode": {"x": 0, "y": col_map[i]},
                    "xAxisIndex": i,
                    "yAxisIndex": i,
                    "showSymbol": False,
                    "lineStyle": {"width": 2, "color": color},
                    "sampling": "lttb",
                    "smooth": 0.15,
                }
            )

    dataset_source = [[t, gx[i], gy[i], gz[i], ax[i], ay[i], az[i], roll[i], pitch[i]] for i, t in enumerate(ts)]

    opt = {
        "title": {"text": "🎮 Detailed IMU Sensor Analysis with Roll & Pitch"},
        "legend": {"top": 30},
        "tooltip": {"trigger": "axis"},
        "dataset": {"source": dataset_source},
        "grid": grids,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "series": series,
        "axisPointer": {"link": [{"xAxisIndex": "all"}]},
        "animation": True,
        "useDirtyRect": True,
        "progressive": 2000,
        "progressiveThreshold": 4000,
        "legend": {"top": 30},
        "dataZoom": [
            {"type": "inside", "xAxisIndex": list(range(9))},
            {"type": "slider", "xAxisIndex": list(range(9)), "bottom": 10, "height": 16},
        ],
        "toolbox": {"right": 10, "feature": {"dataZoom": {"yAxisIndex": "none"}, "restore": {}, "saveAsImage": {}}},
    }
    return opt

def create_efficiency_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    need = {"speed_ms", "power_w"}
    if df.empty or not need.issubset(df.columns):
        return {"title": {"text": "No efficiency data available"}, "animation": False}

    spd = [_num_or_none(v) for v in pd.to_numeric(df["speed_ms"], errors="coerce")]
    pwr = [_num_or_none(v) for v in pd.to_numeric(df["power_w"], errors="coerce")]
    volt_raw = pd.to_numeric(df.get("voltage_v", pd.Series([None] * len(df))), errors="coerce")
    volt = [_num_or_none(v) for v in volt_raw]

    src = [[spd[i], pwr[i], volt[i]] for i in range(len(spd))]
    v_non_none = [v for v in volt if v is not None]
    vm_show = len(v_non_none) > 0
    vmin = min(v_non_none) if vm_show else 0
    vmax = max(v_non_none) if vm_show else 1

    return {
        "title": {"text": "⚡ Efficiency Analysis: Speed vs Power Consumption"},
        "legend": {"top": 30},
        "tooltip": {
            "trigger": "item",
            "formatter": JsCode(
                "function(p){return 'Speed: ' + (p.value[0]==null?'N/A':p.value[0].toFixed(2)) + ' m/s<br/>' +"
                "'Power: ' + (p.value[1]==null?'N/A':p.value[1].toFixed(1)) + ' W' +"
                "(p.value[2]==null ? '' : '<br/>Voltage: ' + p.value[2].toFixed(1) + ' V');}"
            ).js_code,
        },
        "grid": {"left": "6%", "right": "6%", "top": 70, "bottom": 50, "containLabel": True},
        "xAxis": {"type": "value", "name": "Speed (m/s)"},
        "yAxis": {"type": "value", "name": "Power (W)"},
        "visualMap": {
            "type": "continuous",
            "min": vmin,
            "max": vmax,
            "dimension": 2,
            "inRange": {"color": ["#444", "#666", "#888", "#aaa", "#ccc"]},
            "right": 5,
            "top": "middle",
            "calculable": True,
            "show": vm_show,
        },
        "series": [{"type": "scatter", "symbolSize": 6, "encode": {"x": 0, "y": 1}, "itemStyle": {"opacity": 0.85}}],
        "dataset": {"source": src},
        "animation": True,
        "useDirtyRect": True,
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0], "yAxisIndex": [0]},
            {"type": "slider", "xAxisIndex": [0], "bottom": 10, "height": 16},
            {"type": "slider", "yAxisIndex": [0], "right": 10, "height": "60%", "top": 100},
        ],
        "toolbox": {"right": 10, "feature": {"dataZoom": {"yAxisIndex": None}, "restore": {}, "saveAsImage": {}}},
    }

def create_gps_map_with_altitude_option(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or df.empty:
        return {"title": {"text": "No GPS data available"}, "animation": False}

    def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols = list(df.columns)
        lower_map = {c.lower(): c for c in cols}
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        for col in cols:
            low = col.lower()
            for cand in candidates:
                if cand.lower() in low:
                    return col
        return None

    lat_col = _find_col(df, ["latitude", "lat", "gps_lat", "gps_latitude"])
    lon_col = _find_col(df, ["longitude", "lon", "lng", "gps_lon", "gps_longitude"])
    alt_col = _find_col(df, ["altitude", "elevation", "alt", "alt_m", "height", "height_m", "gps_altitude"])
    time_col = _find_col(df, ["timestamp", "time", "ts", "time_utc", "created_at", "datetime", "date"])

    if not lat_col or not lon_col:
        return {"title": {"text": "No GPS coordinate columns found (lat/lon)"}}

    dfw = df.copy()
    dfw["latitude"] = pd.to_numeric(dfw[lat_col], errors="coerce")
    dfw["longitude"] = pd.to_numeric(dfw[lon_col], errors="coerce")
    if alt_col:
        dfw["altitude"] = pd.to_numeric(dfw[alt_col], errors="coerce")
    else:
        dfw["altitude"] = np.nan

    if time_col:
        try:
            dfw["timestamp"] = pd.to_datetime(dfw[time_col], errors="coerce", utc=True)
        except Exception:
            dfw["timestamp"] = pd.to_datetime(dfw[time_col], errors="coerce")

    valid_mask = (
        (~dfw["latitude"].isna())
        & (~dfw["longitude"].isna())
        & (dfw["latitude"].abs() <= 90)
        & (dfw["longitude"].abs() <= 180)
    )
    near_zero_mask = (dfw["latitude"].abs() < 1e-6) & (dfw["longitude"].abs() < 1e-6)
    valid_mask = valid_mask & (~near_zero_mask)
    df_filtered = dfw.loc[valid_mask].copy()

    if df_filtered.empty:
        return {"title": {"text": "No valid GPS coordinates found after filtering"}}

    if "timestamp" in df_filtered.columns and not df_filtered["timestamp"].isna().all():
        df_filtered = df_filtered.sort_values("timestamp").reset_index(drop=True)
    else:
        df_filtered = df_filtered.reset_index(drop=True)

    ts_iso = _ts_to_iso_list(df_filtered.get("timestamp", pd.Series(index=df_filtered.index)))
    speed = pd.to_numeric(df_filtered.get("speed_ms", np.nan), errors="coerce").astype(float)
    curr = pd.to_numeric(df_filtered.get("current_a", np.nan), errors="coerce").astype(float)
    pwr = pd.to_numeric(df_filtered.get("power_w", np.nan), errors="coerce").astype(float)
    alt = pd.to_numeric(df_filtered.get("altitude", np.nan), errors="coerce").astype(float)
    lat = df_filtered["latitude"].astype(float)
    lon = df_filtered["longitude"].astype(float)

    src_track = [
        [float(lon[i]), float(lat[i]), ts_iso[i], float(speed[i]) if not np.isnan(speed[i]) else None, float(curr[i]) if not np.isnan(curr[i]) else None, float(pwr[i]) if not np.isnan(pwr[i]) else None]
        for i in range(len(df_filtered))
    ]
    src_alt = [[ts_iso[i], float(alt[i]) if not np.isnan(alt[i]) else None] for i in range(len(df_filtered))]

    tooltip_fmt_code = JsCode(
        "function(p){var v=p.value;return 'Time: '+(v[2]||'')+'<br/>Speed: '+"
        "(v[3]==null?'N/A':v[3].toFixed(2)+' m/s')+'<br/>Current: '+"
        "(v[4]==null?'N/A':v[4].toFixed(2)+' A')+'<br/>Power: '+"
        "(v[5]==null?'N/A':v[5].toFixed(1)+' W');}"
    ).js_code

    opt = {
        "title": {"text": "🛰️ GPS Tracking and Altitude Analysis"},
        "legend": {"top": 30},
        "grid": [
            {"left": "6%", "right": "40%", "top": 70, "height": 420, "containLabel": True},
            {"left": "64%", "right": "6%", "top": 70, "height": 420, "containLabel": True},
        ],
        "xAxis": [{"type": "value", "gridIndex": 0, "name": "Longitude"}, {"type": "time", "gridIndex": 1, "name": "Time"}],
        "yAxis": [{"type": "value", "gridIndex": 0, "name": "Latitude"}, {"type": "value", "gridIndex": 1, "name": "Altitude (m)"}],
        "tooltip": {"trigger": "item"},
        "dataset": [{"id": "track", "source": src_track}, {"id": "alt", "source": src_alt}],
        "series": [
            {"type": "line", "name": "Track", "datasetId": "track", "encode": {"x": 0, "y": 1}, "xAxisIndex": 0, "yAxisIndex": 0, "showSymbol": False, "lineStyle": {"width": 2, "color": "#4c4c4c"}, "tooltip": {"formatter": tooltip_fmt_code}, "sampling": "lttb"},
            {"type": "scatter", "name": "Points", "datasetId": "track", "encode": {"x": 0, "y": 1}, "xAxisIndex": 0, "yAxisIndex": 0, "symbolSize": 4, "itemStyle": {"color": "#4c4c4c", "opacity": 0.6}, "tooltip": {"formatter": tooltip_fmt_code}},
            {"type": "line", "name": "Altitude", "datasetId": "alt", "encode": {"x": 0, "y": 1}, "xAxisIndex": 1, "yAxisIndex": 1, "showSymbol": False, "lineStyle": {"width": 2, "color": "#666"}, "sampling": "lttb"},
        ],
        "animation": True,
        "useDirtyRect": True,
        "progressive": 2000,
        "progressiveThreshold": 4000,
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0, 1]},
            {"type": "slider", "xAxisIndex": [0, 1], "bottom": 10, "height": 16},
        ],
        "toolbox": {"right": 10, "feature": {"dataZoom": {"yAxisIndex": "none"}, "restore": {}, "saveAsImage": {}}},
    }
    return opt

def get_available_columns(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return []
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    exclude_cols = ["message_id", "uptime_seconds"]
    return [col for col in numeric_columns if col not in exclude_cols]

def create_dynamic_chart_option(df: pd.DataFrame, chart_config: Dict[str, Any]) -> Dict[str, Any]:
    if df.empty:
        return {"title": {"text": "No data available"}, "animation": False}

    chart_type = chart_config.get("chart_type", "line")
    x_col = chart_config.get("x_axis")
    y_col = chart_config.get("y_axis")
    title = chart_config.get("title", f"{y_col} vs {x_col}")

    if chart_type == "heatmap":
        numeric_cols = get_available_columns(df)
        if len(numeric_cols) < 2:
            return {"title": {"text": "Need at least 2 numeric columns"}, "animation": False}
        corr = df[numeric_cols].corr()
        xcats = list(corr.columns)
        ycats = list(corr.index)
        data = []
        for i, yc in enumerate(ycats):
            for j, xc in enumerate(xcats):
                data.append([j, i, float(corr.loc[yc, xc])])

        return {
            "title": {"text": "🔥 Correlation Heatmap"},
            "legend": {"top": 30},
            "tooltip": {"position": "top"},
            "grid": {"left": "8%", "right": "8%", "top": 70, "bottom": 50, "containLabel": True},
            "xAxis": {"type": "category", "data": xcats, "splitArea": {"show": True}, "axisLabel": {"rotate": 40}},
            "yAxis": {"type": "category", "data": ycats, "splitArea": {"show": True}},
            "visualMap": {
                "min": -1,
                "max": 1,
                "calculable": True,
                "orient": "horizontal",
                "left": "center",
                "bottom": 0,
                "inRange": {"color": ["#222", "#bbb", "#222"]},
            },
            "series": [{"name": "corr", "type": "heatmap", "data": data, "label": {"show": False}}],
            "animation": True,
            "useDirtyRect": True,
            "dataZoom": [{"type": "inside"}],  # not critical, but consistent
        }

    if not y_col or y_col not in df.columns:
        return {"title": {"text": "Invalid Y-axis selection"}, "animation": False}
    if chart_type != "histogram" and (x_col not in df.columns):
        return {"title": {"text": "Invalid X-axis selection"}, "animation": False}

    if chart_type == "histogram":
        vals = pd.to_numeric(df[y_col], errors="coerce").dropna().astype(float)
        if vals.empty:
            return {"title": {"text": f"No numeric data in {y_col}"}, "animation": False}
        hist, edges = np.histogram(vals, bins=30)
        centers = (edges[:-1] + edges[1:]) / 2.0
        src = [[float(centers[i]), int(hist[i])] for i in range(len(hist))]
        return {
            "title": {"text": f"Distribution of {y_col}"},
            "legend": {"top": 30},
            "tooltip": {"trigger": "axis"},
            "grid": {"left": "6%", "right": "6%", "top": 70, "bottom": 50, "containLabel": True},
            "xAxis": {"type": "value", "name": y_col},
            "yAxis": {"type": "value", "name": "Count"},
            "series": [{"type": "bar", "data": src, "barWidth": "70%"}],
            "animation": True,
            "useDirtyRect": True,
        }

    x_is_time = x_col == "timestamp"
    if x_is_time:
        x_vals = _ts_to_iso_list(df[x_col])
    else:
        x_vals = pd.to_numeric(df[x_col], errors="coerce").astype(float).tolist()
    y_vals = pd.to_numeric(df[y_col], errors="coerce").astype(float).tolist()
    src = [[x_vals[i], y_vals[i]] for i in range(len(y_vals))]

    x_axis = {"type": "time" if x_is_time else "value", "name": x_col}
    y_axis = {"type": "value", "name": y_col}

    series_def = {
        "type": "line" if chart_type == "line" else "scatter" if chart_type == "scatter" else "bar",
        "encode": {"x": 0, "y": 1},
        "showSymbol": chart_type != "bar",
        "lineStyle": {"width": 2} if chart_type in ("line",) else None,
        "sampling": "lttb" if chart_type == "line" else None,
        "smooth": 0.15 if chart_type == "line" else None,
        "datasetIndex": 0,
    }

    return {
        "title": {"text": title},
        "legend": {"top": 30},
        "tooltip": {"trigger": "axis" if chart_type in ("line", "bar") else "item"},
        "grid": {"left": "6%", "right": "6%", "top": 70, "bottom": 50, "containLabel": True},
        "xAxis": x_axis,
        "yAxis": y_axis,
        "dataset": {"source": src},
        "series": [series_def],
        "animation": True,
        "useDirtyRect": True,
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0]},
            {"type": "slider", "xAxisIndex": [0], "bottom": 10, "height": 16},
        ],
        "toolbox": {"right": 10, "feature": {"dataZoom": {"yAxisIndex": "none"}, "restore": {}, "saveAsImage": {}}},
    }

# ---------------------------
# UI sections
# ---------------------------
def render_overview_tab(kpis: Dict[str, float]):
    st.markdown("### 📊 Performance Overview")
    st.markdown("Real-time key performance indicators for your Shell Eco-marathon vehicle")
    render_kpi_header(kpis, unique_ns="overview", show_gauges=True)

def render_session_info(session_data: Dict[str, Any]):
    session_name = session_data.get("session_name") or "Unnamed"
    st.markdown(
        f"""
    <div class="card session-info">
        <h3>📊 Session Information</h3>
        <p>📛 <strong>Name:</strong> {session_name}</p>
        <p>📋 <strong>Session:</strong> {session_data['session_id'][:8]}...</p>
        <p>📅 <strong>Start:</strong> {session_data['start_time'].strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>⏱️ <strong>Duration:</strong> {str(session_data['duration']).split('.')[0]}</p>
        <p>📊 <strong>Records:</strong> {session_data['record_count']:,}</p>
    </div>
    """,
        unsafe_allow_html=True,
    )

def analyze_data_quality(df: pd.DataFrame, is_realtime: bool):
    if df.empty or len(df) < 10:
        st.session_state.data_quality_notifications = []
        return

    notifications = []
    logger = logging.getLogger("TelemetryDashboard")

    if is_realtime:
        try:
            last_timestamp = df["timestamp"].iloc[-1]
            now_utc = datetime.now(timezone.utc)
            time_since_last = (now_utc - last_timestamp).total_seconds()

            if len(df) > 2:
                time_diffs = df["timestamp"].diff().dt.total_seconds().dropna()
                avg_rate = time_diffs.tail(20).mean()
                if pd.isna(avg_rate) or avg_rate <= 0:
                    avg_rate = 1.0
            else:
                avg_rate = 1.0

            threshold = max(5.0, avg_rate * 5)

            if time_since_last > threshold:
                notifications.append(
                    f"🚨 **Data Stream Stalled:** No new data received for {int(time_since_last)}s. "
                    f"The data bridge might be disconnected. (Expected update every ~{avg_rate:.1f}s)"
                )
        except Exception as e:
            logger.warning(f"Could not perform stale data check: {e}")

    recent_df = df.tail(15)
    sensors_to_check = [
        "latitude",
        "longitude",
        "altitude",
        "voltage_v",
        "current_a",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "accel_x",
        "accel_y",
        "accel_z",
    ]
    failing_sensors = []
    all_sensors_failing = True

    for col in sensors_to_check:
        if col in recent_df.columns:
            sensor_data = recent_df[col].dropna()
            is_failing = False
            if len(sensor_data) < 5:
                all_sensors_failing = False
                continue
            if sensor_data.abs().max() < 1e-6 or sensor_data.std() < 1e-6:
                is_failing = True
            if is_failing:
                failing_sensors.append(col)
            else:
                all_sensors_failing = False
        else:
            all_sensors_failing = False

    if all_sensors_failing and len(failing_sensors) > 3:
        notifications.append(
            "🚨 **Critical Alert:** Multiple sensors (including "
            f"{', '.join(failing_sensors[:3])}...) are reporting static or zero values. "
            "This could indicate a major issue with the data bridge or power."
        )
    elif failing_sensors:
        sensor_list = ", ".join(failing_sensors)
        notifications.append(
            f"⚠️ **Sensor Anomaly:** The following sensor(s) may be unreliable, "
            f"showing static or zero values: **{sensor_list}**."
        )

    st.session_state.data_quality_notifications = notifications

def render_dynamic_charts_section(df: pd.DataFrame):
    if not st.session_state.chart_info_initialized:
        st.session_state.chart_info_text = """
        <div class="card">
            <h3>🎯 Create Custom Charts</h3>
            <p>Click <strong>"Add Chart"</strong> to create custom visualizations with your preferred variables and chart types.</p>
        </div>
        """
        st.session_state.chart_types_grid = """
        <div class="chart-type-grid">
            <div class="chart-type-card"><div class="chart-type-name">📈 Line Chart</div><div class="chart-type-desc">Great for time series data and trends</div></div>
            <div class="chart-type-card"><div class="chart-type-name">🔵 Scatter Plot</div><div class="chart-type-desc">Perfect for correlation analysis between variables</div></div>
            <div class="chart-type-card"><div class="chart-type-name">📊 Bar Chart</div><div class="chart-type-desc">Good for comparing values and discrete data</div></div>
            <div class="chart-type-card"><div class="chart-type-name">📉 Histogram</div><div class="chart-type-desc">Shows data distribution and frequency patterns</div></div>
            <div class="chart-type-card"><div class="chart-type-name">🔥 Heatmap</div><div class="chart-type-desc">Correlations between all numeric variables</div></div>
        </div>
        """
        st.session_state.chart_info_initialized = True

    st.markdown(st.session_state.chart_info_text, unsafe_allow_html=True)
    st.markdown(st.session_state.chart_types_grid, unsafe_allow_html=True)

    try:
        available_columns = get_available_columns(df)
        df_with_rp = calculate_roll_and_pitch(df)
        if "roll_deg" in df_with_rp.columns and "roll_deg" not in available_columns:
            available_columns.extend(["roll_deg", "pitch_deg"])
    except Exception as e:
        st.error(f"Error getting available columns: {e}")
        available_columns = []

    if not available_columns:
        st.warning("⏳ No numeric data available for creating charts. Connect and wait for data.")
        return

    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("➕ Add Chart", key="add_chart_btn", help="Create a new custom chart"):
            try:
                new_chart = {
                    "id": str(uuid.uuid4()),
                    "title": "New Chart",
                    "chart_type": "line",
                    "x_axis": "timestamp" if "timestamp" in df.columns else (available_columns[0] if available_columns else None),
                    "y_axis": available_columns[0] if available_columns else None,
                }
                st.session_state.dynamic_charts.append(new_chart)
                st.rerun()
            except Exception as e:
                st.error(f"Error adding chart: {e}")

    with col2:
        if st.session_state.dynamic_charts:
            st.success(f"📈 {len(st.session_state.dynamic_charts)} custom chart(s) active")

    if st.session_state.dynamic_charts:
        for i, chart_config in enumerate(list(st.session_state.dynamic_charts)):
            try:
                with st.container(border=True):
                    col1, col2, col3, col4, col5 = st.columns([2, 1.5, 1.5, 1.5, 0.5])

                    with col1:
                        new_title = st.text_input("Title", value=chart_config.get("title", "New Chart"), key=f"title_{chart_config['id']}")
                        if new_title != chart_config.get("title"):
                            st.session_state.dynamic_charts[i]["title"] = new_title

                    with col2:
                        new_type = st.selectbox(
                            "Type",
                            options=["line", "scatter", "bar", "histogram", "heatmap"],
                            index=["line", "scatter", "bar", "histogram", "heatmap"].index(chart_config.get("chart_type", "line")),
                            key=f"type_{chart_config['id']}",
                        )
                        if new_type != chart_config.get("chart_type"):
                            st.session_state.dynamic_charts[i]["chart_type"] = new_type

                    with col3:
                        if chart_config.get("chart_type", "line") not in ["histogram", "heatmap"]:
                            x_options = ["timestamp"] + available_columns if "timestamp" in df.columns else available_columns
                            current_x = chart_config.get("x_axis")
                            if current_x not in x_options and x_options:
                                current_x = x_options[0]
                            if x_options:
                                new_x = st.selectbox("X-Axis", options=x_options, index=x_options.index(current_x) if current_x in x_options else 0, key=f"x_{chart_config['id']}")
                                if new_x != chart_config.get("x_axis"):
                                    st.session_state.dynamic_charts[i]["x_axis"] = new_x
                            else:
                                st.empty()

                    with col4:
                        if chart_config.get("chart_type", "line") != "heatmap":
                            if available_columns:
                                current_y = chart_config.get("y_axis")
                                if current_y not in available_columns:
                                    current_y = available_columns[0]
                                new_y = st.selectbox("Y-Axis", options=available_columns, index=available_columns.index(current_y) if current_y in available_columns else 0, key=f"y_{chart_config['id']}")
                                if new_y != chart_config.get("y_axis"):
                                    st.session_state.dynamic_charts[i]["y_axis"] = new_y
                            else:
                                st.empty()

                    with col5:
                        if st.button("🗑️", key=f"delete_{chart_config['id']}", help="Delete chart"):
                            try:
                                idx_to_delete = next((j for j, cfg in enumerate(st.session_state.dynamic_charts) if cfg["id"] == chart_config["id"]), -1)
                                if idx_to_delete != -1:
                                    st.session_state.dynamic_charts.pop(idx_to_delete)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error deleting chart: {e}")

                    try:
                        df_with_rp = calculate_roll_and_pitch(df)
                        opt = create_dynamic_chart_option(df_with_rp, chart_config)
                        _st_echarts_render(opt, 400, key=f"chart_plot_{chart_config['id']}")
                    except Exception as e:
                        st.error(f"Error rendering chart: {e}")

            except Exception as e:
                st.error(f"Error rendering chart configuration: {e}")

# ---------------------------
# Main App
# ---------------------------
def main():
    st.markdown('<div class="main-header">🏎️ Shell Eco-marathon Telemetry Dashboard</div>', unsafe_allow_html=True)

    # One-time splash to stabilize the first render
    first_load_splash()

    if not ECHARTS_AVAILABLE or not PYECHARTS_AVAILABLE:
        st.error("ECharts/JsCode component missing. Install: pip install streamlit-echarts pyecharts")
        return

    initialize_session_state()

    # Sidebar
    with st.sidebar:
        st.header("🔧 Connection & Data Source")

        data_source_mode = st.radio(
            "📊 Data Source",
            options=["realtime_session", "historical"],
            format_func=lambda x: "🔴 Real-time + Session Data" if x == "realtime_session" else "📚 Historical Data",
            key="data_source_mode_radio",
        )

        if data_source_mode != st.session_state.data_source_mode:
            st.session_state.data_source_mode = data_source_mode
            st.session_state.telemetry_data = pd.DataFrame()
            st.session_state.is_viewing_historical = data_source_mode == "historical"
            st.session_state.selected_session = None
            st.session_state.current_session_id = None
            st.rerun()

        if st.session_state.data_source_mode == "realtime_session":
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔌 Connect", use_container_width=True):
                    if st.session_state.telemetry_manager:
                        st.session_state.telemetry_manager.disconnect()
                        time.sleep(0.5)
                    with st.spinner("Connecting..."):
                        st.session_state.telemetry_manager = EnhancedTelemetryManager()
                        supabase_connected = st.session_state.telemetry_manager.connect_supabase()
                        realtime_connected = False
                        if ABLY_AVAILABLE:
                            realtime_connected = st.session_state.telemetry_manager.connect_realtime()
                        if supabase_connected and realtime_connected:
                            st.success("✅ Connected!")
                        elif supabase_connected:
                            st.warning("⚠️ Supabase only connected (Ably not available or failed)")
                        else:
                            st.error("❌ Failed to connect to any service!")
                    st.rerun()

            with col2:
                if st.button("🛑 Disconnect", use_container_width=True):
                    if st.session_state.telemetry_manager:
                        st.session_state.telemetry_manager.disconnect()
                        st.session_state.telemetry_manager = None
                    st.info("🛑 Disconnected")
                    st.rerun()

            if st.session_state.telemetry_manager:
                stats = st.session_state.telemetry_manager.get_stats()
                if st.session_state.telemetry_manager.is_connected:
                    st.markdown('<div class="status-indicator">✅ Real-time Connected</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div class="status-indicator">❌ Real-time Disconnected</div>', unsafe_allow_html=True)

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("📨 Messages", stats["messages_received"])
                    st.metric("🔌 Attempts", stats["connection_attempts"])
                with col2:
                    st.metric("❌ Errors", stats["errors"])
                    if stats["last_message_time"]:
                        time_since = (datetime.now() - stats["last_message_time"]).total_seconds()
                        st.metric("⏱️ Last Msg", f"{time_since:.0f}s ago")
                    else:
                        st.metric("⏱️ Last Msg", "Never")
                if stats["last_error"]:
                    st.error(f"⚠️ {stats['last_error'][:120]}")

            st.divider()
            st.subheader("⚙️ Settings")
            auto_refresh_key = f"auto_refresh_{id(st.session_state)}"
            new_auto_refresh = st.checkbox(
                "🔄 Auto Refresh",
                value=st.session_state.auto_refresh,
                help="Automatically refresh data from real-time stream",
                key=auto_refresh_key,
            )
            if new_auto_refresh != st.session_state.auto_refresh:
                st.session_state.auto_refresh = new_auto_refresh

            if st.session_state.auto_refresh:
                refresh_options = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                current_index = refresh_options.index(3) if 3 in refresh_options else 2
                refresh_interval = st.selectbox(
                    "Refresh Rate (seconds)",
                    options=refresh_options,
                    index=current_index,
                    key=f"refresh_rate_{id(st.session_state)}",
                )
            else:
                refresh_interval = 3
            st.session_state.refresh_interval = refresh_interval

        else:
            st.markdown('<div class="status-indicator">📚 Historical Mode</div>', unsafe_allow_html=True)

            if not st.session_state.telemetry_manager:
                st.session_state.telemetry_manager = EnhancedTelemetryManager()
                st.session_state.telemetry_manager.connect_supabase()

            if st.button("🔄 Refresh Sessions", use_container_width=True):
                with st.spinner("Loading sessions..."):
                    st.session_state.historical_sessions = st.session_state.telemetry_manager.get_historical_sessions()
                st.rerun()

            if st.session_state.historical_sessions:
                session_options = []
                for session in st.session_state.historical_sessions:
                    name = session.get("session_name") or "Unnamed"
                    session_options.append(
                        f"{name} - {session['session_id'][:8]}... - {session['start_time'].strftime('%Y-%m-%d %H:%M')} ({session['record_count']:,} records)"
                    )

                selected_session_idx = st.selectbox(
                    "📋 Select Session",
                    options=range(len(session_options)),
                    format_func=lambda x: session_options[x],
                    key="session_selector",
                    index=0,
                )

                if selected_session_idx is not None:
                    selected_session = st.session_state.historical_sessions[selected_session_idx]

                    if (
                        st.session_state.selected_session is None
                        or st.session_state.selected_session["session_id"] != selected_session["session_id"]
                        or st.session_state.telemetry_data.empty
                    ):
                        st.session_state.selected_session = selected_session
                        st.session_state.is_viewing_historical = True
                        if selected_session["record_count"] > 10000:
                            st.info(
                                f"📊 Loading {selected_session['record_count']:,} records... This may take a moment due to pagination."
                            )

                        with st.spinner(f"Loading data for session {selected_session['session_id'][:8]}..."):
                            historical_df = st.session_state.telemetry_manager.get_historical_data(
                                selected_session["session_id"]
                            )
                            st.session_state.telemetry_data = historical_df
                            st.session_state.last_update = datetime.now()

                        if not historical_df.empty:
                            st.success(f"✅ Loaded {len(historical_df):,} data points")
                        st.rerun()
            else:
                st.info("Click 'Refresh Sessions' to load available sessions from Supabase.")

        st.info(f"📡 Channel: {DASHBOARD_CHANNEL_NAME}")
        st.info(f"🔢 Max records per session: {MAX_DATAPOINTS_PER_SESSION:,}")

    # Ingestion/update
    df = st.session_state.telemetry_data.copy()
    new_messages_count = 0

    if st.session_state.data_source_mode == "realtime_session":
        if st.session_state.telemetry_manager and st.session_state.telemetry_manager.is_connected:
            new_messages = st.session_state.telemetry_manager.get_realtime_messages()

            current_session_data_from_supabase = pd.DataFrame()
            if new_messages and "session_id" in new_messages[0]:
                current_session_id = new_messages[0]["session_id"]
                if st.session_state.current_session_id != current_session_id or st.session_state.telemetry_data.empty:
                    st.session_state.current_session_id = current_session_id
                    with st.spinner(f"Loading current session data for {current_session_id[:8]}..."):
                        current_session_data_from_supabase = st.session_state.telemetry_manager.get_current_session_data(
                            current_session_id
                        )
                    if not current_session_data_from_supabase.empty:
                        st.success(
                            f"✅ Loaded {len(current_session_data_from_supabase):,} historical points for current session"
                        )

            if new_messages or not current_session_data_from_supabase.empty:
                merged_data = merge_telemetry_data(
                    new_messages, current_session_data_from_supabase, st.session_state.telemetry_data
                )
                if not merged_data.empty:
                    new_messages_count = len(new_messages) if new_messages else 0
                    st.session_state.telemetry_data = merged_data
                    st.session_state.last_update = datetime.now()

        st.session_state.is_viewing_historical = False
    else:
        st.session_state.is_viewing_historical = True

    df = st.session_state.telemetry_data.copy()

    if st.session_state.is_viewing_historical and st.session_state.selected_session:
        st.markdown(
            '<div class="historical-notice">📚 Viewing Historical Data - No auto-refresh active</div>',
            unsafe_allow_html=True,
        )
        render_session_info(st.session_state.selected_session)

    if df.empty:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.warning("⏳ Waiting for telemetry data...")
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.data_source_mode == "realtime_session":
                st.info(
                    "Getting Started (Real-time):\n"
                    "1) Ensure your data bridge script is running.\n"
                    "2) Click 'Connect' in the sidebar.\n"
                    "3) Real-time updates auto-merge with session history.\n"
                    "4) Use the top segmented selector to navigate sections."
                )
            else:
                st.info(
                    "Getting Started (Historical):\n"
                    "1) Click 'Refresh Sessions' to load sessions from Supabase.\n"
                    "2) Pick a session to load all data (uses pagination).\n"
                    "3) Navigate sections with the selector above."
                )
        with col2:
            with st.expander("🔍 Debug Information"):
                debug_info = {
                    "Data Source Mode": st.session_state.data_source_mode,
                    "Is Viewing Historical": st.session_state.is_viewing_historical,
                    "Selected Session ID": st.session_state.selected_session["session_id"][:8] + "..."
                    if st.session_state.selected_session
                    else None,
                    "Current Real-time Session ID": st.session_state.current_session_id,
                    "Number of Historical Sessions": len(st.session_state.historical_sessions),
                    "Telemetry Data Points (in memory)": len(st.session_state.telemetry_data),
                    "Max Datapoints Per Session": MAX_DATAPOINTS_PER_SESSION,
                    "Max Rows Per Request": SUPABASE_MAX_ROWS_PER_REQUEST,
                }
                if st.session_state.telemetry_manager:
                    stats = st.session_state.telemetry_manager.get_stats()
                    debug_info.update(
                        {
                            "Ably Connected (Manager Status)": st.session_state.telemetry_manager.is_connected,
                            "Messages Received (via Ably)": stats["messages_received"],
                            "Connection Errors": stats["errors"],
                            "Total Pagination Requests": stats["pagination_stats"]["total_requests"],
                            "Total Rows Fetched": stats["pagination_stats"]["total_rows_fetched"],
                            "Sessions Requiring Pagination": stats["pagination_stats"]["sessions_paginated"],
                            "Largest Session Size": stats["pagination_stats"]["largest_session_size"],
                        }
                    )
                st.json(debug_info)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    analyze_data_quality(df, is_realtime=(st.session_state.data_source_mode == "realtime_session"))
    if st.session_state.data_quality_notifications:
        for msg in st.session_state.data_quality_notifications:
            if "🚨" in msg:
                st.error(msg, icon="🚨")
            else:
                st.warning(msg, icon="⚠️")

    kpis = calculate_kpis(df)

    # Radio-based navigation (styled as tabs)
    TAB_NAMES = ["📊 Overview", "🚗 Speed", "⚡ Power", "🎮 IMU", "🎮 IMU Detail", "⚡ Efficiency", "🛰️ GPS", "📈 Custom", "📃 Data"]
    if "active_tab" not in st.session_state:
        st.session_state.active_tab = TAB_NAMES[0]
    active = st.radio("Sections", options=TAB_NAMES, index=TAB_NAMES.index(st.session_state.active_tab), horizontal=True, key="active_tab_radio")
    st.session_state.active_tab = active

    # Render selected panel
    if active == "📊 Overview":
        render_overview_tab(kpis)

    elif active == "🚗 Speed":
        render_live_gauges(kpis, unique_ns="speedtab")
        render_kpi_header(kpis, unique_ns="speedtab", show_gauges=False)
        opt = create_speed_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        _st_echarts_render(opt, 420, key="chart_speed_main")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active == "⚡ Power":
        render_live_gauges(kpis, unique_ns="powertab")
        render_kpi_header(kpis, unique_ns="powertab", show_gauges=False)
        opt = create_power_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        _st_echarts_render(opt, 500, key="chart_power_main")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active == "🎮 IMU":
        render_live_gauges(kpis, unique_ns="imutab")
        render_kpi_header(kpis, unique_ns="imutab", show_gauges=False)
        opt = create_imu_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        _st_echarts_render(opt, 620, key="chart_imu_main")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active == "🎮 IMU Detail":
        render_live_gauges(kpis, unique_ns="imudetailtab")
        render_kpi_header(kpis, unique_ns="imudetailtab", show_gauges=False)
        opt = create_imu_detail_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        _st_echarts_render(opt, 740, key="chart_imu_detail_main")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active == "⚡ Efficiency":
        render_live_gauges(kpis, unique_ns="efftab")
        render_kpi_header(kpis, unique_ns="efftab", show_gauges=False)
        opt = create_efficiency_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        _st_echarts_render(opt, 460, key="chart_efficiency_main")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active == "🛰️ GPS":
        render_live_gauges(kpis, unique_ns="gpstab")
        render_kpi_header(kpis, unique_ns="gpstab", show_gauges=False)
        opt = create_gps_map_with_altitude_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        _st_echarts_render(opt, 540, key="chart_gps_main")
        st.markdown("</div>", unsafe_allow_html=True)

    elif active == "📈 Custom":
        render_live_gauges(kpis, unique_ns="customtab")
        render_kpi_header(kpis, unique_ns="customtab", show_gauges=False)
        render_dynamic_charts_section(df)

    elif active == "📃 Data":
        render_live_gauges(kpis, unique_ns="datatabletab")
        render_kpi_header(kpis, unique_ns="datatabletab", show_gauges=False)
        st.subheader("📃 Raw Telemetry Data")
        if len(df) > 1000:
            st.info(f"ℹ️ Showing last 100 from all {len(df):,} data points below.")
        else:
            st.info(f"ℹ️ Showing last 100 from all {len(df):,} data points below.")
        display_df = df.tail(100) if len(df) > 100 else df
        st.dataframe(display_df, use_container_width=True, height=400)

        col1, col2 = st.columns(2)
        with col1:
            csv = df.to_csv(index=False)
            st.download_button(
                label=f"📥 Download Full CSV ({len(df):,} rows)",
                data=csv,
                file_name=f"telemetry_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with col2:
            if len(df) > 1000:
                sample_df = df.sample(n=min(1000, len(df)), random_state=42)
                st.download_button(
                    label="📥 Download Sample CSV (1000 rows)",
                    data=sample_df.to_csv(index=False),
                    file_name=f"telemetry_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

    if st.session_state.data_source_mode == "realtime_session" and st.session_state.auto_refresh:
        if AUTOREFRESH_AVAILABLE:
            st_autorefresh(interval=st.session_state.refresh_interval * 1000, key="auto_refresh")
        else:
            st.warning("🔄 To enable smooth auto-refresh install:\n`pip install streamlit-autorefresh`")

    st.divider()
    st.markdown(
        "<div style='text-align: center; opacity: 0.8; padding: 1rem;'>"
        "<p>Shell Eco-marathon Telemetry Dashboard</p>"
        "</div>",
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    main()
