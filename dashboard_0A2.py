# telem_dashboard_echarts.py
# Streamlit + ECharts (HTML charts) real-time/historical telemetry dashboard
# Dependencies:
#   pip install streamlit streamlit-echarts ably supabase matplotlib pandas numpy plotly (plotly only for colors? not used)
#   Note: Plotly is no longer used for charts. We keep matplotlib.colors only for color parsing

import streamlit as st
import streamlit.components.v1 as components
from streamlit_echarts import st_echarts, JsCode

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

# Handles imports with error checking
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

# Disables tracemalloc warnings
warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*tracemalloc.*")

# Configuration
DASHBOARD_ABLY_API_KEY = "DxuYSw.fQHpug:sa4tOcqWDkYBW9ht56s7fT0G091R1fyXQc6mc8WthxQ"
DASHBOARD_CHANNEL_NAME = "telemetry-dashboard-channel"
SUPABASE_URL = "https://dsfmdziehhgmrconjcns.supabase.co"
SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRzZm1kemllaGhnbXJjb25qY25zIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MDEyOTIsImV4cCI6MjA2NzQ3NzI5Mn0.P41bpLkP0tKpTktLx6hFOnnyrAB9N_yihQP1v6zTRwc"
SUPABASE_TABLE_NAME = "telemetry"

# Pagination constants
SUPABASE_MAX_ROWS_PER_REQUEST = 1000
MAX_DATAPOINTS_PER_SESSION = 1000000

# Configures the Streamlit page
st.set_page_config(
    page_title="🏎️ Shell Eco-marathon Telemetry Dashboard (ECharts)",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/your-repo",
        "Report a bug": "https://github.com/your-repo/issues",
        "About": "Shell Eco-marathon Telemetry Dashboard (ECharts)",
    },
)


# -------------------------------------------------------
# Theme-aware CSS (kept from your original for UI/branding)
# -------------------------------------------------------
def get_theme_aware_css():
    return """
<style>
:root {
  color-scheme: light dark;
  --brand-1: 222 35% 56%;
  --brand-2: 280 32% 62%;
  --accent-1: 158 30% 52%;
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

.status-indicator {
  display:flex; align-items:center; justify-content:center;
  padding:.55rem .9rem; border-radius:999px; font-weight:700; font-size:.9rem;
  border:1px solid var(--glass-border); background: var(--glass-strong);
  backdrop-filter: blur(10px) saturate(130%); box-shadow: var(--shadow-1);
}

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

.historical-notice,.pagination-info { border-radius:14px; padding:.9rem 1rem; font-weight:700;
  border:1px solid var(--border); background: var(--glass); }

.widget-grid { display:grid; grid-template-columns: repeat(6, 1fr); gap:1rem; margin-top: .75rem; }
.gauge-container { text-align:center; padding:.75rem; border-radius:16px; border:1px solid var(--glass-border);
  background:
    radial-gradient(120% 120% at 85% 15%, color-mix(in oklab, hsl(var(--brand-1)) 4%, transparent), transparent 60%),
    radial-gradient(120% 130% at 20% 80%, color-mix(in oklab, hsl(var(--brand-2)) 4%, transparent), transparent 60%),
    var(--glass);
  backdrop-filter: blur(10px); transition: transform .2s ease, border-color .2s ease, background .2s ease; }
.gauge-container:hover { transform: translateY(-2px); border-color: var(--border); }
.gauge-title { font-size:.85rem; font-weight:600; color:var(--text-subtle); margin-bottom:.25rem; }

.chart-wrap { border-radius:18px; border:1px solid var(--glass-border);
  background:
    radial-gradient(110% 120% at 85% 10%, color-mix(in oklab, hsl(var(--brand-1)) 3%, transparent), transparent 60%),
    var(--glass);
  padding:.75rem; box-shadow: var(--shadow-1); }

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

.stTabs [data-baseweb="tab-list"] { border-bottom: 1px solid var(--border); gap:6px; }
.stTabs [data-baseweb="tab"] { border:none; border-radius:10px 10px 0 0; background: transparent; color: var(--text-muted);
  font-weight:600; padding:.6rem 1rem; transition: color .2s ease, background .2s ease; }
.stTabs [data-baseweb="tab"]:hover { color: var(--text); background: var(--glass); }
.stTabs [data-baseweb="tab"][aria-selected="true"] { color: color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text)); box-shadow: inset 0 -3px 0 0 color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text)); }

.chart-type-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap:.75rem; }
.chart-type-card { border-radius:16px; padding:1rem; border:1px solid var(--glass-border); box-shadow: var(--shadow-1);
  background:
    radial-gradient(130% 120% at 20% 15%, color-mix(in oklab, hsl(var(--brand-2)) 4%, transparent), transparent 60%),
    var(--glass);
}
.chart-type-name { font-weight:800; background: linear-gradient(90deg,
                 color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text)),
                 color-mix(in oklab, hsl(var(--brand-2)) 60%, var(--text)));
                 -webkit-background-clip:text; background-clip:text; color: transparent; }
.chart-type-desc { color: var(--text-muted); }

[data-testid="stDataFrame"], [data-testid="stExpander"], [data-testid="stAlert"] {
  border-radius:16px; border:1px solid var(--border);
  background:
    radial-gradient(120% 120% at 80% 10%, color-mix(in oklab, hsl(var(--brand-1)) 3%, transparent), transparent 60%),
    var(--glass);
  backdrop-filter: blur(10px);
}

div[data-testid="stMetric"] {
  position: relative;
  border-radius: 18px;
  padding: 1rem 1.1rem;
  background:
    radial-gradient(120% 140% at 10% 0%, color-mix(in oklab, hsl(var(--brand-1)) 7%, transparent), transparent 60%),
    radial-gradient(140% 120% at 90% 100%, color-mix(in oklab, hsl(var(--brand-2)) 7%, transparent), transparent 60%),
    var(--glass);
  backdrop-filter: blur(14px) saturate(140%);
  border: 1px solid var(--glass-border);
  box-shadow: var(--shadow-1);
}
div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
  font-weight: 700;
  padding: .15rem .45rem;
  border-radius: 999px;
  background: color-mix(in oklab, var(--ok) 10%, transparent);
}

[data-testid="stSidebar"] > div { background: var(--glass-strong); border-right:1px solid var(--glass-border); backdrop-filter: blur(18px) saturate(140%); }

label, .stTextInput, .stSelectbox, .stNumberInput, .stSlider { color: var(--text); }
div[data-baseweb="input"] > div { background: var(--glass); border-radius:10px; border:1px solid var(--border); }

::-webkit-scrollbar { width: 10px; height: 10px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border-strong); border-radius: 6px; }
::-webkit-scrollbar-thumb:hover { background: color-mix(in oklab, hsl(var(--brand-1)) 50%, var(--border-strong)); }

*:focus-visible { outline: 2px solid color-mix(in oklab, hsl(var(--brand-1)) 55%, var(--text)); outline-offset:2px; border-radius:4px; }
</style>
"""


# Apply CSS
st.markdown(get_theme_aware_css(), unsafe_allow_html=True)


# Logger setup
def setup_terminal_logging():
    logger = logging.getLogger("TelemetryDashboard")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)


setup_terminal_logging()


class EnhancedTelemetryManager:
    """Telemetry manager with multi-source data integration and pagination support."""

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

    def _paginated_fetch(self, session_id: str, data_source: str = "supabase_current"):
        try:
            if not self.supabase_client:
                self.logger.error("❌ Supabase client not initialized")
                return pd.DataFrame()

            all_data = []
            offset = 0
            total_fetched = 0
            request_count = 0

            self.logger.info(
                f"🔄 Starting paginated fetch for session {session_id[:8]}..."
            )

            while offset < MAX_DATAPOINTS_PER_SESSION:
                try:
                    range_end = offset + SUPABASE_MAX_ROWS_PER_REQUEST - 1

                    self.logger.info(
                        f"📄 Fetching page {request_count + 1}: rows {offset}-{range_end}"
                    )

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
                        self.logger.info(f"✅ No more data found at offset {offset}")
                        break

                    batch_size = len(response.data)
                    all_data.extend(response.data)
                    total_fetched += batch_size

                    self.logger.info(
                        f"📊 Fetched {batch_size} rows (total: {total_fetched})"
                    )

                    if batch_size < SUPABASE_MAX_ROWS_PER_REQUEST:
                        self.logger.info("✅ Reached end of data")
                        break

                    offset += SUPABASE_MAX_ROWS_PER_REQUEST
                    time.sleep(0.1)

                except Exception as e:
                    self.logger.error(
                        f"❌ Error in pagination request {request_count}: {e}"
                    )
                    offset += SUPABASE_MAX_ROWS_PER_REQUEST
                    continue

            with self._lock:
                self.stats["pagination_stats"]["total_requests"] += request_count
                self.stats["pagination_stats"]["total_rows_fetched"] += total_fetched
                self.stats["pagination_stats"]["largest_session_size"] = max(
                    self.stats["pagination_stats"]["largest_session_size"],
                    total_fetched,
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

            self.logger.info("🔄 Fetching historical sessions list...")

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
                    self.logger.error(
                        f"❌ Error fetching session records at offset {offset}: {e}"
                    )
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
                    start_dt = datetime.fromisoformat(
                        session_info["start_time"].replace("Z", "+00:00")
                    )
                    end_dt = datetime.fromisoformat(
                        session_info["end_time"].replace("Z", "+00:00")
                    )
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
                    self.logger.error(
                        f"❌ Error processing session {session_info['session_id']}: {e}"
                    )

            sorted_sessions = sorted(
                session_list, key=lambda x: x["start_time"], reverse=True
            )
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


def merge_telemetry_data(
    realtime_data: List[Dict],
    supabase_data: pd.DataFrame,
    streamlit_history: pd.DataFrame,
) -> pd.DataFrame:
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
        "pagination_info": {
            "is_loading": False,
            "current_session": None,
            "total_requests": 0,
            "total_rows": 0,
        },
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
        df_calc[["roll_rad", "roll_deg", "pitch_rad", "pitch_deg"]] = df_calc[
            ["roll_rad", "roll_deg", "pitch_rad", "pitch_deg"]
        ].replace([np.inf, -np.inf, np.nan], 0)
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
                kpis["current_speed_ms"] = max(0, speed_data.iloc[-1])
                kpis["max_speed_ms"] = max(0, speed_data.max())
                kpis["avg_speed_ms"] = max(0, speed_data.mean())

        kpis["current_speed_kmh"] = kpis["current_speed_ms"] * 3.6
        kpis["max_speed_kmh"] = kpis["max_speed_ms"] * 3.6
        kpis["avg_speed_kmh"] = kpis["avg_speed_ms"] * 3.6

        if "distance_m" in df.columns and not df["distance_m"].dropna().empty:
            kpis["total_distance_km"] = max(0, df["distance_m"].dropna().iloc[-1] / 1000)

        if "energy_j" in df.columns and not df["energy_j"].dropna().empty:
            kpis["total_energy_kwh"] = max(0, df["energy_j"].dropna().iloc[-1] / 3_600_000)

        if "power_w" in df.columns:
            power_data = df["power_w"].dropna()
            if not power_data.empty:
                kpis["avg_power_w"] = max(0, power_data.mean())

        if kpis["total_energy_kwh"] > 0:
            kpis["efficiency_km_per_kwh"] = (
                kpis["total_distance_km"] / kpis["total_energy_kwh"]
            )

        if "voltage_v" in df.columns:
            voltage_data = df["voltage_v"].dropna()
            if not voltage_data.empty:
                kpis["battery_voltage_v"] = max(0, voltage_data.iloc[-1])
                nominal_voltage = 50.4
                max_voltage = 58.5
                min_voltage = 50.4
                current_voltage = kpis["battery_voltage_v"]
                if current_voltage > min_voltage:
                    kpis["battery_percentage"] = min(
                        100,
                        max(
                            0,
                            ((current_voltage - min_voltage) / (max_voltage - min_voltage))
                            * 100,
                        ),
                    )

        if "current_a" in df.columns:
            curr_data = df["current_a"].dropna()
            if not curr_data.empty:
                kpis["avg_current_a"] = max(0.0, curr_data.mean())
                kpis["c_current_a"] = max(0.0, curr_data.iloc[-1])
            else:
                kpis["c_current_a"] = 0.0

        if "roll_deg" in df.columns:
            roll_data = df["roll_deg"].dropna()
            if not roll_data.empty:
                kpis["current_roll_deg"] = roll_data.iloc[-1]
                kpis["max_roll_deg"] = roll_data.abs().max()
        if "pitch_deg" in df.columns:
            pitch_data = df["pitch_deg"].dropna()
            if not pitch_data.empty:
                kpis["current_pitch_deg"] = pitch_data.iloc[-1]
                kpis["max_pitch_deg"] = pitch_data.abs().max()

        return kpis

    except Exception as e:
        st.error(f"Error calculating KPIs: {e}")
        return default_kpis


# ------------------------------
# ECharts chart builders (HTML)
# ------------------------------

def _rgb_tuple(color_hex: str) -> Tuple[int, int, int]:
    rgb = mcolors.to_rgb(color_hex)
    return int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255)


from streamlit_echarts import JsCode

def create_echarts_gauge_option(
    value: float,
    max_val: Optional[float],
    title: str,
    color: str,
    suffix: str = "",
    avg_ref: Optional[float] = None,
    thresh_val: Optional[float] = None,
):
    if max_val is None or max_val <= 0:
        max_val = value * 1.2 if value > 0 else 1.0

    r, g, b = _rgb_tuple(color)
    main_color = f"rgb({r},{g},{b})"

    # ✅ FIX: one-liner JS + use .js_code so it's JSON serializable
    formatter_code = f"function (v) {{ return (Math.round(v*10)/10) + '{suffix}'; }}"
    detail_formatter = JsCode(formatter_code).js_code

    axis_color = (
        [
            [min(max((thresh_val or 0) / max_val, 0), 1), main_color],
            [1, "rgba(120,120,120,0.25)"],
        ]
        if thresh_val is not None
        else [[1, main_color]]
    )

    option = {
        "tooltip": {"show": False},
        "series": [
            {
                "type": "gauge",
                "startAngle": 225,
                "endAngle": -45,
                "min": 0,
                "max": max_val,
                "splitNumber": 6,
                "axisLine": {"lineStyle": {"width": 8, "color": axis_color}},
                "pointer": {"icon": "path://M2,0 L-2,0 L0,-80 Z", "length": "65%"},
                "progress": {"show": True, "width": 8, "itemStyle": {"color": main_color}},
                "axisTick": {"show": False},
                "splitLine": {"show": False},
                "axisLabel": {"show": False},
                "anchor": {"show": False},
                "title": {"show": True, "offsetCenter": [0, "60%"], "fontSize": 12},
                "detail": {
                    "valueAnimation": True,
                    "formatter": detail_formatter,  # ✅ now a string, not a JsCode object
                    "color": "#fff",
                    "fontSize": 16,
                },
                "data": [{"value": float(value), "name": title}],
            }
        ],
        "textStyle": {"color": "#ddd"},
        "backgroundColor": "transparent",
    }

    return option

def _to_echarts_time_series(df: pd.DataFrame, x_col: str, y_col: str) -> List[List]:
    """Return [[ts_ms, value], ...] for ECharts time axis."""
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return []
    x = pd.to_datetime(df[x_col], errors="coerce", utc=True)
    y = pd.to_numeric(df[y_col], errors="coerce")
    mask = (~x.isna()) & (~y.isna())
    xs = (x[mask].astype(np.int64) // 10**6).tolist()
    ys = y[mask].tolist()
    return [[xs[i], ys[i]] for i in range(len(xs))]


def create_speed_chart_option(df: pd.DataFrame):
    data = _to_echarts_time_series(df, "timestamp", "speed_ms")
    if not data:
        return {
            "title": {"text": "🚗 Vehicle Speed Over Time"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No speed data available", "fill": "#999"},
                }
            ],
        }
    option = {
        "title": {"text": "🚗 Vehicle Speed Over Time"},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "time"},
        "yAxis": {"type": "value", "name": "Speed (m/s)"},
        "grid": {"left": "8%", "right": "4%", "top": 50, "bottom": 40},
        "series": [
            {
                "type": "line",
                "showSymbol": False,
                "smooth": True,
                "areaStyle": {"opacity": 0.08},
                "lineStyle": {"color": "#1f77b4", "width": 2},
                "data": data,
            }
        ],
    }
    return option


def create_power_chart_option(df: pd.DataFrame):
    need_cols = ["timestamp", "voltage_v", "current_a", "power_w"]
    if df.empty or not all(c in df.columns for c in need_cols):
        return {
            "title": {"text": "⚡ Electrical System Performance"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No power data available", "fill": "#999"},
                }
            ],
        }

    t_v = _to_echarts_time_series(df, "timestamp", "voltage_v")
    t_c = _to_echarts_time_series(df, "timestamp", "current_a")
    t_p = _to_echarts_time_series(df, "timestamp", "power_w")

    option = {
        "title": {"text": "⚡ Electrical System Performance"},
        "tooltip": {"trigger": "axis"},
        "grid": [
            {"left": "8%", "right": "6%", "top": 60, "height": "32%"},
            {"left": "8%", "right": "6%", "top": "55%", "height": "32%"},
        ],
        "xAxis": [
            {"type": "time", "gridIndex": 0},
            {"type": "time", "gridIndex": 1},
        ],
        "yAxis": [
            {"type": "value", "name": "Voltage (V)", "gridIndex": 0},
            {"type": "value", "name": "Current (A)", "gridIndex": 0, "position": "right"},
            {"type": "value", "name": "Power (W)", "gridIndex": 1},
        ],
        "series": [
            {
                "name": "Voltage (V)",
                "type": "line",
                "showSymbol": False,
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "lineStyle": {"color": "#2ca02c", "width": 2},
                "data": t_v,
            },
            {
                "name": "Current (A)",
                "type": "line",
                "showSymbol": False,
                "xAxisIndex": 0,
                "yAxisIndex": 1,
                "lineStyle": {"color": "#d62728", "width": 2},
                "data": t_c,
            },
            {
                "name": "Power (W)",
                "type": "line",
                "showSymbol": False,
                "xAxisIndex": 1,
                "yAxisIndex": 2,
                "lineStyle": {"color": "#ff7f0e", "width": 2},
                "data": t_p,
            },
        ],
        "legend": {"top": 30},
    }
    return option


def create_imu_chart_option(df: pd.DataFrame):
    need_cols = [
        "timestamp",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "accel_x",
        "accel_y",
        "accel_z",
    ]
    if df.empty or not all(c in df.columns for c in need_cols):
        return {
            "title": {"text": "⚡ IMU System Performance with Roll & Pitch"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No IMU data available", "fill": "#999"},
                }
            ],
        }

    df_rp = calculate_roll_and_pitch(df)

    def ts_series(ycol):
        return _to_echarts_time_series(df_rp, "timestamp", ycol)

    gyros = ["gyro_x", "gyro_y", "gyro_z"]
    accs = ["accel_x", "accel_y", "accel_z"]

    colors_gyro = ["#e74c3c", "#2ecc71", "#3498db"]
    colors_accel = ["#f39c12", "#9b59b6", "#34495e"]

    option = {
        "title": {"text": "⚡ IMU System Performance with Roll & Pitch"},
        "tooltip": {"trigger": "axis"},
        "grid": [
            {"left": "7%", "right": "5%", "top": 60, "height": "24%"},
            {"left": "7%", "right": "5%", "top": "42%", "height": "24%"},
            {"left": "7%", "right": "5%", "top": "73%", "height": "22%"},
        ],
        "xAxis": [
            {"type": "time", "gridIndex": 0},
            {"type": "time", "gridIndex": 1},
            {"type": "time", "gridIndex": 2},
        ],
        "yAxis": [
            {"type": "value", "name": "Gyro (deg/s)", "gridIndex": 0},
            {"type": "value", "name": "Accel (m/s²)", "gridIndex": 1},
            {"type": "value", "name": "Angle (°)", "gridIndex": 2},
        ],
        "series": [],
        "legend": {"top": 30},
    }

    for i, g in enumerate(gyros):
        option["series"].append(
            {
                "name": f"Gyro {g[-1].upper()}",
                "type": "line",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "showSymbol": False,
                "lineStyle": {"color": colors_gyro[i], "width": 2},
                "data": ts_series(g),
            }
        )

    for i, a in enumerate(accs):
        option["series"].append(
            {
                "name": f"Accel {a[-1].upper()}",
                "type": "line",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "showSymbol": False,
                "lineStyle": {"color": colors_accel[i], "width": 2},
                "data": ts_series(a),
            }
        )

    option["series"].append(
        {
            "name": "Roll (°)",
            "type": "line",
            "xAxisIndex": 2,
            "yAxisIndex": 2,
            "showSymbol": False,
            "lineStyle": {"color": "#e377c2", "width": 3},
            "data": ts_series("roll_deg"),
        }
    )
    option["series"].append(
        {
            "name": "Pitch (°)",
            "type": "line",
            "xAxisIndex": 2,
            "yAxisIndex": 2,
            "showSymbol": False,
            "lineStyle": {"color": "#17becf", "width": 3},
            "data": ts_series("pitch_deg"),
        }
    )
    return option


def create_imu_detail_chart_option(df: pd.DataFrame):
    need_cols = [
        "timestamp",
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "accel_x",
        "accel_y",
        "accel_z",
    ]
    if df.empty or not all(c in df.columns for c in need_cols):
        return {
            "title": {"text": "🎮 Detailed IMU Sensor Analysis with Roll & Pitch"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No IMU data available", "fill": "#999"},
                }
            ],
        }

    df_rp = calculate_roll_and_pitch(df)

    # Build a 3x3 grid of charts: Gyro X/Y/Z, Accel X/Y/Z, Roll, Pitch, Combined
    grids = []
    xaxes = []
    yaxes = []
    series = []

    titles = [
        "🌀 Gyro X",
        "🌀 Gyro Y",
        "🌀 Gyro Z",
        "📊 Accel X",
        "📊 Accel Y",
        "📊 Accel Z",
        "🔄 Roll (°)",
        "📐 Pitch (°)",
        "🎯 R&P Combined",
    ]
    cols = 3
    rows = 3
    # Layout percentages
    top_margin = 60
    vert_gap = 8
    left_margin = 7
    right_margin = 5
    col_gap_pct = 1.5

    # Compute grid rects
    for r in range(rows):
        for c in range(cols):
            top_pct = top_margin + r * 30  # 30% height per row approx
            left_pct = left_margin + c * (30 + col_gap_pct)
            grids.append(
                {
                    "left": f"{left_pct}%",
                    "top": f"{top_pct}%",
                    "width": "28%",
                    "height": "24%",
                }
            )

    for i in range(9):
        xaxes.append({"type": "time", "gridIndex": i})
        yaxes.append({"type": "value", "gridIndex": i})

    def ts(ycol):
        return _to_echarts_time_series(df_rp, "timestamp", ycol)

    gyros = [("gyro_x", "#e74c3c"), ("gyro_y", "#2ecc71"), ("gyro_z", "#3498db")]
    accs = [("accel_x", "#f39c12"), ("accel_y", "#9b59b6"), ("accel_z", "#34495e")]

    # Gyro panels 0,1,2
    for idx in range(3):
        y, col = gyros[idx]
        series.append(
            {
                "name": titles[idx],
                "type": "line",
                "xAxisIndex": idx,
                "yAxisIndex": idx,
                "showSymbol": False,
                "lineStyle": {"color": col, "width": 2},
                "data": ts(y),
            }
        )

    # Accel panels 3,4,5
    for idx in range(3):
        grid_idx = 3 + idx
        y, col = accs[idx]
        series.append(
            {
                "name": titles[grid_idx],
                "type": "line",
                "xAxisIndex": grid_idx,
                "yAxisIndex": grid_idx,
                "showSymbol": False,
                "lineStyle": {"color": col, "width": 2},
                "data": ts(y),
            }
        )

    # Roll 6
    series.append(
        {
            "name": titles[6],
            "type": "line",
            "xAxisIndex": 6,
            "yAxisIndex": 6,
            "showSymbol": False,
            "lineStyle": {"color": "#e377c2", "width": 3},
            "data": ts("roll_deg"),
        }
    )
    # Pitch 7
    series.append(
        {
            "name": titles[7],
            "type": "line",
            "xAxisIndex": 7,
            "yAxisIndex": 7,
            "showSymbol": False,
            "lineStyle": {"color": "#17becf", "width": 3},
            "data": ts("pitch_deg"),
        }
    )
    # Combined 8 (roll + pitch)
    series.append(
        {
            "name": "Roll (°)",
            "type": "line",
            "xAxisIndex": 8,
            "yAxisIndex": 8,
            "showSymbol": False,
            "lineStyle": {"color": "#e377c2", "width": 2},
            "data": ts("roll_deg"),
        }
    )
    series.append(
        {
            "name": "Pitch (°)",
            "type": "line",
            "xAxisIndex": 8,
            "yAxisIndex": 8,
            "showSymbol": False,
            "lineStyle": {"color": "#17becf", "width": 2},
            "data": ts("pitch_deg"),
        }
    )

    option = {
        "title": {"text": "🎮 Detailed IMU Sensor Analysis with Roll & Pitch"},
        "tooltip": {"trigger": "axis"},
        "grid": grids,
        "xAxis": xaxes,
        "yAxis": yaxes,
        "series": series,
        "legend": {"top": 30},
    }
    return option


def create_efficiency_chart_option(df: pd.DataFrame):
    need_cols = ["speed_ms", "power_w"]
    if df.empty or not all(c in df.columns for c in need_cols):
        return {
            "title": {"text": "⚡ Efficiency Analysis: Speed vs Power Consumption"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No efficiency data available", "fill": "#999"},
                }
            ],
        }

    # Prepare data [speed, power, voltage]
    speed = pd.to_numeric(df["speed_ms"], errors="coerce")
    power = pd.to_numeric(df["power_w"], errors="coerce")
    volt = pd.to_numeric(df["voltage_v"], errors="coerce") if "voltage_v" in df.columns else None
    mask = (~speed.isna()) & (~power.isna())
    if volt is not None:
        mask = mask & (~volt.isna())
    data = []
    vmin, vmax = None, None
    if volt is not None:
        vmin, vmax = float(volt[mask].min()), float(volt[mask].max())
    for i in speed[mask].index:
        if volt is not None:
            data.append([float(speed[i]), float(power[i]), float(volt[i])])
        else:
            data.append([float(speed[i]), float(power[i]), None])

    visual_map = []
    if volt is not None and len(data) > 0:
        visual_map = [
            {
                "type": "continuous",
                "dimension": 2,
                "min": vmin,
                "max": vmax,
                "text": ["Voltage", ""],
                "inRange": {
                    # approximate viridis palette
                    "color": ["#440154", "#375a8c", "#2eb37c", "#b8de29", "#fde725"]
                },
                "right": 10,
                "top": "middle",
            }
        ]

    option = {
        "title": {"text": "⚡ Efficiency Analysis: Speed vs Power Consumption"},
        "tooltip": {"trigger": "item"},
        "xAxis": {"type": "value", "name": "Speed (m/s)"},
        "yAxis": {"type": "value", "name": "Power (W)"},
        "grid": {"left": "8%", "right": "8%", "top": 60, "bottom": 40},
        "visualMap": visual_map,
        "series": [
            {
                "type": "scatter",
                "symbolSize": 6,
                "itemStyle": {"opacity": 0.8},
                "data": data if data else [],
            }
        ],
    }
    return option


def create_gps_map_with_altitude_option(df: pd.DataFrame):
    """
    ECharts-based: Left panel = Lat/Lon XY track (line+points), Right panel = Altitude vs time.
    Note: Uses XY plane for GPS rather than a tile map to avoid external extensions.
    """
    if df is None or df.empty:
        return {
            "title": {"text": "🛰️ GPS Tracking and Altitude Analysis"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No GPS data available", "fill": "#999"},
                }
            ],
        }

    # Normalize columns
    lat = None
    lon = None
    for c in df.columns:
        lc = c.lower()
        if lat is None and ("latitude" in lc or lc == "lat" or "gps_lat" in lc):
            lat = c
        if lon is None and ("longitude" in lc or lc == "lon" or lc == "lng" or "gps_lon" in lc):
            lon = c
    if lat is None or lon is None:
        return {
            "title": {"text": "🛰️ GPS Tracking and Altitude Analysis"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No GPS coordinate columns found", "fill": "#999"},
                }
            ],
        }

    dfw = df.copy()
    dfw["latitude"] = pd.to_numeric(dfw[lat], errors="coerce")
    dfw["longitude"] = pd.to_numeric(dfw[lon], errors="coerce")
    if "altitude" in dfw.columns:
        dfw["altitude"] = pd.to_numeric(dfw["altitude"], errors="coerce")
    else:
        dfw["altitude"] = np.nan
    if "timestamp" in dfw.columns:
        dfw["timestamp"] = pd.to_datetime(dfw["timestamp"], errors="coerce", utc=True)

    # Filter invalid coordinates
    valid = (
        (~dfw["latitude"].isna())
        & (~dfw["longitude"].isna())
        & (dfw["latitude"].abs() <= 90)
        & (dfw["longitude"].abs() <= 180)
    )
    dfp = dfw.loc[valid].copy()
    if dfp.empty:
        return {
            "title": {"text": "🛰️ GPS Tracking and Altitude Analysis"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No valid GPS coordinates found", "fill": "#999"},
                }
            ],
        }
    if "timestamp" in dfp.columns and not dfp["timestamp"].isna().all():
        dfp = dfp.sort_values("timestamp")
        ts_ms = (dfp["timestamp"].astype(np.int64) // 10**6).tolist()
    else:
        ts_ms = list(range(len(dfp)))

    # Track colored by power if present
    if "power_w" in dfp.columns:
        pw = pd.to_numeric(dfp["power_w"], errors="coerce")
        data_track = (
            pd.DataFrame(
                {
                    "lon": dfp["longitude"].values,
                    "lat": dfp["latitude"].values,
                    "power": pw.values,
                }
            )
            .replace({np.nan: None})
            .values.tolist()
        )
        pmin, pmax = float(pw.dropna().min()) if pw.notna().any() else 0, float(
            pw.dropna().max()
        ) if pw.notna().any() else 1
        vmap = [
            {
                "type": "continuous",
                "dimension": 2,
                "min": pmin,
                "max": pmax,
                "text": ["Power (W)", ""],
                "inRange": {
                    "color": ["#440154", "#375a8c", "#2eb37c", "#b8de29", "#fde725"]
                },
                "left": "55%",
                "top": "middle",
            }
        ]
    else:
        data_track = (
            pd.DataFrame(
                {
                    "lon": dfp["longitude"].values,
                    "lat": dfp["latitude"].values,
                    "power": [None] * len(dfp),
                }
            )
            .replace({np.nan: None})
            .values.tolist()
        )
        vmap = []

    # Altitude data
    alt = pd.to_numeric(dfp["altitude"], errors="coerce")
    have_alt = (~alt.isna()).any()
    data_alt = (
        [[ts_ms[i], float(alt.iloc[i])] for i in range(len(dfp)) if not pd.isna(alt.iloc[i])]
        if have_alt
        else []
    )

    option = {
        "title": {"text": "🛰️ GPS Tracking and Altitude Analysis"},
        "tooltip": {"trigger": "item"},
        "visualMap": vmap,
        "grid": [
            {"left": "5%", "right": "55%", "top": 60, "height": "70%"},
            {"left": "55%", "right": "5%", "top": 60, "height": "70%"},
        ],
        "xAxis": [
            {"type": "value", "name": "Longitude", "gridIndex": 0},
            {"type": "time", "name": "Time", "gridIndex": 1},
        ],
        "yAxis": [
            {"type": "value", "name": "Latitude", "gridIndex": 0},
            {"type": "value", "name": "Altitude (m)", "gridIndex": 1},
        ],
        "series": [
            {
                "name": "Track",
                "type": "line",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "smooth": False,
                "showSymbol": False,
                "lineStyle": {"color": "#1f77b4", "width": 2, "opacity": 0.8},
                "data": [[row[0], row[1]] for row in data_track],
            },
            {
                "name": "Track points",
                "type": "scatter",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "symbolSize": 6,
                "itemStyle": {"opacity": 0.9},
                "data": data_track,
                "encode": {"x": 0, "y": 1},
            },
        ],
        "legend": {"top": 30},
    }

    if have_alt and data_alt:
        option["series"].append(
            {
                "name": "Altitude (m)",
                "type": "line",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "showSymbol": False,
                "lineStyle": {"color": "#2ca02c", "width": 2},
                "data": data_alt,
            }
        )
    else:
        option["series"].append(
            {
                "name": "Altitude",
                "type": "line",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "data": [],
            }
        )

    return option


def get_available_columns(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return []
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    exclude_cols = ["message_id", "uptime_seconds"]
    return [col for col in numeric_columns if col not in exclude_cols]


def create_dynamic_chart_option(df: pd.DataFrame, chart_config: Dict[str, Any]):
    if df.empty:
        return {
            "title": {"text": "No data available"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "No data available", "fill": "#999"},
                }
            ],
        }

    x_col = chart_config.get("x_axis")
    y_col = chart_config.get("y_axis")
    chart_type = chart_config.get("chart_type", "line")
    title = chart_config.get("title", f"{y_col} vs {x_col}")

    if chart_type == "heatmap":
        numeric_cols = get_available_columns(df)
        if len(numeric_cols) < 2:
            return {
                "title": {"text": "Need at least 2 numeric columns for heatmap"},
                "graphic": [
                    {
                        "type": "text",
                        "left": "center",
                        "top": "middle",
                        "style": {"text": "Need at least 2 numeric columns", "fill": "#999"},
                    }
                ],
            }
        corr = df[numeric_cols].corr().round(2)
        xs = numeric_cols
        ys = numeric_cols[::-1]  # flip so origin top-left
        data = []
        for i, yname in enumerate(ys):
            for j, xname in enumerate(xs):
                val = float(corr.loc[yname, xname])
                data.append([j, i, val])
        option = {
            "title": {"text": "🔥 Correlation Heatmap"},
            "tooltip": {"position": "top"},
            "xAxis": {"type": "category", "data": xs, "splitArea": {"show": True}},
            "yAxis": {"type": "category", "data": ys, "splitArea": {"show": True}},
            "visualMap": {
                "min": -1,
                "max": 1,
                "calculable": True,
                "orient": "vertical",
                "left": "right",
                "top": "middle",
                "inRange": {"color": ["#b2182b", "#f7f7f7", "#2166ac"]},
            },
            "series": [
                {
                    "name": "Correlation",
                    "type": "heatmap",
                    "data": data,
                    "label": {"show": True, "formatter": "{c}"},
                    "emphasis": {"itemStyle": {"shadowBlur": 10}},
                }
            ],
            "grid": {"left": "8%", "right": "10%", "top": 60, "bottom": 40},
        }
        return option

    # Non-heatmap
    if not y_col or y_col not in df.columns:
        return {
            "title": {"text": "Invalid Y-axis column selection"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "Invalid Y-axis", "fill": "#999"},
                }
            ],
        }
    if x_col not in df.columns:
        return {
            "title": {"text": "Invalid X-axis column selection"},
            "graphic": [
                {
                    "type": "text",
                    "left": "center",
                    "top": "middle",
                    "style": {"text": "Invalid X-axis", "fill": "#999"},
                }
            ],
        }

    # Prepare data
    if x_col == "timestamp":
        series_data = _to_echarts_time_series(df, x_col, y_col)
        xaxis = {"type": "time", "name": "Time"}
    else:
        x = pd.to_numeric(df[x_col], errors="coerce")
        y = pd.to_numeric(df[y_col], errors="coerce")
        mask = (~x.isna()) & (~y.isna())
        series_data = [[float(x.iloc[i]), float(y.iloc[i])] for i in x[mask].index]
        xaxis = {"type": "value", "name": x_col}

    yaxis = {"type": "value", "name": y_col}
    option = {
        "title": {"text": title},
        "tooltip": {"trigger": "axis" if chart_type != "scatter" else "item"},
        "xAxis": xaxis,
        "yAxis": yaxis,
        "grid": {"left": "8%", "right": "4%", "top": 60, "bottom": 40},
    }

    if chart_type == "line":
        option["series"] = [
            {
                "type": "line",
                "showSymbol": False,
                "data": series_data,
                "lineStyle": {"color": "#1f77b4", "width": 2},
            }
        ]
    elif chart_type == "scatter":
        option["series"] = [
            {
                "type": "scatter",
                "symbolSize": 6,
                "data": series_data,
                "itemStyle": {"color": "#ff7f0e", "opacity": 0.9},
            }
        ]
    elif chart_type == "bar":
        recent = df.tail(20)
        if recent.empty:
            return {
                "title": {"text": title},
                "graphic": [
                    {
                        "type": "text",
                        "left": "center",
                        "top": "middle",
                        "style": {"text": "Not enough recent data", "fill": "#999"},
                    }
                ],
            }
        if x_col == "timestamp":
            x = pd.to_datetime(recent[x_col], errors="coerce", utc=True)
            y = pd.to_numeric(recent[y_col], errors="coerce")
            mask = (~x.isna()) & (~y.isna())
            xs = (x[mask].astype(np.int64) // 10**6).tolist()
            ys = y[mask].tolist()
            cats = [xs[i] for i in range(len(xs))]
            option["xAxis"] = {"type": "time", "name": "Time"}
            option["series"] = [{"type": "bar", "data": [[cats[i], ys[i]] for i in range(len(ys))]}]
        else:
            x = pd.to_numeric(recent[x_col], errors="coerce")
            y = pd.to_numeric(recent[y_col], errors="coerce")
            mask = (~x.isna()) & (~y.isna())
            option["xAxis"] = {"type": "category", "data": [str(x.iloc[i]) for i in x[mask].index]}
            option["series"] = [{"type": "bar", "data": [float(y.iloc[i]) for i in y[mask].index]}]
    elif chart_type == "histogram":
        arr = pd.to_numeric(df[y_col], errors="coerce").dropna().values
        if arr.size == 0:
            return {
                "title": {"text": f"Distribution of {y_col}"},
                "graphic": [
                    {
                        "type": "text",
                        "left": "center",
                        "top": "middle",
                        "style": {"text": "No data for histogram", "fill": "#999"},
                    }
                ],
            }
        counts, bin_edges = np.histogram(arr, bins=30)
        centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
        option["xAxis"] = {"type": "value", "name": y_col}
        option["series"] = [
            {
                "type": "bar",
                "data": [[float(centers[i]), int(counts[i])] for i in range(len(counts))],
                "itemStyle": {"color": "#d62728"},
            }
        ]
        option["yAxis"] = {"type": "value", "name": "Count"}
    else:
        option["series"] = [
            {"type": "line", "showSymbol": False, "data": series_data}
        ]
    return option


# ------------------------------
# UI: Gauges and Sections
# ------------------------------

def render_live_gauges(kpis: Dict[str, float], unique_ns: str = "gauges"):
    st.markdown("##### 📊 Live Performance Gauges")
    st.markdown('<div class="widget-grid">', unsafe_allow_html=True)
    cols = st.columns(6)

    # Speed
    with cols[0]:
        st.markdown('<div class="gauge-container">', unsafe_allow_html=True)
        st.markdown('<div class="gauge-title">🚀 Speed (km/h)</div>', unsafe_allow_html=True)
        option = create_echarts_gauge_option(
            value=kpis["current_speed_kmh"],
            max_val=max(100, kpis["max_speed_kmh"] + 5),
            title="Speed",
            color="#1f77b4",
            suffix=" km/h",
            avg_ref=kpis["avg_speed_kmh"] if kpis["avg_speed_kmh"] > 0 else None,
        )
        st_echarts(option, height="140px", key=f"{unique_ns}_gauge_speed")
        st.markdown("</div>", unsafe_allow_html=True)

    # Battery %
    with cols[1]:
        st.markdown('<div class="gauge-container">', unsafe_allow_html=True)
        st.markdown('<div class="gauge-title">🔋 Battery (%)</div>', unsafe_allow_html=True)
        option = create_echarts_gauge_option(
            value=kpis["battery_percentage"], max_val=100, title="Battery", color="#2ca02c", suffix="%"
        )
        st_echarts(option, height="140px", key=f"{unique_ns}_gauge_battery")
        st.markdown("</div>", unsafe_allow_html=True)

    # Avg Power
    with cols[2]:
        st.markdown('<div class="gauge-container">', unsafe_allow_html=True)
        st.markdown('<div class="gauge-title">💡 Power (W)</div>', unsafe_allow_html=True)
        option = create_echarts_gauge_option(
            value=kpis["avg_power_w"],
            max_val=max(1000, kpis["avg_power_w"] * 2),
            title="Power",
            color="#ff7f0e",
            suffix=" W",
        )
        st_echarts(option, height="140px", key=f"{unique_ns}_gauge_power")
        st.markdown("</div>", unsafe_allow_html=True)

    # Efficiency
    with cols[3]:
        st.markdown('<div class="gauge-container">', unsafe_allow_html=True)
        st.markdown(
            '<div class="gauge-title">♻️ Efficiency (km/kWh)</div>',
            unsafe_allow_html=True,
        )
        eff_val = kpis["efficiency_km_per_kwh"]
        option = create_echarts_gauge_option(
            value=eff_val,
            max_val=max(100, eff_val * 1.5) if eff_val > 0 else 100,
            title="Efficiency",
            color="#6a51a3",
            suffix="",
        )
        st_echarts(option, height="140px", key=f"{unique_ns}_gauge_efficiency")
        st.markdown("</div>", unsafe_allow_html=True)

    # Roll
    with cols[4]:
        st.markdown('<div class="gauge-container">', unsafe_allow_html=True)
        st.markdown('<div class="gauge-title">🔄 Roll (°)</div>', unsafe_allow_html=True)
        roll_max = (
            max(45, abs(kpis["current_roll_deg"]) + 10) if kpis["current_roll_deg"] != 0 else 45
        )
        option = create_echarts_gauge_option(
            value=kpis["current_roll_deg"], max_val=roll_max, title="Roll", color="#e377c2", suffix="°"
        )
        st_echarts(option, height="140px", key=f"{unique_ns}_gauge_roll")
        st.markdown("</div>", unsafe_allow_html=True)

    # Pitch
    with cols[5]:
        st.markdown('<div class="gauge-container">', unsafe_allow_html=True)
        st.markdown('<div class="gauge-title">📐 Pitch (°)</div>', unsafe_allow_html=True)
        pitch_max = (
            max(45, abs(kpis["current_pitch_deg"]) + 10)
            if kpis["current_pitch_deg"] != 0
            else 45
        )
        option = create_echarts_gauge_option(
            value=kpis["current_pitch_deg"], max_val=pitch_max, title="Pitch", color="#17becf", suffix="°"
        )
        st_echarts(option, height="140px", key=f"{unique_ns}_gauge_pitch")
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
            f"⚠️ **Sensor Anomaly:** The following sensor(s) may be unreliable, showing static or zero values: **{sensor_list}**."
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
            <div class="chart-type-card">
                <div class="chart-type-name">📈 Line Chart</div>
                <div class="chart-type-desc">Great for time series data and trends</div>
            </div>
            <div class="chart-type-card">
                <div class="chart-type-name">🔵 Scatter Plot</div>
                <div class="chart-type-desc">Perfect for correlation analysis between variables</div>
            </div>
            <div class="chart-type-card">
                <div class="chart-type-name">📊 Bar Chart</div>
                <div class="chart-type-desc">Good for comparing recent values and discrete data</div>
            </div>
            <div class="chart-type-card">
                <div class="chart-type-name">📉 Histogram</div>
                <div class="chart-type-desc">Shows data distribution and frequency patterns</div>
            </div>
            <div class="chart-type-card">
                <div class="chart-type-name">🔥 Heatmap</div>
                <div class="chart-type-desc">Visualizes correlations between all numeric variables</div>
            </div>
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
        if st.button(
            "➕ Add Chart",
            key="add_chart_btn",
            help="Create a new custom chart",
        ):
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
                        new_title = st.text_input(
                            "Title",
                            value=chart_config.get("title", "New Chart"),
                            key=f"title_{chart_config['id']}",
                        )
                        if new_title != chart_config.get("title"):
                            st.session_state.dynamic_charts[i]["title"] = new_title

                    with col2:
                        new_type = st.selectbox(
                            "Type",
                            options=["line", "scatter", "bar", "histogram", "heatmap"],
                            index=[
                                "line",
                                "scatter",
                                "bar",
                                "histogram",
                                "heatmap",
                            ].index(chart_config.get("chart_type", "line")),
                            key=f"type_{chart_config['id']}",
                        )
                        if new_type != chart_config.get("chart_type"):
                            st.session_state.dynamic_charts[i]["chart_type"] = new_type

                    with col3:
                        if chart_config.get("chart_type", "line") not in ["histogram", "heatmap"]:
                            x_options = (
                                ["timestamp"] + available_columns
                                if "timestamp" in df.columns
                                else available_columns
                            )
                            current_x = chart_config.get("x_axis")
                            if current_x not in x_options and x_options:
                                current_x = x_options[0]
                            if x_options:
                                new_x = st.selectbox(
                                    "X-Axis",
                                    options=x_options,
                                    index=x_options.index(current_x) if current_x in x_options else 0,
                                    key=f"x_{chart_config['id']}",
                                )
                                if new_x != chart_config.get("x_axis"):
                                    st.session_state.dynamic_charts[i]["x_axis"] = new_x

                    with col4:
                        if chart_config.get("chart_type", "line") != "heatmap":
                            if available_columns:
                                current_y = chart_config.get("y_axis")
                                if current_y not in available_columns:
                                    current_y = available_columns[0]
                                new_y = st.selectbox(
                                    "Y-Axis",
                                    options=available_columns,
                                    index=available_columns.index(current_y) if current_y in available_columns else 0,
                                    key=f"y_{chart_config['id']}",
                                )
                                if new_y != chart_config.get("y_axis"):
                                    st.session_state.dynamic_charts[i]["y_axis"] = new_y

                    with col5:
                        if st.button(
                            "🗑️",
                            key=f"delete_{chart_config['id']}",
                            help="Delete chart",
                        ):
                            try:
                                idx_to_delete = next(
                                    (
                                        j
                                        for j, cfg in enumerate(st.session_state.dynamic_charts)
                                        if cfg["id"] == chart_config["id"]
                                    ),
                                    -1,
                                )
                                if idx_to_delete != -1:
                                    st.session_state.dynamic_charts.pop(idx_to_delete)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error deleting chart: {e}")

                    try:
                        option = create_dynamic_chart_option(df, chart_config)
                        st_echarts(option, height="400px", key=f"chart_plot_{chart_config['id']}")
                    except Exception as e:
                        st.error(f"Error rendering chart: {e}")

            except Exception as e:
                st.error(f"Error rendering chart configuration: {e}")


def main():
    st.markdown(
        '<div class="main-header">🏎️ Shell Eco-marathon Telemetry Dashboard (ECharts)</div>',
        unsafe_allow_html=True,
    )

    initialize_session_state()

    # Sidebar
    with st.sidebar:
        st.header("🔧 Connection & Data Source")
        data_source_mode = st.radio(
            "📊 Data Source",
            options=["realtime_session", "historical"],
            format_func=lambda x: "🔴 Real-time + Session Data"
            if x == "realtime_session"
            else "📚 Historical Data",
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
                        supabase_connected = (
                            st.session_state.telemetry_manager.connect_supabase()
                        )

                        realtime_connected = False
                        if ABLY_AVAILABLE:
                            realtime_connected = (
                                st.session_state.telemetry_manager.connect_realtime()
                            )

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
                    st.markdown(
                        '<div class="status-indicator">✅ Real-time Connected</div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        '<div class="status-indicator">❌ Real-time Disconnected</div>',
                        unsafe_allow_html=True,
                    )

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
                    st.error(f"⚠️ {stats['last_error'][:40]}...")

            st.divider()
            st.subheader("⚙️ Settings")
            new_auto_refresh = st.checkbox(
                "🔄 Auto Refresh",
                value=st.session_state.auto_refresh,
                help="Automatically refresh data from real-time stream",
                key=f"auto_refresh_{id(st.session_state)}",
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
                    st.session_state.historical_sessions = (
                        st.session_state.telemetry_manager.get_historical_sessions()
                    )
                st.rerun()

            if st.session_state.historical_sessions:
                session_options = []
                for session in st.session_state.historical_sessions:
                    name = session.get("session_name") or "Unnamed"
                    session_options.append(
                        f"{name} - {session['session_id'][:8]}... - "
                        f"{session['start_time'].strftime('%Y-%m-%d %H:%M')} "
                        f"({session['record_count']:,} records)"
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
                        or st.session_state.selected_session["session_id"]
                        != selected_session["session_id"]
                        or st.session_state.telemetry_data.empty
                    ):
                        st.session_state.selected_session = selected_session
                        st.session_state.is_viewing_historical = True
                        if selected_session["record_count"] > 10000:
                            st.info(
                                f"📊 Loading {selected_session['record_count']:,} records... This may take a moment due to pagination."
                            )

                        with st.spinner(
                            f"Loading data for session {selected_session['session_id'][:8]}..."
                        ):
                            historical_df = (
                                st.session_state.telemetry_manager.get_historical_data(
                                    selected_session["session_id"]
                                )
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

    # Main content
    df = st.session_state.telemetry_data.copy()
    new_messages_count = 0

    # Data ingestion
    if st.session_state.data_source_mode == "realtime_session":
        if st.session_state.telemetry_manager and st.session_state.telemetry_manager.is_connected:
            new_messages = st.session_state.telemetry_manager.get_realtime_messages()

            current_session_data_from_supabase = pd.DataFrame()
            if new_messages and "session_id" in new_messages[0]:
                current_session_id = new_messages[0]["session_id"]
                if (
                    st.session_state.current_session_id != current_session_id
                    or st.session_state.telemetry_data.empty
                ):
                    st.session_state.current_session_id = current_session_id
                    with st.spinner(f"Loading current session data for {current_session_id[:8]}..."):
                        current_session_data_from_supabase = (
                            st.session_state.telemetry_manager.get_current_session_data(
                                current_session_id
                            )
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
                    "**Getting Started (Real-time):**\n"
                    "1. Ensure the bridge (your data sending script) is running\n"
                    "2. Click 'Connect' in the sidebar to start receiving data\n"
                    "3. Large sessions are automatically paginated for optimal performance"
                )
            else:
                st.info(
                    "**Getting Started (Historical):**\n"
                    "1. Click 'Refresh Sessions' in the sidebar to load available sessions\n"
                    "2. Select a session and its data will load automatically\n"
                    "3. Large datasets use pagination to load all data points"
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

    # Data quality
    analyze_data_quality(df, is_realtime=(st.session_state.data_source_mode == "realtime_session"))
    if st.session_state.data_quality_notifications:
        for msg in st.session_state.data_quality_notifications:
            if "🚨" in msg:
                st.error(msg, icon="🚨")
            else:
                st.warning(msg, icon="⚠️")

    # Status row
    with st.container():
        col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
        with col1:
            st.info("📚 Historical" if st.session_state.is_viewing_historical else "🔴 Real-time")
        with col2:
            st.info(f"📊 {len(df):,} data points available")
        with col3:
            st.info(f"⏰ Last update: {st.session_state.last_update.strftime('%H:%M:%S')}")
        with col4:
            if st.session_state.data_source_mode == "realtime_session" and new_messages_count > 0:
                st.success(f"📨 +{new_messages_count}")

    if len(df) > 10000:
        st.markdown(
            f"""
        <div class="pagination-info">
            <strong>📊 Large Dataset Loaded:</strong> {len(df):,} data points successfully retrieved using pagination
        </div>
        """,
            unsafe_allow_html=True,
        )

    # KPIs
    kpis = calculate_kpis(df)

    st.subheader("📈 Dashboard")
    tab_names = [
        "📊 Overview",
        "🚗 Speed",
        "⚡ Power",
        "🎮 IMU",
        "🎮 IMU Detail",
        "⚡ Efficiency",
        "🛰️ GPS",
        "📈 Custom",
        "📃 Data",
    ]
    tabs = st.tabs(tab_names)

    # Overview
    with tabs[0]:
        render_overview_tab(kpis)

    # Speed
    with tabs[1]:
        render_live_gauges(kpis, unique_ns="speedtab")
        render_kpi_header(kpis, unique_ns="speedtab", show_gauges=False)
        option = create_speed_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(option, height="420px", key="chart_speed_main")
        st.markdown("</div>", unsafe_allow_html=True)

    # Power
    with tabs[2]:
        render_live_gauges(kpis, unique_ns="powertab")
        render_kpi_header(kpis, unique_ns="powertab", show_gauges=False)
        option = create_power_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(option, height="520px", key="chart_power_main")
        st.markdown("</div>", unsafe_allow_html=True)

    # IMU
    with tabs[3]:
        render_live_gauges(kpis, unique_ns="imutab")
        render_kpi_header(kpis, unique_ns="imutab", show_gauges=False)
        option = create_imu_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(option, height="700px", key="chart_imu_main")
        st.markdown("</div>", unsafe_allow_html=True)

    # IMU detail
    with tabs[4]:
        render_live_gauges(kpis, unique_ns="imudetailtab")
        render_kpi_header(kpis, unique_ns="imudetailtab", show_gauges=False)
        option = create_imu_detail_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(option, height="900px", key="chart_imu_detail_main")
        st.markdown("</div>", unsafe_allow_html=True)

    # Efficiency
    with tabs[5]:
        render_live_gauges(kpis, unique_ns="efftab")
        render_kpi_header(kpis, unique_ns="efftab", show_gauges=False)
        option = create_efficiency_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(option, height="420px", key="chart_efficiency_main")
        st.markdown("</div>", unsafe_allow_html=True)

    # GPS + Altitude
    with tabs[6]:
        render_live_gauges(kpis, unique_ns="gpstab")
        render_kpi_header(kpis, unique_ns="gpstab", show_gauges=False)
        option = create_gps_map_with_altitude_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(option, height="540px", key="chart_gps_main")
        st.markdown("</div>", unsafe_allow_html=True)

    # Custom
    with tabs[7]:
        render_live_gauges(kpis, unique_ns="customtab")
        render_kpi_header(kpis, unique_ns="customtab", show_gauges=False)
        render_dynamic_charts_section(df)

    # Data
    with tabs[8]:
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
                sample_csv = sample_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Sample CSV (1000 rows)",
                    data=sample_csv,
                    file_name=f"telemetry_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

        with st.expander("📊 Dataset Statistics"):
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", f"{len(df):,}")
                st.metric("Columns", len(df.columns))
                if "roll_deg" in calculate_roll_and_pitch(df).columns:
                    st.metric("Roll & Pitch", "✅ Calculated")
            with col2:
                if "timestamp" in df.columns and len(df) > 1:
                    try:
                        timestamp_series = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
                        timestamp_series = timestamp_series.dropna()
                        if len(timestamp_series) > 1:
                            time_span = timestamp_series.max() - timestamp_series.min()
                            st.metric("Time Span", str(time_span).split(".")[0])
                            if time_span.total_seconds() > 0:
                                data_rate = len(df) / time_span.total_seconds()
                                st.metric("Data Rate", f"{data_rate:.2f} Hz")
                        else:
                            st.metric("Time Span", "N/A")
                            st.metric("Data Rate", "N/A")
                    except Exception:
                        st.metric("Time Span", "Error")
                        st.metric("Data Rate", "Error")
            with col3:
                memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
                st.metric("Memory Usage", f"{memory_usage:.2f} MB")
                if "data_source" in df.columns:
                    source_counts = df["data_source"].value_counts()
                    st.write("**Data Sources:**")
                    for source, count in source_counts.items():
                        st.write(f"• {source}: {count:,} rows")

    if st.session_state.data_source_mode == "realtime_session" and st.session_state.auto_refresh:
        if AUTOREFRESH_AVAILABLE:
            st_autorefresh(interval=st.session_state.refresh_interval * 1000, key="auto_refresh")
        else:
            st.warning(
                "🔄 To enable smooth auto-refresh install:\n"
                "`pip install streamlit-autorefresh`"
            )

    st.divider()
    st.markdown(
        "<div style='text-align: center; opacity: 0.8; padding: 1rem;'>"
        "<p>Shell Eco-marathon Telemetry Dashboard (ECharts)</p>"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
