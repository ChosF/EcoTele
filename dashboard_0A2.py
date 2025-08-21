# fmt: off
# Prettier print width ~80 via manual wrapping; Streamlit app code.

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import matplotlib.colors as mcolors  # used for color parsing only
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

# Charts: Apache ECharts (HTML) via Streamlit component
try:
    from streamlit_raw_echarts import st_echarts, JsCode
    ECHARTS_AVAILABLE = True
except ImportError:
    ECHARTS_AVAILABLE = False
    st.error(
        "❌ ECharts component not available. Please install: "
        "pip install streamlit-raw-echarts"
    )
    st.stop()

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
SUPABASE_API_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRz"
    "Zm1kemllaGhnbXJjb25qY25zIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTE5MDEyOTIsImV4c"
    "CI6MjA2NzQ3NzI5Mn0.P41bpLkP0tKpTktLx6hFOnnyrAB9N_yihQP1v6zTRwc"
)
SUPABASE_TABLE_NAME = "telemetry"

# Pagination constants
SUPABASE_MAX_ROWS_PER_REQUEST = 1000
MAX_DATAPOINTS_PER_SESSION = 1_000_000

# Streamlit page config
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
# Theme-aware CSS (unchanged styles, usable with ECharts)
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
.card {
  border-radius:18px; padding:1.1rem; border:1px solid var(--glass-border);
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

.historical-notice,.pagination-info {
  border-radius:14px; padding:.9rem 1rem; font-weight:700;
  border:1px solid var(--border); background: var(--glass);
}
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
.stTabs [data-baseweb="tab-list"] { border-bottom: 1px solid var(--border); gap:6px; }
.stTabs [data-baseweb="tab"] { border:none; border-radius:10px 10px 0 0; background: transparent; color: var(--text-muted);
  font-weight:600; padding:.6rem 1rem; transition: color .2s ease, background .2s ease; }
.stTabs [data-baseweb="tab"]:hover { color: var(--text); background: var(--glass); }
.stTabs [data-baseweb="tab"][aria-selected="true"] {
  color: color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text));
  box-shadow: inset 0 -3px 0 0 color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text));
}
.chart-type-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap:.75rem; }
.chart-type-card { border-radius:16px; padding:1rem; border:1px solid var(--glass-border); box-shadow: var(--shadow-1);
  background:
    radial-gradient(130% 120% at 20% 15%, color-mix(in oklab, hsl(var(--brand-2)) 4%, transparent), transparent 60%),
    var(--glass);
}
.chart-type-name {
  font-weight:800;
  background: linear-gradient(90deg,
                 color-mix(in oklab, hsl(var(--brand-1)) 60%, var(--text)),
                 color-mix(in oklab, hsl(var(--brand-2)) 60%, var(--text)));
  -webkit-background-clip:text; background-clip:text; color: transparent;
}
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


# ======================================================
# Data manager (Supabase + Ably) - unchanged mechanics
# ======================================================
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
            self.realtime_subscriber.connection.on(
                "disconnected", on_disconnected
            )
            self.realtime_subscriber.connection.on("failed", on_failed)

            await self.realtime_subscriber.connection.once_async("connected")
            channel = self.realtime_subscriber.channels.get(
                DASHBOARD_CHANNEL_NAME
            )
            await channel.subscribe(
                "telemetry_update", self._on_message_received
            )

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

    def _paginated_fetch(
        self, session_id: str, data_source: str = "supabase_current"
    ) -> pd.DataFrame:
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
                        f"📄 Fetching page {request_count + 1}: rows "
                        f"{offset}-{range_end}"
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
                        self.logger.info(
                            f"✅ No more data found at offset {offset}"
                        )
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
                self.stats["pagination_stats"][
                    "total_rows_fetched"
                ] += total_fetched
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
                    f"✅ Successfully fetched {len(df)} total rows for session "
                    f"{session_id[:8]}..."
                )
                return df
            else:
                self.logger.warning(f"⚠️ No data found for session {session_id}")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(
                f"❌ Error in paginated fetch for session {session_id}: {e}"
            )
            with self._lock:
                self.stats["errors"] += 1
                self.stats["last_error"] = str(e)
            return pd.DataFrame()

    def get_current_session_data(self, session_id: str) -> pd.DataFrame:
        self.logger.info(
            f"🔄 Fetching current session data for {session_id[:8]}..."
        )
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
                        f"❌ Error fetching session records at offset "
                        f"{offset}: {e}"
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
                    if session_name and not sessions[session_id].get(
                        "session_name"
                    ):
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
                        f"❌ Error processing session "
                        f"{session_info['session_id']}: {e}"
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
        self.logger.info(
            f"🔄 Fetching historical data for session {session_id[:8]}..."
        )
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


# =========================================
# Telemetry utils & calculations (unchanged)
# =========================================
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
        denominator_roll = np.sqrt(
            df_calc["accel_x"] ** 2 + df_calc["accel_z"] ** 2
        )
        denominator_roll = np.where(denominator_roll == 0, 1e-10, denominator_roll)
        df_calc["roll_rad"] = np.arctan2(df_calc["accel_y"], denominator_roll)
        df_calc["roll_deg"] = np.degrees(df_calc["roll_rad"])

        denominator_pitch = np.sqrt(
            df_calc["accel_y"] ** 2 + df_calc["accel_z"] ** 2
        )
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
            kpis["total_distance_km"] = max(
                0, df["distance_m"].dropna().iloc[-1] / 1000
            )
        if "energy_j" in df.columns and not df["energy_j"].dropna().empty:
            kpis["total_energy_kwh"] = max(
                0, df["energy_j"].dropna().iloc[-1] / 3_600_000
            )
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
                            (
                                (current_voltage - min_voltage)
                                / (max_voltage - min_voltage)
                            )
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


# ===========================
# ECharts helper constructions
# ===========================
def _empty_chart_option(msg: str, height: int = 320) -> Dict[str, Any]:
    # A centered text using graphic to show no data messages
    return {
        "animation": False,
        "graphic": [
            {
                "type": "text",
                "left": "center",
                "top": "middle",
                "style": {
                    "text": msg,
                    "fontSize": 14,
                    "fill": "#888",
                },
            }
        ],
        "grid": {"left": "5%", "right": "5%", "top": 40, "bottom": 40},
    }


def _to_time_pairs(df: pd.DataFrame, x_col: str, y_col: str) -> List[List]:
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return []
    x = pd.to_datetime(df[x_col], errors="coerce", utc=True)
    y = pd.to_numeric(df[y_col], errors="coerce")
    mask = (~x.isna()) & (~y.isna())
    x = x[mask]
    y = y[mask]
    if x.empty or y.empty:
        return []
    # ECharts supports ms since epoch within JS safe integer range
    xs = (x.values.astype("datetime64[ms]").astype(np.int64)).tolist()
    ys = y.values.tolist()
    return [[xs[i], ys[i]] for i in range(len(xs))]


def _theme_text_color() -> str:
    # Default to a neutral color that works on both themes reasonably well
    return "#bbbbbb"


def _axis_style():
    return {
        "axisLine": {"lineStyle": {"color": "#666"}},
        "axisLabel": {"color": _theme_text_color()},
        "splitLine": {"lineStyle": {"color": "#444", "opacity": 0.25}},
    }


def echarts_small_gauge_option(
    value: float,
    max_val: Optional[float],
    title: str,
    color_hex: str,
    suffix: str = "",
    avg_ref: Optional[float] = None,
    thresh_val: Optional[float] = None,
) -> Dict[str, Any]:
    # Determine max
    if max_val is None or max_val <= 0:
        max_val = value * 1.2 if value > 0 else 1.0
    # Steps (two bands)
    try:
        rgb = mcolors.to_rgb(color_hex)
        band1 = f"rgba({int(rgb[0]*255)},{int(rgb[1]*255)},{int(rgb[2]*255)},0.25)"
        band2 = f"rgba({int(rgb[0]*255)},{int(rgb[1]*255)},{int(rgb[2]*255)},0.45)"
    except Exception:
        band1, band2 = "rgba(31,119,180,0.25)", "rgba(31,119,180,0.45)"

    axis_line_colors = [
        [0.6, band1],
        [1.0, band2],
    ]
    pointer_color = color_hex
    detail_fmt = "{value}" + suffix

    # Delta-like visualization on title/detail is non-native; keep simple
    option = {
        "animation": True,
        "series": [
            {
                "type": "gauge",
                "min": 0,
                "max": max_val,
                "startAngle": 225,
                "endAngle": -45,
                "splitNumber": 6,
                "axisLine": {
                    "lineStyle": {"width": 10, "color": axis_line_colors}
                },
                "progress": {"show": True, "roundCap": True, "width": 10},
                "pointer": {
                    "length": "60%",
                    "width": 4,
                    "itemStyle": {"color": pointer_color},
                },
                "axisTick": {"show": False},
                "splitLine": {"length": 8, "lineStyle": {"width": 1}},
                "axisLabel": {"color": _theme_text_color(), "distance": 12},
                "title": {
                    "show": False
                },  # external label handled by streamlit HTML above
                "detail": {
                    "valueAnimation": True,
                    "formatter": detail_fmt,
                    "color": "#ddd",
                    "fontSize": 16,
                    "offsetCenter": [0, "20%"],
                },
                "data": [{"value": float(value)}],
            }
        ],
    }
    if thresh_val is not None:
        option["series"][0]["axisLine"]["roundCap"] = True
        # Use an axis pointer mark line via markPoint-like style not native to gauge;
        # simple approach: add a small second series with fixed pointer
        option["series"].append(
            {
                "type": "gauge",
                "min": 0,
                "max": max_val,
                "startAngle": 225,
                "endAngle": -45,
                "pointer": {
                    "length": "60%",
                    "width": 3,
                    "itemStyle": {"color": "#dc3545"},
                },
                "axisLine": {"lineStyle": {"width": 0}},
                "axisTick": {"show": False},
                "splitLine": {"show": False},
                "axisLabel": {"show": False},
                "detail": {"show": False},
                "data": [{"value": float(thresh_val)}],
                "silent": True,
                "z": 2,
            }
        )
    return option


def render_live_gauges(kpis: Dict[str, float], unique_ns: str = "gauges"):
    st.markdown("##### 📊 Live Performance Gauges")
    st.markdown('<div class="widget-grid">', unsafe_allow_html=True)
    cols = st.columns(6)

    with cols[0]:
        st.markdown(
            '<div class="gauge-container"><div class="gauge-title">'
            "🚀 Speed (km/h)</div>",
            unsafe_allow_html=True,
        )
        opt = echarts_small_gauge_option(
            kpis["current_speed_kmh"],
            max(100, kpis["max_speed_kmh"] + 5),
            "Speed",
            "#1f77b4",
            " km/h",
        )
        st_echarts(opt, height="140px", key=f"{unique_ns}_gauge_speed",
                   notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[1]:
        st.markdown(
            '<div class="gauge-container"><div class="gauge-title">'
            "🔋 Battery (%)</div>",
            unsafe_allow_html=True,
        )
        opt = echarts_small_gauge_option(
            kpis["battery_percentage"], 100, "Battery", "#2ca02c", "%"
        )
        st_echarts(opt, height="140px", key=f"{unique_ns}_gauge_battery",
                   notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[2]:
        st.markdown(
            '<div class="gauge-container"><div class="gauge-title">'
            "💡 Power (W)</div>",
            unsafe_allow_html=True,
        )
        opt = echarts_small_gauge_option(
            kpis["avg_power_w"],
            max(1000, kpis["avg_power_w"] * 2),
            "Power",
            "#ff7f0e",
            " W",
        )
        st_echarts(opt, height="140px", key=f"{unique_ns}_gauge_power",
                   notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[3]:
        st.markdown(
            '<div class="gauge-container"><div class="gauge-title">'
            "♻️ Efficiency (km/kWh)</div>",
            unsafe_allow_html=True,
        )
        eff = kpis["efficiency_km_per_kwh"]
        opt = echarts_small_gauge_option(
            eff, max(100, eff * 1.5) if eff > 0 else 100, "Efficiency", "#6a51a3", ""
        )
        st_echarts(opt, height="140px", key=f"{unique_ns}_gauge_efficiency",
                   notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[4]:
        st.markdown(
            '<div class="gauge-container"><div class="gauge-title">'
            "🔄 Roll (°)</div>",
            unsafe_allow_html=True,
        )
        roll_max = (
            max(45, abs(kpis["current_roll_deg"]) + 10)
            if kpis["current_roll_deg"] != 0
            else 45
        )
        opt = echarts_small_gauge_option(
            kpis["current_roll_deg"], roll_max, "Roll", "#e377c2", "°"
        )
        st_echarts(opt, height="140px", key=f"{unique_ns}_gauge_roll",
                   notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with cols[5]:
        st.markdown(
            '<div class="gauge-container"><div class="gauge-title">'
            "📐 Pitch (°)</div>",
            unsafe_allow_html=True,
        )
        pitch_max = (
            max(45, abs(kpis["current_pitch_deg"]) + 10)
            if kpis["current_pitch_deg"] != 0
            else 45
        )
        opt = echarts_small_gauge_option(
            kpis["current_pitch_deg"], pitch_max, "Pitch", "#17becf", "°"
        )
        st_echarts(opt, height="140px", key=f"{unique_ns}_gauge_pitch",
                   notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)


def render_kpi_header(
    kpis: Dict[str, float], unique_ns: str = "kpiheader", show_gauges: bool = True
):
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
    st.markdown(
        "Real-time key performance indicators for your Shell Eco-marathon vehicle"
    )
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
                    f"🚨 **Data Stream Stalled:** No new data received for "
                    f"{int(time_since_last)}s. (Expected ~{avg_rate:.1f}s)"
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
            f"{', '.join(failing_sensors[:3])}...) are static/zero."
        )
    elif failing_sensors:
        sensor_list = ", ".join(failing_sensors)
        notifications.append(
            f"⚠️ **Sensor Anomaly:** Static/zero values in: **{sensor_list}**."
        )
    st.session_state.data_quality_notifications = notifications


# ==============================
# ECharts chart builders (HTML)
# ==============================
def create_speed_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or "speed_ms" not in df.columns:
        return _empty_chart_option("No speed data available")
    pairs = _to_time_pairs(df, "timestamp", "speed_ms")
    if not pairs:
        return _empty_chart_option("No speed data available")
    return {
        "title": {"text": "🚗 Vehicle Speed Over Time", "left": "center"},
        "tooltip": {"trigger": "axis"},
        "grid": {"left": "5%", "right": "5%", "top": 60, "bottom": 40},
        "xAxis": {
            "type": "time",
            "axisLabel": {"color": _theme_text_color()},
            "splitLine": {"lineStyle": {"color": "#444", "opacity": 0.25}},
        },
        "yAxis": {
            "type": "value",
            "name": "Speed (m/s)",
            "nameTextStyle": {"color": _theme_text_color()},
            "axisLabel": {"color": _theme_text_color()},
            "splitLine": {"lineStyle": {"color": "#444", "opacity": 0.25}},
        },
        "dataZoom": [{"type": "inside"}, {"type": "slider"}],
        "series": [
            {
                "type": "line",
                "name": "Speed (m/s)",
                "showSymbol": False,
                "smooth": True,
                "lineStyle": {"width": 2, "color": "#1f77b4"},
                "data": pairs,
            }
        ],
    }


def create_power_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    needed = ["voltage_v", "current_a", "power_w"]
    if df.empty or any(col not in df.columns for col in needed):
        return _empty_chart_option("No power data available", height=480)

    v_pairs = _to_time_pairs(df, "timestamp", "voltage_v")
    c_pairs = _to_time_pairs(df, "timestamp", "current_a")
    p_pairs = _to_time_pairs(df, "timestamp", "power_w")
    if not v_pairs and not c_pairs and not p_pairs:
        return _empty_chart_option("No power data available", height=480)

    option = {
        "title": {"text": "⚡ Electrical System Performance", "left": "center"},
        "tooltip": {"trigger": "axis"},
        "grid": [
            {"left": "8%", "right": "5%", "top": 60, "height": "35%"},
            {"left": "8%", "right": "5%", "top": "58%", "height": "32%"},
        ],
        "xAxis": [
            {"type": "time", **_axis_style()},
            {"type": "time", "gridIndex": 1, **_axis_style()},
        ],
        "yAxis": [
            {
                "type": "value",
                "name": "V / A",
                "nameTextStyle": {"color": _theme_text_color()},
                **_axis_style(),
            },
            {
                "type": "value",
                "name": "Power (W)",
                "nameTextStyle": {"color": _theme_text_color()},
                "gridIndex": 1,
                **_axis_style(),
            },
        ],
        "dataZoom": [
            {"type": "inside", "xAxisIndex": [0, 1]},
            {"type": "slider", "xAxisIndex": [0, 1]},
        ],
        "series": [
            {
                "type": "line",
                "name": "Voltage (V)",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#2ca02c"},
                "data": v_pairs,
            },
            {
                "type": "line",
                "name": "Current (A)",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#d62728"},
                "data": c_pairs,
            },
            {
                "type": "line",
                "name": "Power (W)",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#ff7f0e"},
                "data": p_pairs,
            },
        ],
        "legend": {"top": 30},
    }
    return option


def create_imu_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    needed = [
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "accel_x",
        "accel_y",
        "accel_z",
    ]
    if df.empty or any(col not in df.columns for col in needed):
        return _empty_chart_option("No IMU data available", height=660)

    df = calculate_roll_and_pitch(df)
    gyro_cols = ["gyro_x", "gyro_y", "gyro_z"]
    accel_cols = ["accel_x", "accel_y", "accel_z"]
    gyro_colors = ["#e74c3c", "#2ecc71", "#3498db"]
    accel_colors = ["#f39c12", "#9b59b6", "#34495e"]

    series = []
    # 3 grids: Gyro, Accel, Roll & Pitch
    grids = [
        {"left": "8%", "right": "5%", "top": 60, "height": "24%"},
        {"left": "8%", "right": "5%", "top": "38%", "height": "24%"},
        {"left": "8%", "right": "5%", "top": "70%", "height": "22%"},
    ]
    x_axes = [
        {"type": "time", **_axis_style()},
        {"type": "time", "gridIndex": 1, **_axis_style()},
        {"type": "time", "gridIndex": 2, **_axis_style()},
    ]
    y_axes = [
        {
            "type": "value",
            "name": "deg/s",
            "nameTextStyle": {"color": _theme_text_color()},
            **_axis_style(),
        },
        {
            "type": "value",
            "name": "m/s²",
            "nameTextStyle": {"color": _theme_text_color()},
            "gridIndex": 1,
            **_axis_style(),
        },
        {
            "type": "value",
            "name": "°",
            "nameTextStyle": {"color": _theme_text_color()},
            "gridIndex": 2,
            **_axis_style(),
        },
    ]

    for i, col in enumerate(gyro_cols):
        pairs = _to_time_pairs(df, "timestamp", col)
        series.append(
            {
                "type": "line",
                "name": f"Gyro {col[-1].upper()}",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": gyro_colors[i]},
                "data": pairs,
            }
        )

    for i, col in enumerate(accel_cols):
        pairs = _to_time_pairs(df, "timestamp", col)
        series.append(
            {
                "type": "line",
                "name": f"Accel {col[-1].upper()}",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": accel_colors[i]},
                "data": pairs,
            }
        )

    roll_pairs = _to_time_pairs(df, "timestamp", "roll_deg")
    pitch_pairs = _to_time_pairs(df, "timestamp", "pitch_deg")
    series.append(
        {
            "type": "line",
            "name": "Roll (°)",
            "xAxisIndex": 2,
            "yAxisIndex": 2,
            "showSymbol": False,
            "lineStyle": {"width": 3, "color": "#e377c2"},
            "data": roll_pairs,
        }
    )
    series.append(
        {
            "type": "line",
            "name": "Pitch (°)",
            "xAxisIndex": 2,
            "yAxisIndex": 2,
            "showSymbol": False,
            "lineStyle": {"width": 3, "color": "#17becf"},
            "data": pitch_pairs,
        }
    )
    return {
        "title": {"text": "⚡ IMU System Performance with Roll & Pitch", "left": "center"},
        "tooltip": {"trigger": "axis"},
        "grid": grids,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "dataZoom": [{"type": "inside", "xAxisIndex": [0, 1, 2]}],
        "legend": {"top": 30},
        "series": series,
    }


def create_imu_detail_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    needed = [
        "gyro_x",
        "gyro_y",
        "gyro_z",
        "accel_x",
        "accel_y",
        "accel_z",
    ]
    if df.empty or any(col not in df.columns for col in needed):
        return _empty_chart_option("No IMU data available", height=680)

    df = calculate_roll_and_pitch(df)

    # Build 3x3 grids (rows x cols)
    grids = []
    x_axes = []
    y_axes = []
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

    # Layout parameters
    top_offsets = [60, 60 + 200, 60 + 400]  # approximate row tops
    grid_height = 180
    left_offsets = [8, 36, 64]  # percentages (approx)
    grid_width = 28  # percentage width each

    def _grid_idx(r, c):
        return r * 3 + c

    for r in range(3):
        for c in range(3):
            top = top_offsets[r]
            left = f"{left_offsets[c]}%"
            grids.append(
                {
                    "left": left,
                    "right": "2%",
                    "top": top,
                    "height": grid_height,
                }
            )
            x_axes.append(
                {"type": "time", "gridIndex": _grid_idx(r, c), **_axis_style()}
            )
            y_axes.append(
                {"type": "value", "gridIndex": _grid_idx(r, c), **_axis_style()}
            )

    # Gyro
    gyro_cols = ["gyro_x", "gyro_y", "gyro_z"]
    gyro_colors = ["#e74c3c", "#2ecc71", "#3498db"]
    for i, col in enumerate(gyro_cols):
        pairs = _to_time_pairs(df, "timestamp", col)
        series.append(
            {
                "type": "line",
                "name": titles[i],
                "xAxisIndex": i,
                "yAxisIndex": i,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": gyro_colors[i]},
                "data": pairs,
            }
        )

    # Accel
    accel_cols = ["accel_x", "accel_y", "accel_z"]
    accel_colors = ["#f39c12", "#9b59b6", "#34495e"]
    for i, col in enumerate(accel_cols):
        pairs = _to_time_pairs(df, "timestamp", col)
        x_i = 3 + i
        y_i = 3 + i
        series.append(
            {
                "type": "line",
                "name": titles[3 + i],
                "xAxisIndex": x_i,
                "yAxisIndex": y_i,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": accel_colors[i]},
                "data": pairs,
            }
        )

    # Roll (bottom-left)
    roll_pairs = _to_time_pairs(df, "timestamp", "roll_deg")
    x_i = 6
    y_i = 6
    series.append(
        {
            "type": "line",
            "name": "Roll (°)",
            "xAxisIndex": x_i,
            "yAxisIndex": y_i,
            "showSymbol": False,
            "lineStyle": {"width": 3, "color": "#e377c2"},
            "data": roll_pairs,
        }
    )

    # Pitch (bottom-middle)
    pitch_pairs = _to_time_pairs(df, "timestamp", "pitch_deg")
    x_i = 7
    y_i = 7
    series.append(
        {
            "type": "line",
            "name": "Pitch (°)",
            "xAxisIndex": x_i,
            "yAxisIndex": y_i,
            "showSymbol": False,
            "lineStyle": {"width": 3, "color": "#17becf"},
            "data": pitch_pairs,
        }
    )

    # Combined (bottom-right)
    x_i = 8
    y_i = 8
    series.append(
        {
            "type": "line",
            "name": "Roll (°)",
            "xAxisIndex": x_i,
            "yAxisIndex": y_i,
            "showSymbol": False,
            "lineStyle": {"width": 2, "color": "#e377c2"},
            "data": roll_pairs,
        }
    )
    series.append(
        {
            "type": "line",
            "name": "Pitch (°)",
            "xAxisIndex": x_i,
            "yAxisIndex": y_i,
            "showSymbol": False,
            "lineStyle": {"width": 2, "color": "#17becf"},
            "data": pitch_pairs,
        }
    )

    return {
        "title": {
            "text": "🎮 Detailed IMU Sensor Analysis with Roll & Pitch",
            "left": "center",
        },
        "tooltip": {"trigger": "axis"},
        "grid": grids,
        "xAxis": x_axes,
        "yAxis": y_axes,
        "dataZoom": [{"type": "inside", "xAxisIndex": list(range(9))}],
        "legend": {"top": 30},
        "series": series,
    }


def create_efficiency_chart_option(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty or not all(col in df.columns for col in ["speed_ms", "power_w"]):
        return _empty_chart_option("No efficiency data available")
    # Prepare scatter with optional color by voltage
    x = pd.to_numeric(df.get("speed_ms", pd.Series(dtype=float)), errors="coerce")
    y = pd.to_numeric(df.get("power_w", pd.Series(dtype=float)), errors="coerce")
    v = pd.to_numeric(df.get("voltage_v", pd.Series(dtype=float)), errors="coerce")
    mask = (~x.isna()) & (~y.isna())
    data = []
    if "voltage_v" in df.columns and not v.dropna().empty:
        mask = mask & (~v.isna())
        for sx, sy, sv in zip(x[mask], y[mask], v[mask]):
            data.append([float(sx), float(sy), float(sv)])
        visual = {
            "min": float(v[mask].min()) if not v[mask].empty else 0,
            "max": float(v[mask].max()) if not v[mask].empty else 1,
        }
        visual_map = {
            "type": "continuous",
            "min": visual["min"],
            "max": visual["max"],
            "dimension": 2,
            "inRange": {
                "color": [
                    "#440154",
                    "#3b528b",
                    "#21908d",
                    "#5dc962",
                    "#fde725",
                ]  # viridis-ish
            },
            "text": ["High V", "Low V"],
            "calculable": True,
            "right": 10,
            "top": 100,
        }
    else:
        for sx, sy in zip(x[mask], y[mask]):
            data.append([float(sx), float(sy)])
        visual_map = None

    option = {
        "title": {
            "text": "⚡ Efficiency Analysis: Speed vs Power Consumption",
            "left": "center",
        },
        "tooltip": {"trigger": "item"},
        "grid": {"left": "5%", "right": "5%", "top": 60, "bottom": 40},
        "xAxis": {
            "type": "value",
            "name": "Speed (m/s)",
            "nameTextStyle": {"color": _theme_text_color()},
            **_axis_style(),
        },
        "yAxis": {
            "type": "value",
            "name": "Power (W)",
            "nameTextStyle": {"color": _theme_text_color()},
            **_axis_style(),
        },
        "series": [
            {
                "type": "scatter",
                "symbolSize": 6,
                "itemStyle": {"opacity": 0.8},
                "data": data,
            }
        ],
    }
    if visual_map:
        option["visualMap"] = visual_map
    return option


def create_gps_map_with_altitude_option(df: pd.DataFrame) -> Dict[str, Any]:
    # No external map tiles to keep reliability; draw lon/lat track (top) + altitude (bottom)
    if df is None or df.empty:
        return _empty_chart_option("No GPS data available", height=520)

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
    alt_col = _find_col(
        df, ["altitude", "elevation", "elev", "alt", "alt_m", "height", "gps_altitude"]
    )
    time_col = _find_col(
        df, ["timestamp", "time", "ts", "time_utc", "created_at", "datetime", "date"]
    )

    if not lat_col or not lon_col:
        return _empty_chart_option(
            "No GPS coordinate columns found (latitude/longitude).", height=520
        )

    dfw = df.copy()
    dfw["latitude"] = pd.to_numeric(dfw[lat_col], errors="coerce")
    dfw["longitude"] = pd.to_numeric(dfw[lon_col], errors="coerce")
    dfw["altitude"] = (
        pd.to_numeric(dfw[alt_col], errors="coerce") if alt_col else np.nan
    )
    if time_col:
        dfw["timestamp"] = pd.to_datetime(dfw[time_col], errors="coerce", utc=True)

    valid_mask = (
        (~dfw["latitude"].isna())
        & (~dfw["longitude"].isna())
        & (dfw["latitude"].abs() <= 90)
        & (dfw["longitude"].abs() <= 180)
        & ~((dfw["latitude"].abs() < 1e-6) & (dfw["longitude"].abs() < 1e-6))
    )
    dff = dfw.loc[valid_mask].copy()
    if dff.empty:
        return _empty_chart_option("No valid GPS coordinates found", height=520)

    if "timestamp" in dff.columns:
        dff = dff.sort_values("timestamp").reset_index(drop=True)

    # Top grid: lon vs lat (track)
    track_data = dff[["longitude", "latitude"]].dropna()
    track_pairs = track_data.values.tolist()

    # Bottom grid: altitude vs time/index
    if "timestamp" in dff.columns and not dff["timestamp"].isna().all():
        alt_pairs = _to_time_pairs(dff, "timestamp", "altitude")
        x_axis_bottom = {"type": "time", "gridIndex": 1, **_axis_style()}
    else:
        # Index-based fallback
        idx = np.arange(len(dff))
        alt = pd.to_numeric(dff["altitude"], errors="coerce")
        mask = ~np.isnan(alt)
        alt_pairs = [[int(idx[i]), float(alt.iloc[i])] for i in range(len(idx)) if mask.iloc[i]]
        x_axis_bottom = {"type": "value", "gridIndex": 1, **_axis_style()}

    option = {
        "title": {
            "text": "🛰️ GPS Tracking and Altitude Analysis",
            "left": "center",
        },
        "tooltip": {"trigger": "item"},
        "grid": [
            {"left": "6%", "right": "5%", "top": 60, "height": "52%"},
            {"left": "6%", "right": "5%", "top": "68%", "height": "24%"},
        ],
        "xAxis": [
            {
                "type": "value",
                "name": "Longitude",
                "nameTextStyle": {"color": _theme_text_color()},
                **_axis_style(),
            },
            x_axis_bottom,
        ],
        "yAxis": [
            {
                "type": "value",
                "name": "Latitude",
                "nameTextStyle": {"color": _theme_text_color()},
                **_axis_style(),
            },
            {
                "type": "value",
                "name": "Altitude (m)",
                "nameTextStyle": {"color": _theme_text_color()},
                "gridIndex": 1,
                **_axis_style(),
            },
        ],
        "series": [
            {
                "type": "line",
                "name": "Track",
                "xAxisIndex": 0,
                "yAxisIndex": 0,
                "data": track_pairs,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#1f77b4"},
            },
            {
                "type": "line",
                "name": "Altitude",
                "xAxisIndex": 1,
                "yAxisIndex": 1,
                "data": alt_pairs,
                "showSymbol": False,
                "lineStyle": {"width": 2, "color": "#2ca02c"},
            },
        ],
        "legend": {"top": 30},
    }
    return option


def get_available_columns(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return []
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    exclude_cols = ["message_id", "uptime_seconds"]
    return [col for col in numeric_columns if col not in exclude_cols]


def create_dynamic_chart_option(
    df: pd.DataFrame, chart_config: Dict[str, Any]
) -> Dict[str, Any]:
    if df.empty:
        return _empty_chart_option("No data available")

    x_col = chart_config.get("x_axis")
    y_col = chart_config.get("y_axis")
    chart_type = chart_config.get("chart_type", "line")
    title = chart_config.get("title") or (
        f"{y_col} vs {x_col}" if y_col and x_col else "Custom Chart"
    )

    if chart_type == "heatmap":
        # Correlation heatmap
        numeric_cols = get_available_columns(df)
        if len(numeric_cols) < 2:
            return _empty_chart_option(
                "Need at least 2 numeric columns for heatmap"
            )
        corr = df[numeric_cols].corr().fillna(0)
        x_labels = numeric_cols
        y_labels = numeric_cols
        data = []
        for i, yi in enumerate(y_labels):
            for j, xj in enumerate(x_labels):
                data.append([j, i, float(corr.loc[yi, xj])])
        return {
            "title": {"text": "🔥 Correlation Heatmap", "left": "center"},
            "tooltip": {"position": "top"},
            "grid": {"left": "10%", "right": "5%", "top": 60, "bottom": 60},
            "xAxis": {
                "type": "category",
                "data": x_labels,
                "axisLabel": {"rotate": 45, "color": _theme_text_color()},
            },
            "yAxis": {
                "type": "category",
                "data": y_labels,
                "axisLabel": {"color": _theme_text_color()},
            },
            "visualMap": {
                "min": -1,
                "max": 1,
                "calculable": True,
                "orient": "vertical",
                "left": "right",
                "top": "center",
                "inRange": {"color": ["#4575b4", "#ffffbf", "#d73027"]},
            },
            "series": [
                {
                    "name": "corr",
                    "type": "heatmap",
                    "data": data,
                    "label": {"show": False},
                }
            ],
        }

    # Non-heatmap charts
    if not y_col or y_col not in df.columns:
        return _empty_chart_option("Invalid Y-axis column selection")
    if chart_type not in ["histogram", "heatmap"] and (
        not x_col or x_col not in df.columns
    ):
        return _empty_chart_option("Invalid X-axis column selection")

    # Prepare data
    if chart_type == "line":
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            pairs = _to_time_pairs(df, x_col, y_col)
            x_axis = {"type": "time", **_axis_style()}
        else:
            # category or numeric x
            x_vals = df[x_col].astype(str).tolist()
            y_vals = pd.to_numeric(df[y_col], errors="coerce").tolist()
            pairs = list(zip(x_vals, y_vals))
            x_axis = {"type": "category", **_axis_style()}

        return {
            "title": {"text": title, "left": "center"},
            "tooltip": {"trigger": "axis"},
            "grid": {"left": "7%", "right": "5%", "top": 60, "bottom": 40},
            "xAxis": x_axis,
            "yAxis": {"type": "value", **_axis_style()},
            "series": [
                {
                    "type": "line",
                    "showSymbol": False,
                    "data": pairs,
                    "lineStyle": {"width": 2, "color": "#1f77b4"},
                }
            ],
        }

    if chart_type == "scatter":
        x_vals = pd.to_numeric(df[x_col], errors="coerce")
        y_vals = pd.to_numeric(df[y_col], errors="coerce")
        mask = (~x_vals.isna()) & (~y_vals.isna())
        data = [[float(x_vals[i]), float(y_vals[i])] for i in x_vals[mask].index]
        return {
            "title": {"text": title, "left": "center"},
            "tooltip": {"trigger": "item"},
            "grid": {"left": "7%", "right": "5%", "top": 60, "bottom": 40},
            "xAxis": {"type": "value", **_axis_style()},
            "yAxis": {"type": "value", **_axis_style()},
            "series": [{"type": "scatter", "symbolSize": 6, "data": data}],
        }

    if chart_type == "bar":
        recent_df = df.tail(20)
        if recent_df.empty:
            return _empty_chart_option("Not enough recent data for bar chart")
        x_vals = recent_df[x_col].astype(str).tolist()
        y_vals = pd.to_numeric(recent_df[y_col], errors="coerce").fillna(0).tolist()
        return {
            "title": {"text": title, "left": "center"},
            "tooltip": {"trigger": "axis"},
            "grid": {"left": "7%", "right": "5%", "top": 60, "bottom": 40},
            "xAxis": {"type": "category", "data": x_vals, **_axis_style()},
            "yAxis": {"type": "value", **_axis_style()},
            "series": [{"type": "bar", "data": y_vals, "itemStyle": {"color": "#2ca02c"}}],
        }

    if chart_type == "histogram":
        # compute histogram bins
        values = pd.to_numeric(df[y_col], errors="coerce").dropna().values
        if len(values) == 0:
            return _empty_chart_option("No numeric data for histogram")
        bins = min(40, max(10, int(np.sqrt(len(values)))))
        hist, bin_edges = np.histogram(values, bins=bins)
        x_centers = 0.5 * (bin_edges[:-1] + bin_edges[-1:0:-1][::-1])  # fix centers
        x_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
        data = [[float(x_centers[i]), int(hist[i])] for i in range(len(hist))]
        return {
            "title": {"text": f"Distribution of {y_col}", "left": "center"},
            "tooltip": {"trigger": "axis"},
            "grid": {"left": "7%", "right": "5%", "top": 60, "bottom": 40},
            "xAxis": {"type": "value", **_axis_style()},
            "yAxis": {"type": "value", **_axis_style()},
            "series": [{"type": "bar", "data": data, "itemStyle": {"color": "#d62728"}}],
        }

    # default fallback
    return _empty_chart_option("Unsupported chart type")


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
        st.warning(
            "⏳ No numeric data available for creating charts. Connect and wait for data."
        )
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
                    "x_axis": "timestamp"
                    if "timestamp" in df.columns
                    else (available_columns[0] if available_columns else None),
                    "y_axis": available_columns[0] if available_columns else None,
                }
                st.session_state.dynamic_charts.append(new_chart)
                st.rerun()
            except Exception as e:
                st.error(f"Error adding chart: {e}")

    with col2:
        if st.session_state.dynamic_charts:
            st.success(
                f"📈 {len(st.session_state.dynamic_charts)} custom chart(s) active"
            )

    if st.session_state.dynamic_charts:
        for i, chart_config in enumerate(list(st.session_state.dynamic_charts)):
            try:
                with st.container(border=True):
                    col1, col2, col3, col4, col5 = st.columns(
                        [2, 1.5, 1.5, 1.5, 0.5]
                    )

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
                            index=["line", "scatter", "bar", "histogram", "heatmap"].index(
                                chart_config.get("chart_type", "line")
                            ),
                            key=f"type_{chart_config['id']}",
                        )
                        if new_type != chart_config.get("chart_type"):
                            st.session_state.dynamic_charts[i]["chart_type"] = new_type

                    with col3:
                        if chart_config.get("chart_type", "line") not in [
                            "histogram",
                            "heatmap",
                        ]:
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
                                    index=x_options.index(current_x)
                                    if current_x in x_options
                                    else 0,
                                    key=f"x_{chart_config['id']}",
                                )
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
                                new_y = st.selectbox(
                                    "Y-Axis",
                                    options=available_columns,
                                    index=available_columns.index(current_y)
                                    if current_y in available_columns
                                    else 0,
                                    key=f"y_{chart_config['id']}",
                                )
                                if new_y != chart_config.get("y_axis"):
                                    st.session_state.dynamic_charts[i]["y_axis"] = new_y
                            else:
                                st.empty()

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
                                        for j, cfg in enumerate(
                                            st.session_state.dynamic_charts
                                        )
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
                        df_with_rp = calculate_roll_and_pitch(df)
                        opt = create_dynamic_chart_option(df_with_rp, chart_config)
                        st_echarts(
                            opt,
                            height="400px",
                            key=f"chart_plot_{chart_config['id']}",
                            notMerge=False,
                            lazyUpdate=True,
                        )
                    except Exception as e:
                        st.error(f"Error rendering chart: {e}")

            except Exception as e:
                st.error(f"Error rendering chart configuration: {e}")


# ===========================
# Main application
# ===========================
def main():
    st.markdown(
        '<div class="main-header">🏎️ Shell Eco-marathon Telemetry Dashboard '
        "(ECharts)</div>",
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
                            st.warning(
                                "⚠️ Supabase only connected (Ably not available or failed)"
                            )
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
                        time_since = (
                            datetime.now() - stats["last_message_time"]
                        ).total_seconds()
                        st.metric("⏱️ Last Msg", f"{time_since:.0f}s ago")
                    else:
                        st.metric("⏱️ Last Msg", "Never")
                if stats["last_error"]:
                    st.error(f"⚠️ {stats['last_error'][:40]}...")

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
            st.markdown(
                '<div class="status-indicator">📚 Historical Mode</div>',
                unsafe_allow_html=True,
            )
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
                    selected_session = st.session_state.historical_sessions[
                        selected_session_idx
                    ]
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
                                "📊 Loading "
                                f"{selected_session['record_count']:,} records... "
                                "This may take a moment due to pagination."
                            )
                        with st.spinner(
                            f"Loading data for session "
                            f"{selected_session['session_id'][:8]}..."
                        ):
                            historical_df = (
                                st.session_state.telemetry_manager.get_historical_data(
                                    selected_session["session_id"]
                                )
                            )
                            st.session_state.telemetry_data = historical_df
                            st.session_state.last_update = datetime.now()
                        if not historical_df.empty:
                            st.success(
                                f"✅ Loaded {len(historical_df):,} data points"
                            )
                        st.rerun()
            else:
                st.info("Click 'Refresh Sessions' to load available sessions from Supabase.")

        st.info(f"📡 Channel: {DASHBOARD_CHANNEL_NAME}")
        st.info(f"🔢 Max records per session: {MAX_DATAPOINTS_PER_SESSION:,}")

    # Main area
    df = st.session_state.telemetry_data.copy()
    new_messages_count = 0

    if st.session_state.data_source_mode == "realtime_session":
        if (
            st.session_state.telemetry_manager
            and st.session_state.telemetry_manager.is_connected
        ):
            new_messages = st.session_state.telemetry_manager.get_realtime_messages()
            current_session_data_from_supabase = pd.DataFrame()
            if new_messages and "session_id" in new_messages[0]:
                current_session_id = new_messages[0]["session_id"]
                if (
                    st.session_state.current_session_id != current_session_id
                    or st.session_state.telemetry_data.empty
                ):
                    st.session_state.current_session_id = current_session_id
                    with st.spinner(
                        f"Loading current session data for {current_session_id[:8]}..."
                    ):
                        current_session_data_from_supabase = (
                            st.session_state.telemetry_manager.get_current_session_data(
                                current_session_id
                            )
                        )
                    if not current_session_data_from_supabase.empty:
                        st.success(
                            "✅ Loaded "
                            f"{len(current_session_data_from_supabase):,} "
                            "historical points for current session"
                        )
            if new_messages or not current_session_data_from_supabase.empty:
                merged_data = merge_telemetry_data(
                    new_messages,
                    current_session_data_from_supabase,
                    st.session_state.telemetry_data,
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
            '<div class="historical-notice">📚 Viewing Historical Data - '
            "No auto-refresh active</div>",
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
                    "1. Click 'Refresh Sessions' to load available sessions\n"
                    "2. Select a session; its data will load automatically\n"
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
                    "Number of Historical Sessions": len(
                        st.session_state.historical_sessions
                    ),
                    "Telemetry Data Points (in memory)": len(
                        st.session_state.telemetry_data
                    ),
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
                            "Total Pagination Requests": stats["pagination_stats"][
                                "total_requests"
                            ],
                            "Total Rows Fetched": stats["pagination_stats"][
                                "total_rows_fetched"
                            ],
                            "Sessions Requiring Pagination": stats["pagination_stats"][
                                "sessions_paginated"
                            ],
                            "Largest Session Size": stats["pagination_stats"][
                                "largest_session_size"
                            ],
                        }
                    )
                st.json(debug_info)
        st.markdown("</div>", unsafe_allow_html=True)
        return

    analyze_data_quality(
        df,
        is_realtime=(st.session_state.data_source_mode == "realtime_session"),
    )
    if st.session_state.data_quality_notifications:
        for msg in st.session_state.data_quality_notifications:
            if "🚨" in msg:
                st.error(msg, icon="🚨")
            else:
                st.warning(msg, icon="⚠️")

    with st.container():
        col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
        with col1:
            if st.session_state.is_viewing_historical:
                st.info("📚 Historical")
            else:
                st.info("🔴 Real-time")
        with col2:
            st.info(f"📊 {len(df):,} data points available")
        with col3:
            st.info(
                f"⏰ Last update: {st.session_state.last_update.strftime('%H:%M:%S')}"
            )
        with col4:
            if (
                st.session_state.data_source_mode == "realtime_session"
                and new_messages_count > 0
            ):
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

    with tabs[0]:
        render_overview_tab(kpis)

    with tabs[1]:
        render_live_gauges(kpis, unique_ns="speedtab")
        render_kpi_header(kpis, unique_ns="speedtab", show_gauges=False)
        opt = create_speed_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(opt, height="400px", key="chart_speed_main", notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[2]:
        render_live_gauges(kpis, unique_ns="powertab")
        render_kpi_header(kpis, unique_ns="powertab", show_gauges=False)
        opt = create_power_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(opt, height="520px", key="chart_power_main", notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[3]:
        render_live_gauges(kpis, unique_ns="imutab")
        render_kpi_header(kpis, unique_ns="imutab", show_gauges=False)
        opt = create_imu_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(opt, height="700px", key="chart_imu_main", notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[4]:
        render_live_gauges(kpis, unique_ns="imudetailtab")
        render_kpi_header(kpis, unique_ns="imudetailtab", show_gauges=False)
        opt = create_imu_detail_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(opt, height="720px", key="chart_imu_detail_main", notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[5]:
        render_live_gauges(kpis, unique_ns="efftab")
        render_kpi_header(kpis, unique_ns="efftab", show_gauges=False)
        opt = create_efficiency_chart_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(opt, height="400px", key="chart_efficiency_main", notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[6]:
        render_live_gauges(kpis, unique_ns="gpstab")
        render_kpi_header(kpis, unique_ns="gpstab", show_gauges=False)
        opt = create_gps_map_with_altitude_option(df)
        st.markdown('<div class="chart-wrap">', unsafe_allow_html=True)
        st_echarts(opt, height="520px", key="chart_gps_main", notMerge=False, lazyUpdate=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[7]:
        render_live_gauges(kpis, unique_ns="customtab")
        render_kpi_header(kpis, unique_ns="customtab", show_gauges=False)
        render_dynamic_charts_section(df)

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
                        timestamp_series = pd.to_datetime(
                            df["timestamp"], errors="coerce", utc=True
                        )
                        timestamp_series = timestamp_series.dropna()
                        if len(timestamp_series) > 1:
                            time_span = (
                                timestamp_series.max() - timestamp_series.min()
                            )
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

    if (
        st.session_state.data_source_mode == "realtime_session"
        and st.session_state.auto_refresh
    ):
        if AUTOREFRESH_AVAILABLE:
            st_autorefresh(
                interval=st.session_state.refresh_interval * 1000,
                key="auto_refresh",
            )
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
# fmt: on
