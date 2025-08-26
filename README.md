# 🏎️ Shell Eco-marathon Telemetry Dashboard

[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)  
[![Status: Beta](https://img.shields.io/badge/status-beta-yellow)](https://github.com/ChosF/EcoTele/releases)

A full-stack, real-time and historical telemetry system for Shell Eco‑marathon vehicles.  
From an ESP32-based transmitter through cloud persistence to a Streamlit dashboard, you get live KPIs, maps, charts, and replayable past runs.

---

## 🚀 Pipeline Overview

```text
ESP32 Transmitter (Transmiter.cpp)
        └─ MQTT/SSL → Ably MQTT Broker →
Bridge & DB Writer (maindata.py)
   • republishes live → Ably Realtime Pub/Sub
   • batches & stores → Supabase (sessions)
        └─ Streamlit Dashboard (dashboard_080.py)
           • Real-time view
           • Historical session browser (paginated)
           • Custom charts
```

---

## ✨ v0.8 Beta Highlights 

- ECharts rendering path in the dashboard → faster real-time plotting, smoother zoom/pan, fixes scroll/resize issues on long sessions.  
- Full historical data workflow → list sessions, load by pagination, replay with session summary.  
- Custom session names and improved UX (gauges, GPS+altitude split, data quality hints).

Full changelog in the release notes.

---

## 🎯 Features

1. ESP32 Transmitter (`Transmiter.cpp`)  
   • FreeRTOS C++ app publishes speed, voltage, current, power, GPS, IMU via MQTT over SSL.

2. Bridge & Database (`maindata.py`)  
   • Consumes hardware/mock telemetry, republishes to Ably, and batches to Supabase with session metadata.

3. Dashboard (`dashboard_080.py`)  
   • Real‑time + Historical modes, session list/loader with pagination.  
   • KPIs, gauges, GPS with altitude, IMU (incl. roll/pitch), efficiency, and a custom chart builder.  
   • CSV export (full/sample), dataset statistics, data‑quality hints.

---

## 🏛️ Architecture Diagram

```text
┌────────────────────────┐
│  ESP32 Transmitter     │
│  (Transmiter.cpp)      │
└──────────┬─────────────┘
           │ MQTT/SSL → Ably MQTT
┌──────────▼─────────────┐
│ Bridge & DB Writer     │
│ (maindata.py)          │
│ • Live → Ably Realtime │
│ • Batch → Supabase     │
└──────────┬─────────────┘
           │ Pub/Sub & HTTP
┌──────────▼─────────────┐
│  Streamlit Dashboard   │
│  (dashboard_080.py)    │
│ • Live view            │
│ • Historical browser   │
│ • Custom charts (ECharts/Plotly) │
└────────────────────────┘
```

---

## 🏃 Quickstart

### Prerequisites
- Python 3.8+  
- ESP‑IDF toolchain (for `Transmiter.cpp`)  
- Install Python deps:
```bash
pip install -r requirements.txt
```

### 1) Flash & Run ESP32
```bash
idf.py set-target esp32
idf.py menuconfig   # configure Wi‑Fi & MQTT
idf.py build
idf.py -p /dev/ttyUSB0 flash monitor
```

### 2) Start Bridge & DB Writer
```bash
python maindata.py
```

### 3) Launch Dashboard
```bash
streamlit run dashboard_080.py
```

> You can also deploy on Streamlit Community Cloud by selecting `dashboard_080.py`.

---

## 🗂️ Repository Structure

```
EcoTele/
├── Transmiter.cpp        # ESP32 data transmitter (MQTT/SSL)
├── maindata.py           # Bridge + batch-to-Supabase service (sessions, persistence)
├── dashboard_080.py      # Current dashboard (ECharts + historical + custom charts)
├── dashboard_070.py      # Previous dashboard with full historical capability
├── dashboard_0B1.py      # Experimental build (single long page, no tabs)
├── demo_1.py             # First prototype (mock-only)
├── requirements.txt      # Python dependencies
├── LICENSE               # Apache 2.0
└── README.md             # This file
```

---

## 📅 Versions
- 0B1: Experimental, single scroll page (no tabs)  
- 070: Historical capability baseline  
- 080: ECharts performance + session names + refined UX

---

## 📄 License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).

---

Feedback, bug reports & contributions:  
https://github.com/ChosF/EcoTele/issues
