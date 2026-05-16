# 🌌 Multi-Pipeline ETL & Reporting Framework
### *Unified Big Data Analytics for Web Server Logs*

![Hadoop MongoDB ETL Banner](./hadoop_mongodb_etl_banner_1777292768623.png)

---

## 📖 Project Overview
This project implements a robust **ETL (Extract, Transform, Load) and Reporting Framework** designed to process large-scale NASA HTTP logs. It features a pluggable architecture allowing seamless switching between **Hadoop MapReduce**, **MongoDB**, **Hive**, and **Pig**, while centralizing all results in a PostgreSQL database.

---

## 🖥 Modern Orchestration (The Frontend)
We have modernized the project with a **Premium Glassmorphic Web Dashboard** that allows for headless pipeline management.

### 🎧 Terminal Listener Architecture
To ensure a 100% environment match with your local Hadoop configuration, the system uses a **Listener-Bridge** pattern:
1. **Frontend (`Node.js`)**: Provides the interactive UI for selecting datasets and engines.
2. **Bridge (`.pipeline_queue`)**: The UI communicates with your terminal via an asynchronous file-based queue.
3. **Listener (`bash listener.sh`)**: A dedicated worker running in your terminal that picks up triggers and executes the Java/Hadoop code natively.

---

## 📊 Advanced Analytics Suite
The `Project/analysis` module provides a comprehensive suite of Java-based visualization tools using **JFreeChart**. These are automatically triggered via the **Master Analyzer**.

| Analyzer Class | Visualization | Description |
| :--- | :--- | :--- |
| **`DataQualityAnalyzer`** | 🥧 Pie Chart | Breaks down total records into **Valid** vs. **Malformed** segments to audit parsing integrity. |
| **`PipelineComparisonAnalyzer`**| 📊 Bar Chart | Compares the total, end-to-end execution runtime of different processing engines (e.g., MapReduce vs. Hive) on the same base dataset. |
| **`QueryTimingAnalyzer`** | ⏱️ Bar & Line Charts | A multi-purpose analyzer generating granular timings for Queries 1, 2, and 3, tracking sequential batch-wise latency trends, and benchmarking specific queries across pipelines. |
| **`BatchSizeImpactAnalyzer`** | ⚡ Line & Bar Charts | Evaluates system scalability by measuring Total Runtime, Records-per-Second Throughput, and Average Batch Latency tradeoffs when changing chunk sizes (e.g., 500 lines vs. 10,000 lines). |
| **`QueryResultAnalyzer`** | 📈 Statistical Graphs | Visualizes the actual mathematical output of the analytical queries (e.g., daily traffic spikes, HTTP status code distribution). |
| **`RunMetadataAnalyzer`** | 📋 Historical Trends | Aggregates and compares macro-level performance metrics across multiple historical execution sessions. |

---

## 🚀 Quick Start Guide
To run the full automated system, you need two terminals open:

### 1. Start the Web Server (Ubuntu/WSL Terminal)
Navigate to the frontend directory and start the Node.js server. **Note:** To ensure the History Library can connect to the Cloud DB, you must inject the environment variables from the root `.env` file.
```bash
cd Project/frontend
export $(grep -v '^#' ../.env | xargs) && node server.js
```

### 2. Start the Terminal Listener (Ubuntu/WSL Terminal)
In a second Ubuntu terminal (where `hadoop` and `mvn` are configured):
```bash
cd Project
bash listener.sh
```

### 3. Open the UI
Navigate to `http://localhost:3000`. 

### 4. 📚 Using the Historical Analysis Library
The dashboard now includes a **Run History** panel. You no longer need to re-run pipelines to see graphs!
- **Visualize History**: The UI lists all unique dataset/pipeline combinations stored in your database. Click **"Analyze Results"** on any previous run to immediately visualize its performance.
- **Hybrid Comparison Logic**: The analytics suite now uses "Hybrid Benchmarking." Your current run is always compared against the **Latest Successful** run of competitor engines. This automatically filters out partial runs or crashes (Total Runtime = 0 or Low Record Count) from your historical charts, ensuring you are always comparing against valid, completed work.

---

## 🔍 Parsing Strategy & Integrity
The framework utilizes a high-performance **Master Regular Expression** to extract 9 distinct fields from the Combined Log Format:

```regex
^(\S+)\s+\S+\s+\S+\s+\[(\d{2}/\w{3}/\d{4}):(\d{2}):\d{2}:\d{2}\s+[+-]\d{4}\]\s+"([A-Z]+)\s+(\S+)\s+(HTTP/[\d.]+)"\s+(\d{3})\s+(\S+)$
```
- ✅ **Data Cleaning**: Automatically handles `-` characters in byte fields, converting them to `0`.
- 🚨 **Malformed Tracking**: Records failing to match the regex are counted and reported as "Malformed" in `run_metadata`.

---

<p align="center">
  <i>Developed with ❤️ for the Big Data & NoSQL Community</i>
</p>
