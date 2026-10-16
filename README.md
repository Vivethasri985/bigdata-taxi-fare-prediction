# Big-Data Driven Taxi Fare Prediction & Demand Hotspot Analytics

Distributed big data analytics and stream processing platform built on **Apache Spark 3.5.6**, **Hadoop HDFS 3.4.3**, and **Scala 2.12**, analyzing over **33.5 Million NYC Taxi trip records (45.3 GB replicated)**.

---

## 👥 Project Team & Course Information

* **Course:** Big Data Analytics
* **Faculty In-Charge:** Dr. Sreeja B. P, Assistant Professor Sr.Gd., School of Artificial Intelligence, Coimbatore

### Team Members
| Name | Register Number |
| :--- | :--- |
| **Aditya S** | `CB.AI.U4AID24104` |
| **Ashwin Naresh M** | `CB.AI.U4AID24106` |
| **Divya Rithanya S** | `CB.AI.U4AID24113` |
| **Vivethasri C** | `CB.AI.U4AID24162` |



## 🚀 The 5 V's of Big Data

| Dimension | Metric / Implementation | Description |
| :--- | :--- | :--- |
| **Volume** | **33.5M Rows (45.3 GB)** | Replicated across Hadoop HDFS with 128 MB block sizes across 2 DataNodes. |
| **Variety** | **Multi-Format Ingestion** | Snappy Parquet (trips) + Nested JSON (Open-Meteo weather) + CSV/GeoJSON (263 taxi zones). |
| **Velocity** | **3-Second Micro-Batches** | Spark Structured Streaming with a 1-hour stateful sliding window (5-minute slide). |
| **Veracity** | **Audited Data Quality** | Outlier bounds applied: distance 0.1 to 100 miles, fare $3.00 to $500.00; chronological 80/20 train/test split. |
| **Value** | **R² = 0.7998, MAE $4.40** | Transparent fare estimation and K-Means demand hotspot guidance for fleet dispatch. |

---

## 🏛️ System Architecture

```
[ NYC TLC Logs (Parquet) ] ──► [ Hadoop HDFS 3.4.3 ] ──► [ Spark 3.5.6 Engine ]
[ Open-Meteo Weather JSON ]        (1 NN + 2 DN)              (Master + Worker)
[ NYC Taxi Zones GIS ]                                                 │
                                                 ┌─────────────────────┴─────────────────────┐
                                                 ▼                                           ▼
                                    [ Supervised Regression ]                   [ Unsupervised Clustering ]
                                    19-Feature L-BFGS Ridge                     log(1+x) Transform + K-Means (K = 3)
                                    R² = 0.7998, MAE = $4.40                     Silhouette = 0.6399 (+64%)
                                                 │                                           │
                                                 └─────────────────────┬─────────────────────┘
                                                                       ▼
                                                       [ Spark Structured Streaming ]
                                                       3s Micro-Batches | 1h Window | 10m Watermark
                                                                       │
                                                                       ▼
                                                       [ Interactive Web Dashboard ]
                                                       Port 8000: Fare Calc, GIS Map, Stream Analytics
```

---

## 📊 Core Machine Learning & Analytics Results

### 1. Supervised Fare Regression: Baseline vs. Tuned
Evaluated on **6.7 Million chronologically held-out test trips** (Mar–May 2026):

| Evaluation Metric | Baseline (λ = 0.01) | Tuned (λ = 0.10) |
| :--- | :---: | :---: |
| **R-Squared Score (R²)** | 0.799582 | **0.799821** |
| **Root Mean Squared Error (RMSE)** | $7.7935 | **$7.7889** |
| **Mean Absolute Error (MAE)** | $4.4118 | **$4.4027** |
| **Tolerance Accuracy (Within ±$2.00)** | 26.0683% | **26.3034% (1.76M trips)** |

### 2. Unsupervised Demand Hotspots: K-Means Clustering (K = 3)
Using a **log(1 + pickup_count)** transform to remove extreme right-skew:
* **Optimal Clusters (K = 3):** Validated across K from 2 to 8 with peak Silhouette Score = **0.6399** (+64% over raw feature space).
* **Demand Regimes:**
  * **Low Demand:** ~3.5 trips/hour
  * **Moderate Demand:** ~8.9 trips/hour
  * **High-Demand Hotspot:** **119.1 trips/hour** (Represents 87.8% of citywide volume concentrated in Upper East Side, JFK Airport, and Midtown).

---

## ⚡ Real-Time Streaming Inference Engine
* **Micro-Batch Execution:** 3-second trigger interval.
* **Stateful Aggregation:** 1-hour sliding window updating every 5 minutes.
* **Event-Time Watermark:** 10 minutes to evict stale state and prevent memory leaks.
* **Fault-Tolerant Checkpoints:** Exactly-once delivery semantics via HDFS write ahead log.

---

## 📁 Repository Structure

```
├── docker-compose.yml           # 5-container cluster: HDFS NameNode + 2 DataNodes, Spark Master + Worker
├── config/                      # Hadoop core-site.xml and hdfs-site.xml cluster configurations
├── hadoop/bin/                  # Windows native Hadoop binary utilities (winutils.exe, hadoop.dll)
├── surgesense-project/          # Scala 2.12 SBT project
│   ├── build.sbt                # Spark MLlib, Spark SQL, and Hadoop dependencies
│   └── src/main/scala/surgesense/
│       ├── Preprocess.scala     # Main ingestion orchestrator
│       ├── Clean.scala          # Outlier bounds & data quality filtering
│       ├── Normalize.scala      # Schema & data type casting
│       ├── Enrich.scala         # GIS Taxi Zone and Weather join
│       ├── Features.scala       # Kinematic speed, duration & temporal feature derivation
│       ├── Phase1SplitAndEDA.scala # Chronological train/test split & EDA
│       ├── FinalModelFeatures.scala# 19-feature vector assembly
│       ├── FinalLinearRegression.scala # Ridge Regression L-BFGS training
│       ├── DemandHotspotsV2.scala # log1p K-Means clustering (K=3)
│       ├── FareErrorAnalysis.scala # Residual error slicing across distance & fare ranges
│       ├── StreamingInference.scala # Real-time Spark Structured Streaming engine
│       └── RunMLAndStreaming.scala # Unified pipeline launcher
├── spark-apps/
│   ├── replay_producer.py      # Real-time trip stream simulation producer
│   ├── bridge.py               # WebHDFS REST bridge (Port 9870) for live snapshot polling
│   ├── surgesense_2.12-1.0.jar  # Compiled production Scala application JAR
│   └── streaming_data.json     # Live aggregated streaming snapshot
├── web-dashboard/               # 4-Tab interactive analytical UI (Port 8000)
│   ├── index.html              # Fare Calculator, GIS Map, Model Analytics & Live Stream
│   ├── app.js                  # Frontend controllers & Leaflet GIS rendering
│   └── chart_images/           # High-resolution error slicing & hotspot distribution charts
├── start_all.bat                # 1-Click production launch script
└── stop_all.bat                 # Graceful cluster shutdown script
```

---

## 🛠️ Quick Start Guide

### 1. Prerequisites
* Docker Desktop (WSL2 Backend enabled)
* Java JDK 8 / 11
* Python 3.10+
* SBT (Scala Build Tool)

### 2. Launch the Platform
Double-click [`start_all.bat`](./start_all.bat) or run:
```cmd
start_all.bat
```
This automatically:
1. Boots the Hadoop HDFS cluster and Spark Master/Worker containers.
2. Starts the Spark Structured Streaming engine on Port `4040`.
3. Initiates the historical replay stream producer and WebHDFS bridge.
4. Opens the **Analytics Dashboard** on `http://localhost:8000` and **Spark Web UI** on `http://localhost:4040`.

---

## 📜 License
Academic Coursework Project &copy; 2026. Developed for Big Data Analytics.
