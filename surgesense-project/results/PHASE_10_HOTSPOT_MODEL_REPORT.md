# Phase 10 — Demand Hotspot Detection: Final Model & Evolution Report

## Executive Summary
This report formalizes the selection of **K-Means V2** as the operational model for **Phase 10 (Demand Hotspot Detection)** in the SurgeSense architecture. Baseline V1 results are preserved to document the iterative engineering process and justify feature choices during project defense.

---

## 1. Model Progression & Evolution: V1 vs V2

```
                       [ Baseline V1 ]
        Features: Raw pickup count + 5 spatio-temporal/weather features
                     K = 2 | Silhouette = 0.3898
                                   │
                                   ▼
                       [ Feature Refinement ]
             • Log-transformation: log1p(pickup_count)
             • Denoising: isolate pickup intensity + pickup_hour
                                   │
                                   ▼
                   [ Final Operational Model: V2 ]
                      K = 3 | Silhouette = 0.6399
              (+64.2% increase in silhouette separation)
```

---

## 2. K-Selection Silhouette Comparison

Optimal $K$ evaluation evaluated across cluster counts $K \in [2, 8]$ on distributed Spark MLlib:

| $K$ | V1 Silhouette (Baseline) | V2 Silhouette (Refined) | Status |
|:---:|:------------------------:|:-----------------------:|:------:|
| 2   | **0.3898** (V1 Selected) | 0.5326                  | Suboptimal |
| **3** | 0.2680                 | **0.6399** (V2 Selected)| **Optimal Global Peak** |
| 4   | 0.3203                   | 0.6270                  | Good cohesion |
| 5   | 0.3892                   | 0.5643                  | Diminishing returns |
| 6   | 0.3702                   | 0.5550                  | Over-fragmentation |
| 7   | —                        | 0.5630                  | Over-fragmentation |
| 8   | —                        | 0.5394                  | Over-fragmentation |

---

## 3. Operational Cluster Profiles (Final V2 Model)

With $K=3$, the model yields three clean, non-overlapping operational demand tiers:

| Metric | Cluster 1: Low Demand | Cluster 0: Moderate Demand | Cluster 2: High Demand (Hotspot) |
|:---|:---:|:---:|:---:|
| **Observation Intervals** | 1,527,610 | 1,806,732 | **1,295,875** |
| **Avg Pickups / Hour** | **3.49** | **8.89** | **119.13** |
| **Pickups Range (Min - Max)** | 1 – 21 | 1 – 176 | **15 – 1,072** |
| **Total Trips Captured** | 5,325,920 (5.3M) | 16,057,207 (16.1M) | **154,373,528 (154.4M)** |
| **Share of Total Pickups** | 3.0% | 9.1% | **87.8%** |
| **Average Hour of Day** | 16.90 (5 PM) | 5.36 (5:30 AM off-peak) | **15.10 (3 PM peak ramp)** |
| **Weekend Ratio** | 29.7% | 29.1% | 28.3% |
| **Avg Temperature (°C)** | 14.16°C | 10.02°C | 13.57°C |
| **Avg Precipitation (mm)** | 0.152 mm | 0.130 mm | 0.148 mm |

---

## 4. Official System State & HDFS Storage

The HDFS directory structure has been updated to establish V2 as the production standard while retaining V1 as historical baseline:

### HDFS Model Registry:
- **Official Operational Model ($K=3$):**  
  `hdfs://namenode:9000/surgesense/models/demand_hotspots/kmeans` (Promoted from V2)
- **Baseline Model ($K=2$):**  
  `hdfs://namenode:9000/surgesense/models/demand_hotspots/kmeans_v1_baseline`

### HDFS Results Data:
- **Official Hotspot Tables:**  
  `hdfs://namenode:9000/surgesense/results/demand_hotspots/`
  - `cluster_summary/`
  - `hotspot_zones/`
  - `hourly_hotspots/`
  - `k_selection/`
  - `weather_analysis/`
  - `weekday_weekend/`
- **Baseline Comparison Archive:**  
  `hdfs://namenode:9000/surgesense/results/demand_hotspots_v1_baseline/`

---

## 5. Summary of Final Project Modelling Suite

Both analytical pillars of SurgeSense are now locked in:

| Track | Objective | Final Algorithm | Core Metric |
|:---|:---|:---|:---:|
| **Supervised** | Dynamic Fare Prediction | Linear Regression (`l-bfgs`, reg=0.01) | **$R^2 = 0.7998$** (MAE: $4.41) |
| **Unsupervised** | Spatio-Temporal Hotspot Detection | K-Means ($K=3$, $\log(1+\text{count}) + \text{hour}$) | **$\text{Silhouette} = 0.6399$** |
