@echo off
title Taxi Analytics - All Services Launcher
echo ==========================================================
echo Starting All Services...
echo ==========================================================

echo [0/5] Cleaning up previous Spark jobs and bridge processes...
docker exec surgesense-spark-master bash -c "pkill -9 -f org.apache.spark.deploy.SparkSubmit; pkill -9 -f StreamingInference; pkill -9 -f replay_producer; pkill -9 -f bridge.py" >nul 2>&1
echo Waiting 3 seconds for port 4040 to release...
ping 127.0.0.1 -n 4 >nul

echo [1/5] Starting Spark Streaming Engine on Port 4040...
docker exec -d surgesense-spark-master /opt/spark/bin/spark-submit --class surgesense.StreamingInference --master local[2] --conf spark.ui.port=4040 --conf spark.port.maxRetries=0 --conf spark.ui.bindAddress=0.0.0.0 --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 /opt/spark/apps/surgesense_2.12-1.0.jar

echo [2/5] Starting Historical Replay Stream Producer...
docker exec -d surgesense-spark-master /opt/spark/bin/spark-submit --master local[1] --conf spark.ui.enabled=false --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 /opt/spark/apps/replay_producer.py

echo [3/5] Starting WebHDFS Dashboard Bridge...
docker exec -d surgesense-spark-master python3 /opt/spark/apps/bridge.py

echo [4/5] Starting Host Web Dashboard and Sync Process...
start "DashboardServer" /min python -m http.server 8000 --directory "d:\DockerHadoop\web-dashboard"
start "SyncStreaming" /min python "d:\DockerHadoop\sync_streaming.py"

echo Waiting 5 seconds for engines to initialize...
ping 127.0.0.1 -n 6 >nul

echo [5/5] Opening Spark UI (4040) and Dashboard (8000) in Browser...
start "" "http://localhost:4040"
start "" "http://localhost:8000"

echo ==========================================================
echo All Services are fully ACTIVE!
echo Spark Application UI: http://localhost:4040
echo Analytics Dashboard:  http://localhost:8000
echo ==========================================================
