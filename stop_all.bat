@echo off
title Stop All Services
echo ==========================================================
echo Stopping All Services...
echo ==========================================================

echo [1/3] Stopping Docker Spark jobs and bridge...
docker exec surgesense-spark-master bash -c "pkill -9 -f org.apache.spark.deploy.SparkSubmit; pkill -9 -f StreamingInference; pkill -9 -f replay_producer; pkill -9 -f bridge.py" >nul 2>&1

echo [2/3] Stopping Windows background python processes...
taskkill /f /im python.exe >nul 2>&1

echo [3/3] Done! All services stopped and ports 4040 and 8000 released.
echo ==========================================================
pause
