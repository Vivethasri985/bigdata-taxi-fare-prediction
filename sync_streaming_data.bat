@echo off
REM SurgeSense - Copy streaming_data.json from container to web-dashboard every 3 seconds
:loop
docker cp surgesense-spark-master:/opt/spark/apps/streaming_data.json d:\DockerHadoop\web-dashboard\streaming_data.json 2>nul
timeout /t 3 /nobreak >nul
goto loop
