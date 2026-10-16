@echo off
title SurgeSense Dashboard Launcher
echo Starting SurgeSense Dashboard on http://localhost:8000 ...
cd /d "d:\DockerHadoop\web-dashboard"
start "" "http://localhost:8000"
python -m http.server 8000
