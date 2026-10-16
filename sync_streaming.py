import time, shutil, os
SRC  = r"d:\DockerHadoop\spark-apps\streaming_data.json"
DEST = r"d:\DockerHadoop\web-dashboard\streaming_data.json"
print("SurgeSense Sync: watching streaming_data.json ...")
last_mtime = None
while True:
    try:
        if os.path.exists(SRC):
            mtime = os.path.getmtime(SRC)
            if mtime != last_mtime:
                shutil.copy2(SRC, DEST)
                last_mtime = mtime
                print(f"  [sync] updated at {time.strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"  [sync] error: {e}")
    time.sleep(2)
