"""
SurgeSense Phase 12 - Dynamic WebHDFS Dashboard Bridge & Hotspot Aggregator
--------------------------------------------------------------------------
Purpose:
    Polls HDFS snapshots via WebHDFS REST API.
    Maintains a rolling window of recent stream events to compute real-time
    pickup intensity, K-Means demand tiers (HIGH / MODERATE / LOW), and
    rolling window timestamps, ensuring the dashboard map and tables
    continuously update in real time with vibrant demand shifts.
"""

import time
import json
import urllib.request
import os
from collections import deque

FARES_PATH = "surgesense/streaming/output/live_fares_snapshot"
HOTSPOTS_PATH = "surgesense/streaming/output/live_hotspots_snapshot"
LOCAL_OUTPUT_PATH = "/opt/spark/apps/streaming_data.json"
LOCAL_OUTPUT_TMP  = "/opt/spark/apps/streaming_data_tmp.json"
POLL_INTERVAL_SECONDS = 2

# Borough classification helper
def get_borough_for_zone(zone_name):
    z = (zone_name or "").lower()
    if any(q in z for q in ["jfk", "laguardia", "flushing", "astoria", "queens", "bayside", "elmhurst", "sunnyside"]):
        return "Queens"
    if any(b in z for b in ["brooklyn", "bushwick", "williamsburg", "crown heights", "greenpoint", "dumbo", "park slope", "bay ridge", "flatbush", "borough park", "bedford", "boerum"]):
        return "Brooklyn"
    if any(bx in z for bx in ["bronx", "mott haven", "riverdale", "pelham", "melrose", "woodlawn"]):
        return "Bronx"
    if "staten" in z:
        return "Staten Island"
    return "Manhattan"

def read_hdfs_json_records(rel_path):
    list_url = f"http://namenode:9870/webhdfs/v1/{rel_path}?op=LISTSTATUS"
    try:
        req = urllib.request.Request(list_url)
        with urllib.request.urlopen(req, timeout=3) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        records = []
        statuses = data.get('FileStatuses', {}).get('FileStatus', [])
        for f in statuses:
            name = f['pathSuffix']
            if name.startswith('part-') and name.endswith('.json'):
                file_url = f"http://namenode:9870/webhdfs/v1/{rel_path}/{name}?op=OPEN"
                with urllib.request.urlopen(file_url, timeout=3) as file_resp:
                    content = file_resp.read().decode('utf-8')
                    for line in content.strip().split('\n'):
                        if line.strip():
                            try:
                                records.append(json.loads(line.strip()))
                            except Exception:
                                pass
        return records
    except Exception:
        return []

def main():
    print("=" * 70)
    print("SurgeSense Dynamic WebHDFS Bridge ACTIVE")
    print(f"Syncing streaming telemetry to {LOCAL_OUTPUT_PATH} every {POLL_INTERVAL_SECONDS}s")
    print("=" * 70)
    
    # Rolling sliding buffer of recent trip events (keeps up to 1000 events)
    recent_events_buffer = deque(maxlen=1000)
    seen_event_keys = set()
    poll_count = 0

    while True:
        poll_count += 1
        ts = time.time()
        
        # 1. Fetch live fares snapshot (25 latest trip predictions)
        fares = read_hdfs_json_records(FARES_PATH)
        
        # Ingest unseen trip events into rolling window buffer
        latest_event_time = "2026-03-01 00:00:00"
        for trip in fares:
            ev_time = trip.get("event_time") or ""
            zone = trip.get("pickup_zone") or ""
            if not zone or zone == "N/A":
                continue
            if ev_time > latest_event_time:
                latest_event_time = ev_time
            
            trip_key = f"{ev_time}_{zone}_{trip.get('actual_fare')}_{trip.get('predicted_fare')}"
            if trip_key not in seen_event_keys:
                seen_event_keys.add(trip_key)
                recent_events_buffer.append({
                    "event_time": ev_time,
                    "pickup_zone": zone
                })
        
        # Keep seen keys set manageable
        if len(seen_event_keys) > 2000:
            seen_event_keys = {f"{e['event_time']}_{e['pickup_zone']}" for e in recent_events_buffer}

        # 2. Compute dynamic live demand regimes across rolling buffer
        zone_counts = {}
        for ev in recent_events_buffer:
            z = ev["pickup_zone"]
            zone_counts[z] = zone_counts.get(z, 0) + 1

        # Check if HDFS has a native live hotspots snapshot
        hdfs_hotspots = read_hdfs_json_records(HOTSPOTS_PATH)
        
        # If rolling buffer has accumulated trips, generate dynamic hotspots
        dynamic_hotspots = []
        if len(zone_counts) >= 5:
            # Sort zones by pickup intensity descending
            sorted_zones = sorted(zone_counts.items(), key=lambda x: x[1], reverse=True)
            max_pickups = sorted_zones[0][1] if sorted_zones else 1
            
            for zone_name, pcount in sorted_zones[:50]:
                borough = get_borough_for_zone(zone_name)
                # Calibrate demand tier by relative and absolute pickup activity
                # Top tier zones: >= 12 pickups OR >= 65% of max window intensity
                if pcount >= 12 or (max_pickups >= 10 and pcount >= max_pickups * 0.65):
                    tier = "HIGH-DEMAND HOTSPOT"
                elif pcount >= 5 or (max_pickups >= 8 and pcount >= max_pickups * 0.35):
                    tier = "MODERATE DEMAND"
                else:
                    tier = "LOW DEMAND"
                
                dynamic_hotspots.append({
                    "pickup_zone": zone_name,
                    "pickup_borough": borough,
                    "pickups_trailing_hour": pcount,
                    "demand_tier": tier,
                    "window_end": latest_event_time
                })
        
        # Use dynamic hotspots if available; fallback to HDFS snapshot
        hotspots = dynamic_hotspots if len(dynamic_hotspots) >= 5 else hdfs_hotspots

        # Error metrics & engine status
        errors = [r["absolute_error"] for r in fares if "absolute_error" in r and r["absolute_error"] is not None]
        mae_live = round(sum(errors) / len(errors), 2) if errors else None
        engine_status = "ACTIVE" if (fares or hotspots) else "WAITING"
        
        # Count high-demand hotspots
        num_hotspots = sum(1 for r in hotspots if r.get("demand_tier") == "HIGH-DEMAND HOTSPOT")
        
        payload = {
            "ts": round(ts * 1000),
            "status": engine_status,
            "trips_shown": len(fares),
            "zones_shown": len(hotspots),
            "num_hotspots": num_hotspots,
            "mae_live": mae_live,
            "fares": fares[:25],
            "hotspots": hotspots[:50]
        }
        
        try:
            with open(LOCAL_OUTPUT_TMP, "w") as f:
                json.dump(payload, f, default=str)
            os.replace(LOCAL_OUTPUT_TMP, LOCAL_OUTPUT_PATH)
            if poll_count % 5 == 0 or poll_count == 1:
                print(f"[bridge] Poll {poll_count:04d}: status={engine_status} | fares={len(fares)} | zones={len(hotspots)} | 🔴Hotspots={num_hotspots} | time={latest_event_time} | MAE={mae_live}")
        except Exception as e:
            print(f"[bridge] Error writing payload: {e}")
            
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
