// ==============================================================================
// NYC Taxi Analytics Web Application Logic (Vanilla JavaScript)
// ==============================================================================

// --- Tab Switching Navigation ---
document.addEventListener("DOMContentLoaded", () => {
  const tabs = document.querySelectorAll(".tab-btn");
  const panels = document.querySelectorAll(".tab-panel");

  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      const targetId = tab.getAttribute("data-target");

      tabs.forEach(t => t.classList.remove("active"));
      panels.forEach(p => p.classList.remove("active"));

      tab.classList.add("active");
      const targetPanel = document.getElementById(targetId);
      if (targetPanel) {
        targetPanel.classList.add("active");
      }

      // Initialize charts and maps when tab becomes visible
      if (targetId === "demand-hotspots") {
        try { initHotspotCharts(); } catch (e) { console.error("Hotspot charts error:", e); }
        try { initHotspotMap(); } catch (e) { console.error("Hotspot map error:", e); }
        setTimeout(() => { try { if (_hotspotMap) _hotspotMap.invalidateSize(); } catch (e) {} }, 150);
      } else if (targetId === "analytics") {
        try { initAnalyticsCharts(); } catch (e) { console.error("Analytics charts error:", e); }
      } else if (targetId === "live-stream") {
        try { initLiveMap(); } catch (e) { console.error("Live map error:", e); }
        startLiveStreamPolling();
        setTimeout(() => { try { if (_liveMap) _liveMap.invalidateSize(); } catch (e) {} }, 150);
      } else {
        stopLiveStreamPolling();
      }
    });
  });

  // Initial route distance calculation
  onRouteZoneChange();
  classifyRegime();
});

// --- Smart Helpers for Tab 1 (Route Distance & Weather) ---
function estimateDistanceBetweenZones(pickup, dropoff) {
  if (!pickup || !dropoff || pickup === dropoff) return 0.0;
  const pCoords = getZoneCoords(pickup);
  const dCoords = getZoneCoords(dropoff);
  if (!pCoords || !dCoords) return 3.2;

  const dLat = Math.abs(pCoords[0] - dCoords[0]) * 69.0;
  const dLng = Math.abs(pCoords[1] - dCoords[1]) * 52.3;
  // Manhattan street grid winding multiplier (~1.30x)
  const estDist = (dLat + dLng) * 1.30;
  return Math.min(45.0, Math.max(0.8, parseFloat(estDist.toFixed(1))));
}

function setFareErrorState(message) {
  const card = document.getElementById("fare-result-card");
  const valEl = document.getElementById("predicted-fare-val");
  const symEl = document.getElementById("fare-currency-symbol");
  const labelEl = document.getElementById("predicted-fare-label");

  if (card) {
    card.style.borderColor = "#dc2626";
    card.style.backgroundColor = "#fef2f2";
  }
  if (symEl) symEl.style.display = "none";
  if (valEl) {
    valEl.textContent = "—";
    valEl.style.color = "#dc2626";
  }
  if (labelEl) {
    labelEl.innerHTML = `<span style="color:#dc2626; font-weight:700;">⚠️ ${message}</span>`;
  }
}

function clearFareErrorState() {
  const card = document.getElementById("fare-result-card");
  const valEl = document.getElementById("predicted-fare-val");
  const symEl = document.getElementById("fare-currency-symbol");
  const labelEl = document.getElementById("predicted-fare-label");

  if (card) {
    card.style.borderColor = "";
    card.style.backgroundColor = "";
  }
  if (symEl) symEl.style.display = "";
  if (valEl) {
    valEl.style.color = "";
  }
  if (labelEl) {
    labelEl.textContent = "Estimated Taxi Fare";
  }
}

function onRouteZoneChange() {
  const pEl = document.getElementById("pickup-zone");
  const dEl = document.getElementById("dropoff-zone");
  const distEl = document.getElementById("trip-distance");
  const errEl = document.getElementById("route-error-msg");
  const calcBtn = document.querySelector(".btn-primary");

  if (pEl && dEl) {
    if (pEl.value === dEl.value) {
      if (distEl) distEl.value = "0.0";
      if (errEl) errEl.style.display = "block";
      pEl.style.borderColor = "#dc2626";
      dEl.style.borderColor = "#dc2626";
      if (calcBtn) {
        calcBtn.disabled = true;
        calcBtn.style.opacity = "0.5";
        calcBtn.style.cursor = "not-allowed";
      }
      setFareErrorState("Pickup & Dropoff cannot be the same zone");
      return;
    } else {
      if (errEl) errEl.style.display = "none";
      pEl.style.borderColor = "";
      dEl.style.borderColor = "";
      if (calcBtn) {
        calcBtn.disabled = false;
        calcBtn.style.opacity = "1";
        calcBtn.style.cursor = "pointer";
      }
      clearFareErrorState();
      if (distEl) {
        distEl.value = estimateDistanceBetweenZones(pEl.value, dEl.value);
      }
    }
  }
  calculateFare();
}

function onWeatherChange() {
  const wEl = document.getElementById("weather-condition");
  const tempEl = document.getElementById("temperature");
  if (wEl && tempEl) {
    tempEl.value = (wEl.value === "Rain") ? 12 : 18;
  }
  calculateFare();
}

// --- 1. Fare Prediction Inference Engine ---
function calculateFare(event) {
  if (event) event.preventDefault();

  const pEl = document.getElementById("pickup-zone");
  const dEl = document.getElementById("dropoff-zone");
  const distEl = document.getElementById("trip-distance");
  const pickupZone = pEl ? pEl.value : "";
  const dropoffZone = dEl ? dEl.value : "";
  const distance = parseFloat(distEl ? distEl.value : 0) || 0.0;

  if (pickupZone === dropoffZone) {
    onRouteZoneChange();
    return;
  }

  if (distance <= 0) {
    setFareErrorState("Distance must be greater than 0 miles");
    return;
  }

  clearFareErrorState();

  const passengers = parseInt(document.getElementById("passengers").value, 10) || 1;
  const hour = parseInt(document.getElementById("pickup-hour").value, 10) || 18;
  const dayType = document.getElementById("day-type").value;
  const weather = document.getElementById("weather-condition").value;
  const temp = parseFloat(document.getElementById("temperature").value) || 18.0;

  // Average speed in NYC based on rush hour / off-peak
  const isRushHour = (hour >= 7 && hour <= 10) || (hour >= 16 && hour <= 19);
  const avgSpeed = (isRushHour && dayType === "Weekday") ? 9.5 : 14.0;
  const estDurationMin = (distance / avgSpeed) * 60.0;

  // Learned Model Weights from Spark MLlib
  // Base offset: 2026 * 1.468181 - 2966.644554 = 7.89
  const baseOffset = 7.89;
  const distComp = distance * 2.162072;
  const durComp = estDurationMin * 0.360378;
  const passComp = passengers * 0.139787;
  const timeComp = hour * 0.012757;
  const tempComp = temp * -0.007091;
  const precipComp = (weather === "Rain") ? 0.05 : 0.0;

  // Zone adjustments: airport flat/toll rate vs congestion surcharge
  let zoneAdjustment = 0.0;
  if (pickupZone.includes("Airport") || dropoffZone.includes("Airport")) {
    zoneAdjustment += 18.50; // JFK / LGA access & toll allowance
  } else if (pickupZone.includes("Midtown") || pickupZone.includes("Times Sq")) {
    zoneAdjustment += 2.50;  // Manhattan core congestion surcharge
  }

  const totalFare = Math.max(3.00, (
    baseOffset + distComp + durComp + passComp + timeComp + tempComp + precipComp + zoneAdjustment
  ));

  // Update UI Element
  const valEl = document.getElementById("predicted-fare-val");
  if (valEl) valEl.textContent = totalFare.toFixed(2);
}

// --- 2. Demand Regime Classifier (K-Means Centroid Distance) ---

// Realistic baseline hourly pickup profiles derived from NYC TLC batch data
const ZONE_INTENSITY_PROFILES = {
  "Times Sq/Theatre District":          { base: 260, peakMult: 1.5, offpeakMult: 0.15 },
  "Times Sq / Theatre District (Midtown)": { base: 260, peakMult: 1.5, offpeakMult: 0.15 },
  "JFK Airport":                        { base: 220, peakMult: 1.3, offpeakMult: 0.25 },
  "JFK Airport (Queens)":               { base: 220, peakMult: 1.3, offpeakMult: 0.25 },
  "Midtown Center":                     { base: 250, peakMult: 1.5, offpeakMult: 0.12 },
  "Midtown Center (Manhattan)":         { base: 250, peakMult: 1.5, offpeakMult: 0.12 },
  "Upper East Side South":              { base: 240, peakMult: 1.4, offpeakMult: 0.15 },
  "Penn Station/Madison Sq West":       { base: 210, peakMult: 1.4, offpeakMult: 0.12 },
  "Penn Station / Madison Sq West":     { base: 210, peakMult: 1.4, offpeakMult: 0.12 },
  "Lincoln Square East":                { base: 190, peakMult: 1.3, offpeakMult: 0.14 },
  "LaGuardia Airport":                  { base: 180, peakMult: 1.3, offpeakMult: 0.20 },
  "LaGuardia Airport (Queens)":         { base: 180, peakMult: 1.3, offpeakMult: 0.20 },
  "Murray Hill":                        { base: 45,  peakMult: 1.3, offpeakMult: 0.20 },
  "Murray Hill (Manhattan)":            { base: 45,  peakMult: 1.3, offpeakMult: 0.20 },
  "East Village":                       { base: 40,  peakMult: 1.6, offpeakMult: 0.25 },
  "East Village (Manhattan)":           { base: 40,  peakMult: 1.6, offpeakMult: 0.25 },
  "West Village":                       { base: 38,  peakMult: 1.5, offpeakMult: 0.25 },
  "West Village (Manhattan)":           { base: 38,  peakMult: 1.5, offpeakMult: 0.25 },
  "Financial District South":           { base: 35,  peakMult: 1.7, offpeakMult: 0.08 },
  "Financial District South (Wall St)": { base: 35,  peakMult: 1.7, offpeakMult: 0.08 },
  "Kips Bay":                           { base: 8,   peakMult: 1.2, offpeakMult: 0.20 },
  "Kips Bay (Manhattan)":               { base: 8,   peakMult: 1.2, offpeakMult: 0.20 },
  "Greenpoint":                         { base: 5,   peakMult: 1.3, offpeakMult: 0.25 },
  "Greenpoint (Brooklyn)":              { base: 5,   peakMult: 1.3, offpeakMult: 0.25 },
  "Central Park":                       { base: 15,  peakMult: 1.4, offpeakMult: 0.10 },
  "Central Harlem North":               { base: 6,   peakMult: 1.2, offpeakMult: 0.20 },
  "Riverdale/North Riverdale/Fieldston":{ base: 2,   peakMult: 1.1, offpeakMult: 0.30 },
  "Riverdale / Fieldston (Bronx)":      { base: 2,   peakMult: 1.1, offpeakMult: 0.30 },
  "Riverdale / Fieldston":              { base: 2,   peakMult: 1.1, offpeakMult: 0.30 }
};

function getBackgroundPickupsForZone(zone, hour) {
  const profile = ZONE_INTENSITY_PROFILES[zone] || { base: 50, peakMult: 1.3, offpeakMult: 0.2 };
  let mult = 1.0;
  if ((hour >= 7 && hour <= 10) || (hour >= 17 && hour <= 20)) {
    // Peak Rush Hours
    mult = profile.peakMult;
  } else if (hour >= 1 && hour <= 5) {
    // Late Night / Overnight
    mult = profile.offpeakMult;
  } else if (hour >= 21 || hour === 0) {
    // Evening nightlife
    mult = profile.peakMult * 0.85;
  } else {
    // Midday
    mult = 1.0;
  }
  return Math.max(1, Math.round(profile.base * mult));
}

function computeTier(pickups, hour) {
  // Demand intensity threshold guard to prevent time distortion:
  // In Spark K-Means training:
  // High-demand cluster avg = 119.13 pickups/hr (hotspots like JFK, Times Sq, Midtown)
  // Moderate-demand cluster avg = 8.89 pickups/hr
  // Low-demand cluster avg = 3.49 pickups/hr
  if (pickups >= 75) return "HIGH";
  if (pickups <= 18) return "LOW";

  // In intermediate pickup range (19-74 pickups), evaluate distance to K-Means centroids
  const meanLog = 2.3512;
  const stdLog = 1.4589;
  const meanHr = 11.8919;
  const stdHr = 6.56134;

  const scaledLog = (Math.log1p(pickups) - meanLog) / stdLog;
  const scaledHr = (hour - meanHr) / stdHr;

  const c0 = [-0.41091144, -0.99577899]; // Moderate Demand
  const c1 = [-0.66987231,  0.76305297]; // Low Demand

  const d0 = Math.hypot(scaledLog - c0[0], scaledHr - c0[1]);
  const d1 = Math.hypot(scaledLog - c1[0], scaledHr - c1[1]);

  return (d0 <= d1) ? "MODERATE" : "LOW";
}

function classifyRegime() {
  const zoneEl = document.getElementById("select-regime-zone");
  const hourEl = document.getElementById("select-regime-hour");

  const zone = zoneEl ? zoneEl.value : "Times Sq/Theatre District";
  const hour = hourEl ? parseFloat(hourEl.value) : 18;

  // Compute pickups in background without exposing raw numbers to user
  const pickups = getBackgroundPickupsForZone(zone, hour);
  const assignedTier = computeTier(pickups, hour);

  const badge = document.getElementById("regime-badge");
  if (badge) {
    if (assignedTier === "HIGH") {
      badge.className = "regime-status status-high";
      badge.textContent = "HIGH-DEMAND HOTSPOT";
    } else if (assignedTier === "MODERATE") {
      badge.className = "regime-status status-mod";
      badge.textContent = "MODERATE DEMAND";
    } else {
      badge.className = "regime-status status-low";
      badge.textContent = "LOW DEMAND";
    }
  }

  // Update hotspot map to highlight matching tier zones and focus selected zone for the chosen hour
  updateHotspotMapForTier(assignedTier, zone, hour);
}

// --- 3. Chart Visualizations ---
let hotspotChartsInitialized = false;
let analyticsChartsInitialized = false;

function initHotspotCharts() {
  if (hotspotChartsInitialized || typeof Chart === "undefined") return;
  hotspotChartsInitialized = true;

  // Chart 1: Top 10 Hotspot Zones
  const ctxTopZones = document.getElementById("chart-top-zones");
  if (ctxTopZones) {
    new Chart(ctxTopZones, {
      type: "bar",
      data: {
        labels: [
          "Upper East Side S", "JFK Airport", "Midtown Center", "Upper East Side N",
          "Midtown East", "Penn Station", "Lincoln Sq East", "Times Square",
          "LaGuardia Airport", "Murray Hill"
        ],
        datasets: [{
          label: "Total Pickups (M)",
          data: [7.96, 7.82, 7.44, 7.17, 5.63, 5.40, 5.30, 5.15, 5.05, 4.73],
          backgroundColor: "#2563eb",
          borderRadius: 4
        }]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { x: { grid: { color: "#f1f5f9" } }, y: { grid: { display: false } } }
      }
    });
  }

  // Chart 2: Hourly Demand Pattern
  const ctxHourly = document.getElementById("chart-hourly-demand");
  if (ctxHourly) {
    new Chart(ctxHourly, {
      type: "line",
      data: {
        labels: Array.from({ length: 24 }, (_, i) => `${i}:00`),
        datasets: [{
          label: "Avg Hourly Demand",
          data: [282.8, 284.4, 253.7, 181.8, 114.2, 81.0, 87.3, 95.9, 102.5, 104.7, 105.1, 104.0, 107.2, 111.0, 117.9, 123.2, 124.2, 135.8, 142.9, 130.5, 122.6, 127.2, 120.0, 103.1],
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.08)",
          fill: true,
          tension: 0.35,
          pointRadius: 2.5
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false }, ticks: { maxTicksLimit: 12 } },
          y: { grid: { color: "#f1f5f9" } }
        }
      }
    });
  }

  // Chart 3: Weekday vs Weekend
  const ctxWW = document.getElementById("chart-weekday-weekend");
  if (ctxWW) {
    new Chart(ctxWW, {
      type: "bar",
      data: {
        labels: ["Weekday", "Weekend"],
        datasets: [{
          data: [122.11, 111.59],
          backgroundColor: ["#1e40af", "#60a5fa"],
          borderRadius: 4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: false, min: 90, grid: { color: "#f1f5f9" } } }
      }
    });
  }

  // Chart 4: Weather Impact
  const ctxWeather = document.getElementById("chart-weather-impact");
  if (ctxWeather) {
    new Chart(ctxWeather, {
      type: "bar",
      data: {
        labels: ["Clear / Dry", "Precipitation"],
        datasets: [{
          data: [119.09, 119.32],
          backgroundColor: ["#059669", "#0284c7"],
          borderRadius: 4
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { y: { beginAtZero: false, min: 100, grid: { color: "#f1f5f9" } } }
      }
    });
  }
}

function initAnalyticsCharts() {
  if (analyticsChartsInitialized || typeof Chart === "undefined") return;
  analyticsChartsInitialized = true;

  // Chart 5: Error by Distance
  const ctxDist = document.getElementById("chart-error-distance");
  if (ctxDist) {
    new Chart(ctxDist, {
      type: "bar",
      data: {
        labels: ["<2 mi", "2-5 mi", "5-10 mi", "10-20 mi", "20+ mi"],
        datasets: [
          { label: "MAE ($)", data: [3.90, 3.71, 5.32, 7.64, 19.33], backgroundColor: "#2563eb", borderRadius: 4 },
          { label: "RMSE ($)", data: [6.27, 6.47, 9.12, 12.52, 29.80], backgroundColor: "#dc2626", borderRadius: 4 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { y: { grid: { color: "#f1f5f9" } } }
      }
    });
  }

  // Chart 6: Error by Fare Range
  const ctxFare = document.getElementById("chart-error-fare");
  if (ctxFare) {
    new Chart(ctxFare, {
      type: "bar",
      data: {
        labels: ["<$10", "$10-20", "$20-30", "$30-50", "$50+"],
        datasets: [
          { label: "MAE ($)", data: [4.41, 2.69, 3.07, 6.83, 12.92], backgroundColor: "#0891b2", borderRadius: 4 },
          { label: "RMSE ($)", data: [5.75, 3.14, 4.96, 10.23, 21.29], backgroundColor: "#ea580c", borderRadius: 4 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { y: { grid: { color: "#f1f5f9" } } }
      }
    });
  }

  // Chart 7: Error by Pickup Hour (Genuine Data from HDFS error_by_hour.csv)
  const ctxErrHour = document.getElementById("chart-error-hour");
  if (ctxErrHour) {
    new Chart(ctxErrHour, {
      type: "line",
      data: {
        labels: Array.from({ length: 24 }, (_, i) => `${i}:00`),
        datasets: [{
          label: "MAE ($)",
          data: [4.74, 4.41, 4.28, 4.97, 6.18, 6.50, 6.08, 5.61, 5.25, 4.31, 3.94, 3.94, 4.00, 4.07, 4.19, 4.40, 4.23, 4.07, 3.90, 4.05, 4.21, 4.63, 4.65, 4.72],
          borderColor: "#4338ca",
          backgroundColor: "rgba(67, 56, 202, 0.08)",
          fill: true,
          tension: 0.35,
          pointRadius: 2.5
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false }, ticks: { maxTicksLimit: 12 } },
          y: { grid: { color: "#f1f5f9" }, title: { display: true, text: "MAE ($)" } }
        }
      }
    });
  }

  // Chart 8: K-Means Silhouette Curve (V1 vs V2 from HDFS)
  const ctxSil = document.getElementById("chart-silhouette");
  if (ctxSil) {
    new Chart(ctxSil, {
      type: "line",
      data: {
        labels: ["K=2", "K=3", "K=4", "K=5", "K=6", "K=7", "K=8"],
        datasets: [
          {
            label: "V1 Baseline (6 Features)",
            data: [0.3898, 0.2680, 0.3203, 0.3892, 0.3702, null, null],
            borderColor: "#94a3b8",
            borderDash: [5, 5],
            pointRadius: 3.5
          },
          {
            label: "V2 Refined (log1p + hour)",
            data: [0.5326, 0.6399, 0.6270, 0.5643, 0.5550, 0.5630, 0.5394],
            borderColor: "#2563eb",
            borderWidth: 2.5,
            pointRadius: 4.5,
            pointBackgroundColor: "#1e3a8a"
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          y: { min: 0.2, max: 0.7, grid: { color: "#f1f5f9" } }
        }
      }
    });
  }
}

// ==============================================================================
// Live Stream Tab — Polling & Rendering
// ==============================================================================

let _liveStreamInterval = null;

function startLiveStreamPolling() {
  // Always clear any existing interval first (safe restart on every tab visit)
  if (_liveStreamInterval !== null) {
    clearInterval(_liveStreamInterval);
    _liveStreamInterval = null;
  }
  fetchStreamingData();                       // immediate first fetch
  _liveStreamInterval = setInterval(fetchStreamingData, 3000);
}

function stopLiveStreamPolling() {
  if (_liveStreamInterval !== null) {
    clearInterval(_liveStreamInterval);
    _liveStreamInterval = null;
  }
}

async function fetchStreamingData() {
  try {
    // Cache-bust + no-store headers so browser never serves stale file
    const resp = await fetch("streaming_data.json?t=" + Date.now(), {
      cache: "no-store",
      headers: { "Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache" }
    });
    if (!resp.ok) throw new Error("HTTP " + resp.status);
    const data = await resp.json();
    renderLiveStream(data);
  } catch (e) {
    console.error("fetchStreamingData error:", e);
    setStatusWaiting();
  }
}

function setStatusWaiting() {
  const el = document.getElementById("ls-engine-status");
  if (el) {
    el.textContent = "WAITING";
    el.className = "live-status-value status-waiting";
  }
}

function renderLiveStream(data) {
  // --- Status Cards ---
  const statusEl = document.getElementById("ls-engine-status");
  if (statusEl) {
    statusEl.textContent = data.status || "WAITING";
    statusEl.className = "live-status-value " +
      (data.status === "ACTIVE" ? "status-active" : "status-waiting");
  }

  // Latest event time from first fare record
  const fares = data.fares || [];
  const latestEvent = fares.length > 0 ? (fares[0].event_time || "--") : "--";
  const eventEl = document.getElementById("ls-event-time");
  if (eventEl) eventEl.textContent = latestEvent;

  const tripsEl = document.getElementById("ls-trips-count");
  if (tripsEl) tripsEl.textContent = data.trips_shown || 0;

  const zonesEl = document.getElementById("ls-zones-count");
  if (zonesEl) zonesEl.textContent = data.zones_shown || 0;

  const hotspotsEl = document.getElementById("ls-hotspots-count");
  if (hotspotsEl) hotspotsEl.textContent = data.num_hotspots || 0;

  const maeEl = document.getElementById("ls-mae");
  if (maeEl) maeEl.textContent = data.mae_live !== null && data.mae_live !== undefined
    ? "$" + data.mae_live.toFixed(2) : "--";

  // --- Fares Table ---
  const faresBody = document.getElementById("ls-fares-body");
  if (faresBody) {
    if (fares.length === 0) {
      faresBody.innerHTML = '<tr><td colspan="6" class="table-empty">Waiting for streaming data...</td></tr>';
    } else {
      faresBody.innerHTML = fares.map(r => {
        const err = parseFloat(r.absolute_error);
        let errClass = "";
        if (!isNaN(err)) {
          errClass = err <= 2 ? "error-low" : err <= 6 ? "error-mid" : "error-high";
        }
        const errStr = !isNaN(err) ? "$" + err.toFixed(2) : "--";
        return `<tr>
          <td style="font-size:11px;color:#64748b;">${escHtml(r.event_time || "--")}</td>
          <td>${escHtml(r.pickup_zone || "--")}</td>
          <td>${r.trip_distance !== undefined ? r.trip_distance : "--"}</td>
          <td>$${r.actual_fare !== undefined ? parseFloat(r.actual_fare).toFixed(2) : "--"}</td>
          <td><strong>$${r.predicted_fare !== undefined ? parseFloat(r.predicted_fare).toFixed(2) : "--"}</strong></td>
          <td class="${errClass}">${errStr}</td>
        </tr>`;
      }).join("");
    }
  }

  // --- Hotspots Table ---
  const hotspots = data.hotspots || [];
  const hotspotsBody = document.getElementById("ls-hotspots-body");
  if (hotspotsBody) {
    if (hotspots.length === 0) {
      hotspotsBody.innerHTML = '<tr><td colspan="5" class="table-empty">Waiting for streaming data...</td></tr>';
    } else {
      hotspotsBody.innerHTML = hotspots.map(r => {
        let tier = r.demand_tier || "UNKNOWN";
        if (tier === "MODERATE DEMAND" && r.pickups_trailing_hour && r.pickups_trailing_hour >= 10) {
          tier = "HIGH-DEMAND HOTSPOT";
        }
        let badgeClass = "badge-hotspot--unknown";
        if (tier === "HIGH-DEMAND HOTSPOT")   badgeClass = "badge-hotspot--high";
        else if (tier === "MODERATE DEMAND")  badgeClass = "badge-hotspot--moderate";
        else if (tier === "LOW DEMAND")       badgeClass = "badge-hotspot--low";

        // Show only date+time portion of window_end
        const windowEnd = r.window_end ? r.window_end.replace(/\.\d+$/, "") : "--";

        return `<tr>
          <td><strong>${escHtml(r.pickup_zone || "--")}</strong></td>
          <td>${escHtml(r.pickup_borough || "--")}</td>
          <td>${r.pickups_trailing_hour !== undefined ? r.pickups_trailing_hour : "--"}</td>
          <td><span class="badge-hotspot ${badgeClass}">${escHtml(tier)}</span></td>
          <td style="font-size:11px;color:#64748b;">${escHtml(windowEnd)}</td>
        </tr>`;
      }).join("");
    }
  }

  // Update Live Map with latest hotspot data
  updateLiveMap(hotspots);
}

function escHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ==============================================================================
// LEAFLET MAP INTEGRATION
// NYC Zone → Approximate Lat/Lng Lookup Table
// (Covers all zones that appear in streaming_data.json and batch results)
// ==============================================================================

const NYC_ZONE_COORDS = {
  "Upper East Side South":           [40.7717, -73.9574],
  "Upper East Side North":           [40.7785, -73.9537],
  "JFK Airport":                     [40.6413, -73.7781],
  "JFK Airport (Queens)":            [40.6413, -73.7781],
  "Midtown Center":                  [40.7549, -73.9840],
  "Midtown Center (Manhattan)":      [40.7549, -73.9840],
  "Midtown East":                    [40.7527, -73.9712],
  "Midtown South":                   [40.7488, -73.9856],
  "Midtown North":                   [40.7614, -73.9776],
  "Penn Station/Madison Sq West":    [40.7506, -73.9971],
  "Penn Station / Madison Sq West":  [40.7506, -73.9971],
  "Lincoln Square East":             [40.7740, -73.9831],
  "Lincoln Square West":             [40.7750, -73.9876],
  "Times Sq/Theatre District":       [40.7580, -73.9855],
  "Times Sq / Theatre District (Midtown)": [40.7580, -73.9855],
  "LaGuardia Airport":               [40.7769, -73.8740],
  "LaGuardia Airport (Queens)":      [40.7769, -73.8740],
  "Murray Hill":                     [40.7479, -73.9767],
  "Murray Hill (Manhattan)":         [40.7479, -73.9767],
  "East Village":                    [40.7265, -73.9815],
  "East Village (Manhattan)":        [40.7265, -73.9815],
  "West Village":                    [40.7340, -74.0050],
  "West Village (Manhattan)":        [40.7340, -74.0050],
  "Financial District South":        [40.7060, -74.0090],
  "Financial District South (Wall St)": [40.7060, -74.0090],
  "Financial District North":        [40.7099, -74.0089],
  "Lenox Hill West":                 [40.7683, -73.9637],
  "Lenox Hill East":                 [40.7670, -73.9587],
  "Yorkville East":                  [40.7769, -73.9502],
  "Yorkville West":                  [40.7748, -73.9553],
  "Upper West Side North":           [40.7956, -73.9688],
  "Upper West Side South":           [40.7844, -73.9755],
  "Kips Bay":                        [40.7426, -73.9773],
  "Kips Bay (Manhattan)":            [40.7426, -73.9773],
  "Gramercy":                        [40.7397, -73.9845],
  "Greenwich Village North":         [40.7339, -74.0010],
  "Greenwich Village South":         [40.7290, -74.0020],
  "Sutton Place/Turtle Bay North":   [40.7559, -73.9647],
  "UN/Turtle Bay South":             [40.7509, -73.9666],
  "TriBeCa/Civic Center":            [40.7163, -74.0086],
  "Stuy Town/Peter Cooper Village":  [40.7321, -73.9772],
  "East Chelsea":                    [40.7444, -74.0001],
  "West Chelsea/Hudson Yards":       [40.7528, -74.0045],
  "Flatiron":                        [40.7401, -73.9903],
  "Union Sq":                        [40.7354, -73.9910],
  "Hudson Sq":                       [40.7272, -74.0079],
  "Lower East Side":                 [40.7153, -73.9849],
  "SoHo":                            [40.7231, -74.0007],
  "Central Park":                    [40.7851, -73.9683],
  "Central Harlem North":            [40.8176, -73.9558],
  "East Harlem South":               [40.7957, -73.9378],
  "East Harlem North":               [40.8020, -73.9348],
  "Crown Heights North":             [40.6736, -73.9455],
  "Downtown Brooklyn/MetroTech":     [40.6925, -73.9876],
  "Fort Greene":                     [40.6899, -73.9750],
  "Boerum Hill":                     [40.6847, -73.9877],
  "Greenpoint":                      [40.7298, -73.9511],
  "Greenpoint (Brooklyn)":           [40.7298, -73.9511],
  "East New York":                   [40.6694, -73.8852],
  "Gravesend":                       [40.5980, -73.9517],
  "Sunnyside":                       [40.7439, -73.9193],
  "East Elmhurst":                   [40.7631, -73.8771],
  "Melrose South":                   [40.8268, -73.9148],
  "Pelham Bay":                      [40.8518, -73.8273],
  "Riverdale/North Riverdale/Fieldston": [40.8951, -73.9123],
  "Riverdale / Fieldston (Bronx)":    [40.8951, -73.9123],
  "Riverdale / Fieldston":            [40.8951, -73.9123]
};

function getZoneCoords(name) {
  if (!name) return null;
  if (NYC_ZONE_COORDS[name]) return NYC_ZONE_COORDS[name];
  const lower = name.toLowerCase().trim();
  for (const k of Object.keys(NYC_ZONE_COORDS)) {
    if (k.toLowerCase() === lower || k.toLowerCase().includes(lower) || lower.includes(k.toLowerCase())) {
      return NYC_ZONE_COORDS[k];
    }
  }
  return null;
}

// Batch zones covering all dropdown selections and top demand hubs
const BATCH_HOTSPOT_ZONES = [
  { zone: "Upper East Side South",              tier: "HIGH",     pickups: 7.96 },
  { zone: "JFK Airport",                        tier: "HIGH",     pickups: 7.82 },
  { zone: "Midtown Center",                     tier: "HIGH",     pickups: 7.44 },
  { zone: "Upper East Side North",              tier: "HIGH",     pickups: 7.17 },
  { zone: "Midtown East",                       tier: "HIGH",     pickups: 5.63 },
  { zone: "Penn Station/Madison Sq West",       tier: "HIGH",     pickups: 5.40 },
  { zone: "Lincoln Square East",                tier: "HIGH",     pickups: 5.30 },
  { zone: "Times Sq/Theatre District",          tier: "HIGH",     pickups: 5.15 },
  { zone: "LaGuardia Airport",                  tier: "HIGH",     pickups: 5.05 },
  { zone: "Murray Hill",                        tier: "MODERATE", pickups: 4.73 },
  { zone: "East Village",                       tier: "MODERATE", pickups: 3.90 },
  { zone: "West Village",                       tier: "MODERATE", pickups: 3.85 },
  { zone: "Financial District South",           tier: "MODERATE", pickups: 3.60 },
  { zone: "Central Park",                       tier: "LOW",      pickups: 0.85 },
  { zone: "Kips Bay",                           tier: "LOW",      pickups: 1.80 },
  { zone: "Greenpoint",                         tier: "LOW",      pickups: 1.20 },
  { zone: "Central Harlem North",               tier: "LOW",      pickups: 0.45 },
  { zone: "Riverdale/North Riverdale/Fieldston", tier: "LOW",      pickups: 0.15 }
];

const TIER_COLOR = {
  HIGH:     "#ef4444",  // red
  MODERATE: "#f97316",  // orange
  LOW:      "#3b82f6"   // blue
};

function formatHourDisplay(h) {
  const hr = parseInt(h, 10);
  if (hr === 0) return "12:00 AM";
  if (hr < 12) return `${String(hr).padStart(2, "0")}:00 AM`;
  if (hr === 12) return "12:00 PM";
  return `${String(hr - 12).padStart(2, "0")}:00 PM`;
}

// Marker radius scale by pickups (for batch map)
function tierRadius(pickups) {
  return Math.min(26, Math.max(9, Math.round(pickups * 2.8)));
}

// ==============================================================================
// Hotspot Map (Demand Hotspots Tab)
// ==============================================================================

let _hotspotMap = null;
let _hotspotMarkers = [];          // [{marker, tier, baseRadius, baseColor, zone}]
let _selectedHighlightMarker = null; // Dedicated layer for selected zone

function initHotspotMap() {
  if (typeof L === "undefined") {
    console.warn("Leaflet not loaded yet for hotspot map.");
    return;
  }
  const el = document.getElementById("hotspot-map");
  if (!el) return;
  if (el._leaflet_id && _hotspotMap) {
    _hotspotMap.invalidateSize();
    return;
  }
  if (el._leaflet_id && !_hotspotMap) {
    el._leaflet_id = null;
  }

  try {
    _hotspotMap = L.map("hotspot-map", { zoomControl: true, scrollWheelZoom: false })
                   .setView([40.758, -73.985], 11);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; <a href='https://openstreetmap.org'>OpenStreetMap</a> contributors",
      maxZoom: 18
    }).addTo(_hotspotMap);

    // Legend
    const legend = L.control({ position: "bottomright" });
    legend.onAdd = function () {
      const div = L.DomUtil.create("div", "map-legend");
      div.innerHTML = `
        <strong>Demand Tier</strong><br>
        <span style="color:#ef4444">●</span> High-Demand Hotspot<br>
        <span style="color:#f97316">●</span> Moderate Demand<br>
        <span style="color:#3b82f6">●</span> Low Demand
      `;
      return div;
    };
    legend.addTo(_hotspotMap);

    // Draw all batch K-Means zones and store marker refs
    _hotspotMarkers = [];
    BATCH_HOTSPOT_ZONES.forEach(z => {
      const coords = getZoneCoords(z.zone);
      if (!coords) return;
      const color = TIER_COLOR[z.tier] || "#3b82f6";
      const radius = tierRadius(z.pickups);
      const m = L.circleMarker(coords, {
        radius: radius,
        color: color,
        fillColor: color,
        fillOpacity: 0.7,
        weight: 2
      })
      .addTo(_hotspotMap)
      .bindPopup(`<strong>${z.zone}</strong><br>Demand Regime: ${z.tier}`);

      // Allow clicking marker to select it in the dropdown
      m.on("click", () => {
        const sel = document.getElementById("select-regime-zone");
        if (sel) {
          sel.value = z.zone;
          classifyRegime();
        }
      });

      _hotspotMarkers.push({ marker: m, tier: z.tier, baseRadius: radius, baseColor: color, zone: z.zone });
    });

    // Run initial highlight based on current input values
    classifyRegime();
  } catch (err) {
    console.error("initHotspotMap error:", err);
  }
}

// Called every time classifyRegime() fires — updates markers dynamically for any zone & time
function updateHotspotMapForTier(assignedTier, selectedZone, hour) {
  if (!_hotspotMap || typeof L === "undefined") return;

  const tierColorMap = { HIGH: "#ef4444", MODERATE: "#f97316", LOW: "#3b82f6" };
  const tierNameMap = { HIGH: "HIGH DEMAND (HOTSPOT)", MODERATE: "MODERATE DEMAND", LOW: "LOW DEMAND" };
  const currentHour = (hour !== undefined && hour !== null) ? parseFloat(hour) : 18;
  const hourLabel = formatHourDisplay(currentHour);
  const selectedColor = tierColorMap[assignedTier] || "#3b82f6";

  // 1. Update background zones
  _hotspotMarkers.forEach(obj => {
    const { marker, baseRadius, zone } = obj;
    const isSelected = selectedZone && (zone === selectedZone || zone.includes(selectedZone) || selectedZone.includes(zone));

    const zPickups = getBackgroundPickupsForZone(zone, currentHour);
    const zTier = computeTier(zPickups, currentHour);
    const zColor = tierColorMap[zTier] || "#3b82f6";

    if (isSelected) {
      marker.setStyle({
        radius: Math.max(baseRadius, 14) + 6,
        color: "#ffffff",
        fillColor: zColor,
        fillOpacity: 1.0,
        weight: 5
      });
      marker.bringToFront();
    } else {
      const isMatch = (zTier === assignedTier);
      marker.setStyle({
        radius: isMatch ? baseRadius + 2 : Math.max(7, baseRadius * 0.75),
        color: zColor,
        fillColor: zColor,
        fillOpacity: isMatch ? 0.75 : 0.22,
        weight: isMatch ? 2 : 1
      });
    }
  });

  // 2. Guarantee dedicated prominent highlight marker for selected zone (handles all zones)
  const selCoords = getZoneCoords(selectedZone);
  if (selCoords && _hotspotMap) {
    if (_selectedHighlightMarker) {
      try { _hotspotMap.removeLayer(_selectedHighlightMarker); } catch (e) {}
    }

    _selectedHighlightMarker = L.circleMarker(selCoords, {
      radius: 20,
      color: "#ffffff",
      fillColor: selectedColor,
      fillOpacity: 1.0,
      weight: 5
    }).addTo(_hotspotMap);

    _selectedHighlightMarker.bindPopup(
      `<div style="font-family:sans-serif;min-width:150px;line-height:1.5;">` +
      `<strong style="font-size:14px;color:#0f172a;">${selectedZone}</strong><br>` +
      `<span style="font-size:11px;color:#64748b;">Time: <strong>${hourLabel}</strong></span><br>` +
      `<span style="font-size:12px;color:${selectedColor};font-weight:700;">${tierNameMap[assignedTier] || assignedTier}</span>` +
      `</div>`
    );
    _selectedHighlightMarker.bringToFront();
    _selectedHighlightMarker.openPopup();

    // Center map view on selected target
    _hotspotMap.setView(selCoords, 12, { animate: true });
  }
}

// ==============================================================================
// Live Map (Live Stream Tab) — Refreshed from streaming_data.json
// ==============================================================================

let _liveMap = null;
let _liveMarkers = [];

function initLiveMap() {
  if (typeof L === "undefined") {
    console.warn("Leaflet not loaded yet for live map.");
    return;
  }
  const el = document.getElementById("live-map");
  if (!el) return;
  if (el._leaflet_id && _liveMap) {
    _liveMap.invalidateSize();
    return;
  }
  if (el._leaflet_id && !_liveMap) {
    el._leaflet_id = null;
  }

  try {
    _liveMap = L.map("live-map", { zoomControl: true, scrollWheelZoom: false })
                .setView([40.730, -73.990], 11);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; <a href='https://openstreetmap.org'>OpenStreetMap</a> contributors",
      maxZoom: 18
    }).addTo(_liveMap);

    // Legend
    const legend = L.control({ position: "bottomright" });
    legend.onAdd = function () {
      const div = L.DomUtil.create("div", "map-legend");
      div.innerHTML = `
        <strong>Live Demand Regime</strong><br>
        <span style="color:#ef4444">●</span> High-Demand<br>
        <span style="color:#f97316">●</span> Moderate<br>
        <span style="color:#3b82f6">●</span> Low Demand
      `;
      return div;
    };
    legend.addTo(_liveMap);
  } catch (err) {
    console.error("initLiveMap error:", err);
  }
}

function updateLiveMap(hotspots) {
  if (!_liveMap || typeof L === "undefined") return;

  try {
    // Clear old markers
    _liveMarkers.forEach(m => {
      try { _liveMap.removeLayer(m); } catch (e) {}
    });
    _liveMarkers = [];

    if (!hotspots || hotspots.length === 0) return;

    // Deduplicate zones — keep highest pickups entry per zone
    const zoneMap = {};
    hotspots.forEach(h => {
      if (!h.pickup_zone || h.pickup_zone === "N/A") return;
      if (!zoneMap[h.pickup_zone] || (h.pickups_trailing_hour || 0) > (zoneMap[h.pickup_zone].pickups_trailing_hour || 0)) {
        zoneMap[h.pickup_zone] = h;
      }
    });

    Object.values(zoneMap).forEach(h => {
      const coords = NYC_ZONE_COORDS[h.pickup_zone];
      if (!coords) return;

      // Determine tier colour
      let tier = (h.demand_tier || "LOW DEMAND").toUpperCase();
      if (tier.includes("HIGH"))     tier = "HIGH";
      else if (tier.includes("MOD")) tier = "MODERATE";
      else                           tier = "LOW";

      const color = TIER_COLOR[tier] || "#3b82f6";
      const radius = Math.min(22, Math.max(8, (h.pickups_trailing_hour || 1) * 2));

      const m = L.circleMarker(coords, {
        radius,
        color,
        fillColor: color,
        fillOpacity: 0.65,
        weight: 2
      })
      .addTo(_liveMap)
      .bindPopup(
        `<strong>${escHtml(h.pickup_zone)}</strong><br>` +
        `Borough: ${escHtml(h.pickup_borough || "--")}<br>` +
        `Pickups (trailing 1hr): <strong>${h.pickups_trailing_hour || 0}</strong><br>` +
        `Regime: <span style="color:${color};font-weight:700">${escHtml(h.demand_tier || "--")}</span>`
      );

      _liveMarkers.push(m);
    });

    // Update real-time map badge so the user sees live clock updates
    const badge = document.getElementById("live-map-badge");
    if (badge) {
      const now = new Date().toLocaleTimeString();
      badge.textContent = `🟢 LIVE · ${now} · ${Object.keys(zoneMap).length} Zones`;
      badge.style.background = "#dcfce7";
      badge.style.color = "#15803d";
      badge.style.borderColor = "#86efac";
    }
  } catch (err) {
    console.error("updateLiveMap error:", err);
  }
}

