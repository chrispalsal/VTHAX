(() => {
  "use strict";

  const DEFAULT_INTERVAL_MS = 60_000;
  const state = {
    active: false,
    timer: null,
    latestPosition: null,
    locationError: null,
    uploads: 0,
    sessionId: null,
    collecting: false,
    controller: null,
  };

  const elements = {
    consentCard: document.querySelector("#consent-card"),
    monitorCard: document.querySelector("#monitor-card"),
    locationConsent: document.querySelector("#location-consent"),
    connectivityConsent: document.querySelector("#connectivity-consent"),
    participantId: document.querySelector("#participant-id"),
    collectorUrl: document.querySelector("#collector-url"),
    accessCode: document.querySelector("#access-code"),
    startButton: document.querySelector("#start-button"),
    stopButton: document.querySelector("#stop-button"),
    locationStatus: document.querySelector("#location-status"),
    locationDetail: document.querySelector("#location-detail"),
    connectionStatus: document.querySelector("#connection-status"),
    connectionDetail: document.querySelector("#connection-detail"),
    uploadCount: document.querySelector("#upload-count"),
    uploadDetail: document.querySelector("#upload-detail"),
    activityDot: document.querySelector("#activity-dot"),
    activityMessage: document.querySelector("#activity-message"),
  };

  function setActivity(message, kind = "pending") {
    elements.activityMessage.textContent = message;
    elements.activityDot.className = `activity-dot ${kind === "pending" ? "" : kind}`;
  }

  function updateStartState() {
    elements.startButton.disabled = !(
      elements.locationConsent.checked || elements.connectivityConsent.checked
    );
  }

  function collectorEndpoint() {
    const custom = elements.collectorUrl.value.trim();
    return `${custom ? custom.replace(/\/$/, "") : window.location.origin}/telemetry`;
  }

  function readConnection() {
    const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    if (!connection) {
      elements.connectionStatus.textContent = navigator.onLine ? "Online" : "Offline";
      elements.connectionDetail.textContent = "Detailed estimates unavailable in this browser";
      return { online: navigator.onLine, supported: false };
    }

    const label = connection.effectiveType ? connection.effectiveType.toUpperCase() : "Available";
    const detailParts = [];
    if (Number.isFinite(connection.downlink)) detailParts.push(`${connection.downlink} Mb/s estimate`);
    if (Number.isFinite(connection.rtt)) detailParts.push(`${connection.rtt} ms RTT estimate`);
    elements.connectionStatus.textContent = label;
    elements.connectionDetail.textContent = detailParts.join(" · ") || "Connection information available";
    return {
      online: navigator.onLine,
      supported: true,
      type: connection.type || null,
      effective_type: connection.effectiveType || null,
      downlink_mbps: Number.isFinite(connection.downlink) ? connection.downlink : null,
      rtt_ms: Number.isFinite(connection.rtt) ? connection.rtt : null,
      save_data: Boolean(connection.saveData),
    };
  }

  function requestLocation() {
    if (!elements.locationConsent.checked || !navigator.geolocation) {
      if (elements.locationConsent.checked) {
        state.locationError = "Geolocation is unavailable in this browser.";
        elements.locationStatus.textContent = "Unavailable";
        elements.locationDetail.textContent = state.locationError;
      } else {
        elements.locationStatus.textContent = "Not shared";
        elements.locationDetail.textContent = "Location permission was not selected";
      }
      return Promise.resolve();
    }

    return new Promise((resolve) => {
      navigator.geolocation.getCurrentPosition(
        (position) => {
          if (state.active) {
            state.latestPosition = position;
            state.locationError = null;
            elements.locationStatus.textContent = "Available";
            elements.locationDetail.textContent = `Accuracy about ${Math.round(position.coords.accuracy)} m`;
          }
          resolve();
        },
        (error) => {
          if (state.active) {
            state.locationError = error.message || "Location permission was not granted.";
            elements.locationStatus.textContent = "Not available";
            elements.locationDetail.textContent = state.locationError;
          }
          resolve();
        },
        { enableHighAccuracy: false, maximumAge: 60_000, timeout: 15_000 },
      );
    });
  }

  function locationPayload() {
    if (!elements.locationConsent.checked) return null;
    if (!state.latestPosition) return { available: false, error: state.locationError };
    const { coords, timestamp } = state.latestPosition;
    return {
      available: true,
      latitude: coords.latitude,
      longitude: coords.longitude,
      accuracy_m: coords.accuracy,
      altitude_m: coords.altitude,
      altitude_accuracy_m: coords.altitudeAccuracy,
      heading_degrees: coords.heading,
      speed_mps: coords.speed,
      device_timestamp: new Date(timestamp).toISOString(),
    };
  }

  async function measureReachability(signal) {
    const started = performance.now();
    try {
      const base = elements.collectorUrl.value.trim().replace(/\/$/, "") || window.location.origin;
      const response = await fetch(`${base}/health?sample=${Date.now()}`, {
        cache: "no-store",
        signal,
      });
      return {
        reachable: response.ok,
        latency_ms: Math.round(performance.now() - started),
      };
    } catch {
      return { reachable: false, latency_ms: null };
    }
  }

  async function collectAndUpload() {
    if (!state.active || state.collecting) return;
    state.collecting = true;
    const controller = new AbortController();
    state.controller = controller;
    setActivity("Collecting a sample…");
    if (elements.locationConsent.checked) await requestLocation();
    if (!state.active) {
      state.collecting = false;
      return;
    }
    const connectivity = elements.connectivityConsent.checked ? readConnection() : null;
    if (connectivity) connectivity.collector_check = await measureReachability(controller.signal);
    if (!state.active) {
      state.collecting = false;
      return;
    }

    const payload = {
      session_id: state.sessionId,
      participant_id: elements.participantId.value.trim() || null,
      measured_at: new Date().toISOString(),
      consent: {
        location: elements.locationConsent.checked,
        connectivity: elements.connectivityConsent.checked,
      },
      location: locationPayload(),
      connectivity,
    };

    const headers = { "Content-Type": "application/json" };
    const accessCode = elements.accessCode.value;
    if (accessCode) headers.Authorization = `Bearer ${accessCode}`;

    try {
      const response = await fetch(collectorEndpoint(), {
        method: "POST",
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(`Collector returned ${response.status}`);
      state.uploads += 1;
      elements.uploadCount.textContent = String(state.uploads);
      elements.uploadDetail.textContent = `Last sent ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
      setActivity("Sample received by the collector.", "success");
    } catch (error) {
      if (state.active) {
        setActivity(`Could not send this sample: ${error.message}`, "error");
        elements.uploadDetail.textContent = "Will try again at the next interval";
      }
    } finally {
      state.collecting = false;
      if (state.controller === controller) state.controller = null;
    }
  }

  function startCollection() {
    if (state.active || elements.startButton.disabled) return;
    state.active = true;
    state.sessionId = crypto.randomUUID ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
    state.uploads = 0;
    elements.consentCard.hidden = true;
    elements.monitorCard.hidden = false;
    if (!elements.connectivityConsent.checked) {
      elements.connectionStatus.textContent = "Not shared";
      elements.connectionDetail.textContent = "Connectivity permission was not selected";
    }
    collectAndUpload();
    state.timer = window.setInterval(collectAndUpload, DEFAULT_INTERVAL_MS);
  }

  function stopCollection() {
    state.active = false;
    if (state.controller) state.controller.abort();
    window.clearInterval(state.timer);
    state.timer = null;
    state.latestPosition = null;
    state.locationError = null;
    state.collecting = false;
    elements.monitorCard.hidden = true;
    elements.consentCard.hidden = false;
    setActivity("Collection stopped.");
    elements.startButton.focus();
  }

  function registerCollectionStatusTool() {
    const context = document.modelContext;
    if (!context?.registerTool) return;
    try {
      void Promise.resolve(context.registerTool({
        name: "read_collection_status",
        title: "Read collection status",
        description: "Read whether this page is collecting diagnostic data and which permissions the participant selected. This tool cannot grant consent or start collection.",
        inputSchema: {
          type: "object",
          properties: {},
          additionalProperties: false,
        },
        annotations: { readOnlyHint: true, untrustedContentHint: false },
        execute() {
          return {
            active: state.active,
            selected_permissions: {
              location: elements.locationConsent.checked,
              connectivity: elements.connectivityConsent.checked,
            },
            uploaded_samples: state.uploads,
            status_message: elements.activityMessage.textContent,
          };
        },
      })).catch(() => {});
    } catch {
      // WebMCP is optional and unsupported browsers use the visible controls.
    }
  }

  elements.locationConsent.addEventListener("change", updateStartState);
  elements.connectivityConsent.addEventListener("change", updateStartState);
  elements.startButton.addEventListener("click", startCollection);
  elements.stopButton.addEventListener("click", stopCollection);
  window.addEventListener("online", () => state.active && readConnection());
  window.addEventListener("offline", () => state.active && readConnection());
  registerCollectionStatusTool();
})();
