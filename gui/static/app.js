/* AI Essentials demo GUI client (SSE-based) */
(function () {
  const $ = (id) => document.getElementById(id);
  const logEl = $("log");
  let count = 0;
  let producerOn = false;

  // ---- helpers ----
  function setStatus() {
    $("st-producer").textContent = producerOn ? "ON" : "off";
    $("st-producer").style.color = producerOn ? "#8AD4C0" : "inherit";
    $("btn-stop").disabled = !producerOn;
    $("btn-fault").disabled = !producerOn;
    $("btn-clear").disabled = !producerOn;
    $("btn-start").disabled = producerOn;
  }

  function fmtTime(iso) {
    if (!iso) return "--";
    const d = new Date(iso);
    return d.toLocaleTimeString("en-GB", { hour12: false }) + "." +
      String(d.getMilliseconds()).padStart(3, "0");
  }

  function addLog(payload) {
    count++;
    const lvl = payload.alert_level || "INFO";
    const row = document.createElement("div");
    row.className = "row";

    const ts = document.createElement("span"); ts.className = "ts";
    ts.textContent = fmtTime(payload.timestamp);
    const lv = document.createElement("span"); lv.className = "lvl " + lvl;
    lv.textContent = lvl;

    // body (parsed / pretty view)
    const body = document.createElement("span"); body.className = "body";
    const isAlert = payload.is_alert || payload.fault;
    body.textContent =
      `${payload.entity} ${payload.source} p95=${payload.value}ms ` +
      `thr=${payload.threshold} ` +
      (isAlert ? `+${payload.alert_count_boost || 0} alerts` : "");
    if (isAlert) {
      const b = document.createElement("span"); b.className = "fault-badge";
      b.textContent = "ALERT";
      body.appendChild(b);
    }

    // JSON toggle button
    const btn = document.createElement("button");
    btn.className = "json-btn";
    btn.textContent = "JSON";
    btn.onclick = () => {
      if (row.querySelector(".json-detail")) {
        row.querySelector(".json-detail").remove();
        btn.textContent = "JSON";
      } else {
        const pre = document.createElement("pre");
        pre.className = "json-detail";
        pre.textContent = JSON.stringify(payload, null, 2);
        row.appendChild(pre);
        btn.textContent = "hide";
      }
    };

    row.append(ts, lv, body, btn);
    logEl.appendChild(row);

    $("count-label").textContent = `(${count} events)`;

    // cap DOM
    while (logEl.children.length > 500) logEl.removeChild(logEl.firstChild);

    if ($("cb-autoscroll").checked) logEl.scrollTop = logEl.scrollHeight;
  }

  async function api(path, method) {
    const r = await fetch(path, { method: method || "POST" });
    return r.json();
  }

  // ---- SSE live stream ----
  const es = new EventSource("/stream");
  es.addEventListener("log", (e) => addLog(JSON.parse(e.data)));
  es.addEventListener("phase", (e) => {
    const p = JSON.parse(e.data);
    const el = $("st-phase");
    el.textContent = p.phase;
    el.className = p.phase === "fault" ? "flt" : "nrm";
  });
  es.onerror = () => { /* EventSource auto-reconnects */ };

  // Spark driver log tail
  es.addEventListener("sparklog", (e) => {
    const sparkEl = $("sparklog");
    const line = JSON.parse(e.data).line || "";
    const row = document.createElement("div");
    row.className = "row sparkrow";
    row.textContent = line;
    sparkEl.appendChild(row);
    while (sparkEl.children.length > 300) sparkEl.removeChild(sparkEl.firstChild);
    if ($("cb-autoscroll").checked) sparkEl.scrollTop = sparkEl.scrollHeight;
  });

  // Langflow agent log tail — timestamp + output message per transaction.
  es.addEventListener("langflowlog", (e) => {
    const lfEl = $("langflowlog");
    const d = JSON.parse(e.data);
    const row = document.createElement("div");
    row.className = "row langflowrow";

    const ts = document.createElement("span"); ts.className = "ts";
    ts.textContent = fmtTime(d.timestamp);

    const body = document.createElement("span"); body.className = "body langflowbody";
    body.textContent = d.text || "";

    row.append(ts, body);
    lfEl.appendChild(row);
    while (lfEl.children.length > 300) lfEl.removeChild(lfEl.firstChild);
    if ($("cb-autoscroll").checked) lfEl.scrollTop = lfEl.scrollHeight;
  });

  // ---- initial status ----
  fetch("/api/status").then(r => r.json()).then(st => {
    producerOn = st.producer_running;
    setStatus();
    $("st-sent").textContent = st.producer ? st.producer.sent : 0;
    $("st-recv").textContent = st.consumer ? st.consumer.received : 0;
  });

  // ---- controls ----
  $("btn-start").onclick = async () => {
    await api("/api/start");
    producerOn = true; setStatus();
    const s = await (await fetch("/api/status")).json();
    $("st-sent").textContent = s.producer ? s.producer.sent : 0;
  };
  $("btn-stop").onclick = async () => { await api("/api/stop"); producerOn = false; setStatus(); };
  $("btn-fault").onclick = async () => { await api("/api/fault"); };
  $("btn-clear").onclick = async () => { await api("/api/clear-fault"); };

  // Reset Topic: recreate rundmc at the MapR level to drop stale data.
  // Destructive, so confirm first and surface the result.
  $("btn-reset-topic").onclick = async () => {
    if (!confirm("Recreate the rundmc topic at the MapR level? This clears all "
                 + "current topic data (stale messages) before you start.")) return;
    const btn = $("btn-reset-topic");
    btn.disabled = true; btn.textContent = "Resetting…";
    try {
      const r = await api("/api/reset-topic");
      alert(r.message + (r.ok ? ` (${r.topic}, ${r.partitions} partition)` : ""));
      console.log("reset-topic result:", r);
    } catch (e) {
      alert("Reset Topic failed unexpectedly: " + e);
    } finally {
      btn.disabled = false; btn.textContent = "Reset Topic";
    }
  };
  $("btn-clear-log").onclick = () => {
    logEl.innerHTML = ""; count = 0; $("count-label").textContent = "";
  };

  // periodic counter refresh
  setInterval(async () => {
    try {
      const s = await (await fetch("/api/status")).json();
      $("st-sent").textContent = s.producer ? s.producer.sent : 0;
      $("st-recv").textContent = s.consumer ? s.consumer.received : 0;
    } catch (e) { /* ignore */ }
  }, 1000);

  // ---- settings modal (user-configurable overrides for the three windows) ----
  const settingsModal = $("settings-modal");
  const settingsMsg = $("settings-msg");
  const FIELD_IDS = [
    "kafka_topic", "spark_pod", "spark_ns",
    "langflow_base", "langflow_flow_id", "langflow_api_key",
  ];

  function openSettings() {
    settingsModal.classList.remove("hidden");
    settingsMsg.textContent = "";
    // prefill from server (current effective values)
    fetch("/api/settings").then(r => r.json()).then(d => {
      const s = d.settings || {};
      FIELD_IDS.forEach(f => { $("set-" + f).value = s[f] || ""; });
    }).catch(() => {
      settingsMsg.textContent = "Could not load current settings.";
    });
  }

  function closeSettings() {
    settingsModal.classList.add("hidden");
  }

  $("btn-settings").onclick = openSettings;
  $("btn-settings-close").onclick = closeSettings;
  settingsModal.addEventListener("click", (e) => {
    if (e.target === settingsModal) closeSettings();
  });

  $("btn-settings-save").onclick = async () => {
    const overrides = {};
    FIELD_IDS.forEach(f => { overrides[f] = $("set-" + f).value.trim(); });
    const btn = $("btn-settings-save");
    btn.disabled = true;
    settingsMsg.textContent = "Saving…";
    try {
      const r = await fetch("/api/settings", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ overrides }),
      });
      const d = await r.json();
      if (d.ok) {
        settingsMsg.textContent = "Saved — applied to live windows.";
        setStatus();
      } else {
        settingsMsg.textContent = "Save failed.";
      }
    } catch (e) {
      settingsMsg.textContent = "Save error: " + e;
    } finally {
      btn.disabled = false;
    }
  };

  $("btn-settings-reset").onclick = async () => {
    if (!confirm("Reset all window settings back to defaults?")) return;
    const btn = $("btn-settings-reset");
    btn.disabled = true;
    settingsMsg.textContent = "Resetting…";
    try {
      const d = await (await fetch("/api/settings/reset", { method: "POST" })).json();
      if (d.ok) {
        FIELD_IDS.forEach(f => { $("set-" + f).value = d.settings[f] || ""; });
        settingsMsg.textContent = "Reset to defaults.";
        setStatus();
      }
    } catch (e) {
      settingsMsg.textContent = "Reset error: " + e;
    } finally {
      btn.disabled = false;
    }
  };

  setStatus();
})();
