use axum::response::Html;

pub async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Mujina Dashboard</title>
    <style>
        :root {
            --bg: #120f0c;
            --panel: rgba(29, 23, 18, 0.9);
            --panel-strong: rgba(39, 30, 23, 0.98);
            --border: rgba(255, 177, 92, 0.18);
            --text: #f2e7d9;
            --muted: #c0ad98;
            --accent: #ffb15c;
            --accent-strong: #ff8844;
            --good: #72d49a;
            --warn: #ffd166;
            --bad: #ff6b6b;
            --shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
            --radius: 22px;
            --mono: "SFMono-Regular", "SF Mono", "IBM Plex Mono", ui-monospace, monospace;
            --sans: "Avenir Next", "Segoe UI", sans-serif;
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            min-height: 100vh;
            font-family: var(--sans);
            color: var(--text);
            background:
                radial-gradient(circle at top left, rgba(255, 136, 68, 0.18), transparent 28%),
                radial-gradient(circle at top right, rgba(255, 196, 92, 0.12), transparent 24%),
                linear-gradient(180deg, #18120f 0%, #0f0b09 100%);
        }

        body::before {
            content: "";
            position: fixed;
            inset: 0;
            background-image:
                linear-gradient(rgba(255, 255, 255, 0.02) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
            background-size: 28px 28px;
            pointer-events: none;
            opacity: 0.2;
        }

        main {
            width: min(1280px, calc(100vw - 32px));
            margin: 0 auto;
            padding: 32px 0 48px;
            position: relative;
        }

        .hero {
            display: grid;
            grid-template-columns: 1.4fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }

        .panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: var(--radius);
            box-shadow: var(--shadow);
            backdrop-filter: blur(14px);
        }

        .hero-card {
            padding: 28px;
            overflow: hidden;
            position: relative;
        }

        .hero-card::after {
            content: "";
            position: absolute;
            inset: auto -10% -40% auto;
            width: 260px;
            height: 260px;
            background: radial-gradient(circle, rgba(255, 136, 68, 0.2), transparent 70%);
            transform: rotate(18deg);
            pointer-events: none;
        }

        .eyebrow {
            font-size: 12px;
            letter-spacing: 0.22em;
            text-transform: uppercase;
            color: var(--accent);
            margin-bottom: 14px;
        }

        h1 {
            margin: 0;
            font-size: clamp(34px, 5vw, 60px);
            line-height: 0.95;
            max-width: 10ch;
        }

        .subtitle {
            margin: 16px 0 0;
            max-width: 56ch;
            color: var(--muted);
            line-height: 1.55;
        }

        .hero-side {
            padding: 28px;
            display: grid;
            gap: 18px;
            align-content: space-between;
        }

        .status-pill {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            width: fit-content;
            padding: 10px 14px;
            border-radius: 999px;
            background: rgba(255, 177, 92, 0.08);
            border: 1px solid rgba(255, 177, 92, 0.2);
            font-size: 13px;
            color: var(--muted);
        }

        .status-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: var(--warn);
            box-shadow: 0 0 16px currentColor;
        }

        .status-dot.live { color: var(--good); background: var(--good); }
        .status-dot.idle { color: var(--warn); background: var(--warn); }
        .status-dot.down { color: var(--bad); background: var(--bad); }

        .meta-grid,
        .summary-grid,
        .board-grid,
        .detail-grid {
            display: grid;
            gap: 16px;
        }

        .summary-grid {
            grid-template-columns: repeat(4, minmax(0, 1fr));
            margin-bottom: 20px;
        }

        .board-grid {
            grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        }

        .detail-grid {
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }

        .stat-card,
        .board-card,
        .table-card {
            padding: 22px;
        }

        .label {
            font-size: 12px;
            letter-spacing: 0.18em;
            text-transform: uppercase;
            color: var(--muted);
        }

        .value {
            margin-top: 10px;
            font-family: var(--mono);
            font-size: clamp(24px, 3.2vw, 42px);
            font-weight: 700;
            line-height: 1;
        }

        .subvalue {
            margin-top: 10px;
            color: var(--muted);
            font-size: 14px;
        }

        .board-head,
        .section-head {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 12px;
            margin-bottom: 18px;
        }

        .board-title,
        .section-title {
            margin: 0;
            font-size: 22px;
            line-height: 1.05;
        }

        .serial {
            color: var(--muted);
            font-family: var(--mono);
            font-size: 13px;
            word-break: break-all;
        }

        .chip {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            border-radius: 999px;
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.06);
            color: var(--muted);
            font-size: 12px;
        }

        .mini-list,
        .source-list {
            display: grid;
            gap: 10px;
        }

        .mini-row,
        .source-row {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 12px;
            padding: 12px 14px;
            border-radius: 16px;
            background: rgba(255,255,255,0.035);
            border: 1px solid rgba(255,255,255,0.05);
        }

        .mini-name,
        .source-name {
            font-size: 14px;
            color: var(--text);
        }

        .mini-meta,
        .source-meta {
            font-size: 12px;
            color: var(--muted);
            margin-top: 3px;
            word-break: break-word;
        }

        .mini-value,
        .source-value {
            align-self: center;
            font-family: var(--mono);
            font-size: 14px;
            color: var(--accent);
        }

        .footer-note {
            margin-top: 22px;
            color: var(--muted);
            font-size: 13px;
        }

        .empty {
            padding: 18px;
            border-radius: 16px;
            background: rgba(255,255,255,0.03);
            border: 1px dashed rgba(255,255,255,0.12);
            color: var(--muted);
        }

        .links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 18px;
        }

        .links a {
            color: var(--text);
            text-decoration: none;
            padding: 10px 14px;
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.1);
            background: rgba(255,255,255,0.04);
        }

        .links a:hover { border-color: var(--accent); }

        @media (max-width: 920px) {
            .hero,
            .summary-grid,
            .detail-grid {
                grid-template-columns: 1fr;
            }

            main {
                width: min(100vw - 20px, 1280px);
                padding-top: 18px;
            }

            .hero-card,
            .hero-side,
            .stat-card,
            .board-card,
            .table-card {
                padding: 18px;
            }
        }
    </style>
</head>
<body>
    <main>
        <section class="hero">
            <div class="panel hero-card">
                <div class="eyebrow">Mujina Live Monitor</div>
                <h1>Miner state, not API scaffolding.</h1>
                <p class="subtitle">
                    This dashboard reads the same live data exposed by the Mujina API and turns it into an operator view for hashrate, thermals, fans, power, and pool connectivity.
                </p>
                <div class="links">
                    <a href="/api/v0/miner" target="_blank" rel="noreferrer">Raw Miner JSON</a>
                    <a href="/swagger-ui/" target="_blank" rel="noreferrer">Swagger UI</a>
                </div>
            </div>

            <div class="panel hero-side">
                <div class="status-pill">
                    <span id="status-dot" class="status-dot idle"></span>
                    <span id="status-text">Waiting for first sample</span>
                </div>
                <div class="meta-grid">
                    <div>
                        <div class="label">Last Refresh</div>
                        <div class="subvalue" id="refresh-time">never</div>
                    </div>
                    <div>
                        <div class="label">API Poll</div>
                        <div class="subvalue">every 2 seconds</div>
                    </div>
                    <div>
                        <div class="label">View</div>
                        <div class="subvalue">single-page built-in dashboard</div>
                    </div>
                </div>
            </div>
        </section>

        <section class="summary-grid">
            <article class="panel stat-card">
                <div class="label">Hashrate</div>
                <div class="value" id="hashrate">--</div>
                <div class="subvalue" id="hashrate-detail">waiting for data</div>
            </article>
            <article class="panel stat-card">
                <div class="label">Shares Submitted</div>
                <div class="value" id="shares">--</div>
                <div class="subvalue" id="shares-detail">cumulative accepted submissions</div>
            </article>
            <article class="panel stat-card">
                <div class="label">Uptime</div>
                <div class="value" id="uptime">--</div>
                <div class="subvalue" id="uptime-detail">daemon runtime</div>
            </article>
            <article class="panel stat-card">
                <div class="label">Boards</div>
                <div class="value" id="board-count">--</div>
                <div class="subvalue" id="board-detail">connected board snapshots</div>
            </article>
        </section>

        <section class="detail-grid">
            <article class="panel table-card">
                <div class="section-head">
                    <div>
                        <div class="label">Pool Sources</div>
                        <h2 class="section-title">Connection state</h2>
                    </div>
                </div>
                <div id="sources" class="source-list"></div>
            </article>

            <article class="panel table-card">
                <div class="section-head">
                    <div>
                        <div class="label">Fleet Summary</div>
                        <h2 class="section-title">Thermals and airflow</h2>
                    </div>
                </div>
                <div id="fleet-summary" class="mini-list"></div>
            </article>
        </section>

        <section>
            <div class="section-head" style="margin: 22px 0 16px;">
                <div>
                    <div class="label">Boards</div>
                    <h2 class="section-title">Live hardware detail</h2>
                </div>
            </div>
            <div id="boards" class="board-grid"></div>
        </section>

        <p class="footer-note">Rendered by the miner itself. No external frontend build step, no second process, no browser-side secrets.</p>
    </main>

    <script>
        const POLL_MS = 2000;

        function formatHashrate(hashrate) {
            if (hashrate == null) return "--";
            const units = ["H/s", "kH/s", "MH/s", "GH/s", "TH/s", "PH/s"];
            let value = Number(hashrate);
            let unitIndex = 0;
            while (value >= 1000 && unitIndex < units.length - 1) {
                value /= 1000;
                unitIndex += 1;
            }
            const digits = value >= 100 ? 0 : value >= 10 ? 1 : 2;
            return `${value.toFixed(digits)} ${units[unitIndex]}`;
        }

        function formatUptime(totalSeconds) {
            if (totalSeconds == null) return "--";
            const seconds = Number(totalSeconds);
            const days = Math.floor(seconds / 86400);
            const hours = Math.floor((seconds % 86400) / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            if (days > 0) return `${days}d ${hours}h ${minutes}m`;
            if (hours > 0) return `${hours}h ${minutes}m`;
            return `${minutes}m`;
        }

        function formatTemp(value) {
            return value == null ? "--" : `${value.toFixed(1)} C`;
        }

        function formatVoltage(value) {
            return value == null ? "--" : `${value.toFixed(2)} V`;
        }

        function formatRpm(value) {
            return value == null ? "--" : `${Math.round(value).toLocaleString()} RPM`;
        }

        function statusClass(data) {
            if (!data) return "down";
            if (data.paused) return "idle";
            if ((data.boards || []).length === 0) return "idle";
            return "live";
        }

        function statusText(data) {
            if (!data) return "Dashboard offline";
            if (data.paused) return "Miner paused";
            if ((data.boards || []).length === 0) return "No board snapshots";
            return "Miner live";
        }

        function renderSources(sources) {
            const root = document.getElementById("sources");
            if (!sources || sources.length === 0) {
                root.innerHTML = '<div class="empty">No job sources are currently registered.</div>';
                return;
            }

            root.innerHTML = sources.map(source => {
                const meta = [source.url, source.difficulty != null ? `difficulty ${source.difficulty.toLocaleString()}` : null]
                    .filter(Boolean)
                    .join(" · ");
                return `
                    <div class="source-row">
                        <div>
                            <div class="source-name">${escapeHtml(source.name || "unnamed source")}</div>
                            <div class="source-meta">${escapeHtml(meta || "no connection metadata")}</div>
                        </div>
                        <div class="source-value">${source.difficulty != null ? source.difficulty.toLocaleString() : "connected"}</div>
                    </div>
                `;
            }).join("");
        }

        function renderFleetSummary(boards) {
            const root = document.getElementById("fleet-summary");
            if (!boards || boards.length === 0) {
                root.innerHTML = '<div class="empty">No board snapshots have been published yet.</div>';
                return;
            }

            const temps = boards.flatMap(board => (board.temperatures || []).map(sensor => sensor.temperature_c).filter(v => v != null));
            const rpms = boards.flatMap(board => (board.fans || []).map(fan => fan.rpm).filter(v => v != null));
            const volts = boards.flatMap(board => (board.powers || []).map(power => power.voltage_v).filter(v => v != null));

            const rows = [
                ["Hottest sensor", temps.length ? formatTemp(Math.max(...temps)) : "--", "highest reported board temperature"],
                ["Coolest sensor", temps.length ? formatTemp(Math.min(...temps)) : "--", "lowest reported board temperature"],
                ["Fastest fan", rpms.length ? formatRpm(Math.max(...rpms)) : "--", "peak measured tachometer speed"],
                ["PSU rail", volts.length ? formatVoltage(volts[volts.length - 1]) : "--", "latest reported power measurement"],
            ];

            root.innerHTML = rows.map(([name, value, meta]) => `
                <div class="mini-row">
                    <div>
                        <div class="mini-name">${escapeHtml(name)}</div>
                        <div class="mini-meta">${escapeHtml(meta)}</div>
                    </div>
                    <div class="mini-value">${escapeHtml(value)}</div>
                </div>
            `).join("");
        }

        function renderBoards(boards) {
            const root = document.getElementById("boards");
            if (!boards || boards.length === 0) {
                root.innerHTML = '<div class="panel board-card"><div class="empty">No boards are currently reporting state.</div></div>';
                return;
            }

            root.innerHTML = boards.map(board => {
                const temperatures = renderMiniList(
                    board.temperatures,
                    sensor => sensor.name || "temp",
                    sensor => formatTemp(sensor.temperature_c),
                    () => "temperature sensor"
                );

                const fans = renderMiniList(
                    board.fans,
                    fan => fan.name || "fan",
                    fan => formatRpm(fan.rpm),
                    fan => {
                        const target = fan.target_percent != null ? `${fan.target_percent}% target` : "no target";
                        const percent = fan.percent != null ? `${fan.percent}% measured` : null;
                        return [target, percent].filter(Boolean).join(" · ");
                    }
                );

                const power = renderMiniList(
                    board.powers,
                    reading => reading.name || "power",
                    reading => formatVoltage(reading.voltage_v),
                    reading => {
                        const bits = [];
                        if (reading.current_a != null) bits.push(`${reading.current_a.toFixed(2)} A`);
                        if (reading.power_w != null) bits.push(`${reading.power_w.toFixed(1)} W`);
                        return bits.join(" · ") || "voltage only";
                    }
                );

                const threads = renderMiniList(
                    board.threads,
                    thread => thread.name || "thread",
                    thread => formatHashrate(thread.hashrate) + " · 1m " + formatHashrate(thread.hashrate_1min),
                    thread => thread.is_active ? "active" : "idle"
                );

                return `
                    <article class="panel board-card">
                        <div class="board-head">
                            <div>
                                <div class="label">${escapeHtml(board.model || "Board")}</div>
                                <h3 class="board-title">${escapeHtml(board.name || "unnamed-board")}</h3>
                                <div class="serial">${escapeHtml(board.serial || "no serial reported")}</div>
                            </div>
                            <div class="chip">${(board.fans || []).length} fans · ${(board.temperatures || []).length} temps</div>
                        </div>
                        <div class="detail-grid">
                            <section>
                                <div class="label">Temperatures</div>
                                <div class="mini-list">${temperatures}</div>
                            </section>
                            <section>
                                <div class="label">Fans</div>
                                <div class="mini-list">${fans}</div>
                            </section>
                            <section>
                                <div class="label">Power</div>
                                <div class="mini-list">${power}</div>
                            </section>
                            <section>
                                <div class="label">Threads</div>
                                <div class="mini-list">${threads}</div>
                            </section>
                        </div>
                    </article>
                `;
            }).join("");
        }

        function renderMiniList(items, getName, getValue, getMeta) {
            if (!items || items.length === 0) {
                return '<div class="empty">No live values reported.</div>';
            }

            return items.map(item => `
                <div class="mini-row">
                    <div>
                        <div class="mini-name">${escapeHtml(getName(item))}</div>
                        <div class="mini-meta">${escapeHtml(getMeta(item) || "")}</div>
                    </div>
                    <div class="mini-value">${escapeHtml(getValue(item))}</div>
                </div>
            `).join("");
        }

        function escapeHtml(value) {
            return String(value)
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;")
                .replaceAll("'", "&#39;");
        }

        function applySnapshot(data) {
            document.getElementById("hashrate").textContent = formatHashrate(data.hashrate);
            const hr1min = (data.boards || []).reduce(
                (sum, b) => sum + (b.threads || []).reduce((t, th) => t + (th.hashrate_1min || 0), 0), 0);
            document.getElementById("hashrate-detail").textContent =
                data.paused ? "mining paused" : "1-min: " + formatHashrate(hr1min);
            document.getElementById("shares").textContent = Number(data.shares_submitted || 0).toLocaleString();
            document.getElementById("uptime").textContent = formatUptime(data.uptime_secs);
            document.getElementById("board-count").textContent = Number((data.boards || []).length).toString();
            document.getElementById("refresh-time").textContent = new Date().toLocaleTimeString();

            const dot = document.getElementById("status-dot");
            dot.className = `status-dot ${statusClass(data)}`;
            document.getElementById("status-text").textContent = statusText(data);

            renderSources(data.sources || []);
            renderFleetSummary(data.boards || []);
            renderBoards(data.boards || []);
        }

        async function refresh() {
            try {
                const response = await fetch("/api/v0/miner", { cache: "no-store" });
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}`);
                }
                const data = await response.json();
                applySnapshot(data);
            } catch (error) {
                document.getElementById("status-dot").className = "status-dot down";
                document.getElementById("status-text").textContent = `Dashboard error: ${error.message}`;
                document.getElementById("refresh-time").textContent = new Date().toLocaleTimeString();
            }
        }

        refresh();
        setInterval(refresh, POLL_MS);
    </script>
</body>
</html>
"#;
