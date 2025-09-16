import os
import json
import time
import argparse
import re
from html import unescape
from datetime import datetime, timezone
from threading import Thread, Lock

import requests
from flask import Flask, jsonify, render_template_string
from git import Repo, Actor

# --- Configuration ---
API_KEY = os.getenv("IDFM_API_KEY", "oyTLFQfbTUuF4smBI4bHq9v07r42EPrI")
API_URL_BASE = os.getenv(
    "IDFM_API_URL",
    "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2",
)


def _build_api_url_candidates(base: str):
    candidates = []
    base = (base or "").strip()
    if base:
        candidates.append(base)
        if not base.endswith('/'):
            candidates.append(base + '/')
    if 'disruptions_bulk' in base:
        alt = base.replace('disruptions_bulk', 'disruptions-bulk')
        candidates.append(alt)
        if not alt.endswith('/'):
            candidates.append(alt + '/')
    if 'disruptions-bulk' in base:
        alt = base.replace('disruptions-bulk', 'disruptions_bulk')
        candidates.append(alt)
        if not alt.endswith('/'):
            candidates.append(alt + '/')
    # Variantes v1/v2 sous-chemins
    if '/disruptions/v2' in base:
        candidates.append(base.replace('/disruptions/v2', '/disruptions/v1'))
    if '/disruptions/v1' in base:
        candidates.append(base.replace('/disruptions/v1', '/disruptions/v2'))
    candidates.append("https://prim.iledefrance-mobilites.fr/marketplace/general-message")
    seen = set()
    result = []
    for u in candidates:
        if u not in seen:
            result.append(u)
            seen.add(u)
    return result


API_URL_CANDIDATES = _build_api_url_candidates(API_URL_BASE)
CACHE_DURATION = 15  # seconds
REPO_PATH = "."
HISTORY_PATH = os.path.join(REPO_PATH, "data", "history.json")

# --- Globals ---
app = Flask(__name__)
data_cache = {"timestamp": 0, "data": None}
cache_lock = Lock()

# Diagnostics
api_diag = {"last_url": None, "last_method": None, "last_headers": None, "last_status": None, "last_error": None, "ok": False}


def get_api_data():
    """Fetch PRIM data trying several URL/header/method variants."""
    header_variants = [
        {"apikey": API_KEY, "Accept": "application/json"},
        {"apiKey": API_KEY, "Accept": "application/json"},
        {"X-API-KEY": API_KEY, "Accept": "application/json"},
        {"Authorization": f"Apikey {API_KEY}", "Accept": "application/json"},
    ]
    api_diag.update({"last_url": None, "last_method": None, "last_headers": None, "last_status": None, "last_error": None, "ok": False})
    last_error = None
    methods = ("GET", "POST")
    for url in API_URL_CANDIDATES:
        qs_variants = [
            url,
            (url + ("&" if "?" in url else "?") + f"apiKey={API_KEY}") if API_KEY else url,
            (url + ("&" if "?" in url else "?") + f"apikey={API_KEY}") if API_KEY else url,
            (url + ("&" if "?" in url else "?") + f"key={API_KEY}") if API_KEY else url,
        ]
        for target_url in qs_variants:
            for headers in header_variants:
                for method in methods:
                    try:
                        if method == "GET":
                            resp = requests.get(target_url, headers=headers, timeout=20)
                        else:
                            resp = requests.post(target_url, headers={**headers, "Content-Type": "application/json"}, json={}, timeout=20)
                        api_diag.update({
                            "last_url": target_url,
                            "last_method": method,
                            "last_headers": list(headers.keys()),
                            "last_status": resp.status_code,
                        })
                        if resp.status_code >= 400:
                            last_error = f"{resp.status_code} {method} {target_url} with headers {list(headers.keys())}"
                            continue
                        data = resp.json()
                        api_diag["ok"] = True
                        return data
                    except requests.exceptions.RequestException as e:
                        last_error = str(e)
                        api_diag["last_error"] = last_error
                    except ValueError:
                        last_error = f"Invalid JSON from {method} {target_url}"
                        api_diag["last_error"] = last_error
                        continue
    api_diag["last_error"] = last_error
    return None


def normalize_data(data):
    """Normalize upstream payload to a stable schema: {updatedAt, items[], meta}."""
    normalized = {
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "items": [],
        "meta": {"api": {k: api_diag.get(k) for k in ["ok", "last_url", "last_method", "last_headers", "last_status", "last_error"]}},
    }
    if not data:
        return normalized

    # disruptions_bulk v2
    disruptions = data.get("disruptions") if isinstance(data, dict) else None
    if isinstance(disruptions, list):
        line_map = {}
        lines = data.get("lines") or []
        if isinstance(lines, list):
            for l in lines:
                lid = l.get("id") or l.get("lineId") or l.get("code")
                lname = l.get("name") or l.get("label") or l.get("code")
                if lid:
                    line_map[str(lid)] = lname or str(lid)
        for d in disruptions:
            raw_msg = d.get("title") or d.get("message") or d.get("cause") or "N/A"
            msg = unescape(re.sub(r"<[^>]*>", " ", str(raw_msg))).strip()
            severity = d.get("severity")
            cause = d.get("cause")
            last_update = d.get("lastUpdate")
            affected = d.get("affected_objects") or d.get("impacted_objects") or []
            if not affected:
                normalized["items"].append({
                    "line": None,
                    "message": msg,
                    "severity": severity,
                    "cause": cause,
                    "lastUpdate": last_update,
                    "id": d.get("id"),
                    "source": "disruptions_bulk_v2",
                })
                continue
            for a in affected:
                pt = a.get("pt_object") or a.get("pt_line") or {}
                line_id = pt.get("id") or pt.get("lineId") or pt.get("code")
                line_name = pt.get("name") or pt.get("label") or (line_map.get(str(line_id)) if line_id else None) or (str(line_id) if line_id else None)
                normalized["items"].append({
                    "line": line_name,
                    "message": msg,
                    "severity": severity,
                    "cause": cause,
                    "lastUpdate": last_update,
                    "id": d.get("id"),
                    "source": "disruptions_bulk_v2",
                })
        return normalized

    # Fallback: Siri GeneralMessage
    siri = isinstance(data, dict) and data.get("Siri")
    if siri:
        delivery = siri.get("ServiceDelivery", {}).get("GeneralMessageDelivery", [{}])[0]
        for msg in delivery.get("InfoMessage", []):
            content = msg.get("Content", {})
            message_text = content.get("Message", [{}])[0].get("MessageText", {}).get("value", "N/A")
            affected_lines = content.get("AffectedLine", [])
            if not affected_lines:
                normalized["items"].append({"line": None, "message": message_text, "severity": None, "id": None, "source": "Siri-GeneralMessage"})
            for al in affected_lines:
                line_name = al.get("LineRef", {}).get("value")
                normalized["items"].append({"line": line_name, "message": message_text, "severity": None, "id": None, "source": "Siri-GeneralMessage"})
    return normalized


def load_history():
    try:
        if os.path.exists(HISTORY_PATH):
            with open(HISTORY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"Warning: could not load history: {e}")
    return {"perLine": {}, "lastUpdated": None, "version": 1}


def save_history(history):
    try:
        os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
        tmp = HISTORY_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        os.replace(tmp, HISTORY_PATH)
    except Exception as e:
        print(f"Warning: could not save history: {e}")


def update_history(normalized):
    history = load_history()
    per_line = history.get("perLine", {})
    ts = normalized.get("updatedAt")
    for it in normalized.get("items", []):
        line = it.get("line") or "unknown"
        entry_id = it.get("id")
        if not entry_id:
            continue
        lst = per_line.get(line, [])
        if not any(e.get("id") == entry_id for e in lst):
            lst.append({"id": entry_id, "ts": ts, "message": it.get("message"), "severity": it.get("severity"), "cause": it.get("cause")})
            lst = lst[-100:]
            per_line[line] = lst
    history["perLine"] = per_line
    history["lastUpdated"] = ts
    save_history(history)


def ensure_git_repo(repo_path: str):
    try:
        repo = Repo(repo_path)
    except Exception:
        repo = Repo.init(repo_path)
    user_name = os.getenv("GIT_USER_NAME")
    user_email = os.getenv("GIT_USER_EMAIL")
    if user_name:
        repo.config_writer().set_value("user", "name", user_name).release()
    if user_email:
        repo.config_writer().set_value("user", "email", user_email).release()
    if 'origin' not in {r.name for r in repo.remotes}:
        remote_url = os.getenv("GIT_REMOTE")
        if remote_url:
            repo.create_remote('origin', remote_url)
        else:
            print("Git: no 'origin' remote configured; set GIT_REMOTE to enable pushes.")
    return repo


def archive_to_github(data):
    """Write raw and normalized snapshots with unique names, commit, and best-effort push."""
    try:
        repo = ensure_git_repo(REPO_PATH)
        today_str = datetime.now().strftime("%Y-%m-%d")
        data_dir = os.path.join(REPO_PATH, "data", today_str)
        os.makedirs(data_dir, exist_ok=True)

        base_min = datetime.now().strftime("%Y-%m-%d_%H-%M")
        candidate = base_min
        sec = datetime.now().strftime("%S")

        def make_paths(stem: str):
            return (
                os.path.join(data_dir, f"{stem}.json"),
                os.path.join(data_dir, f"{stem}.normalized.json"),
            )

        file_path, norm_path = make_paths(candidate)
        if os.path.exists(file_path) or os.path.exists(norm_path):
            candidate = f"{base_min}-{sec}s"
            file_path, norm_path = make_paths(candidate)
        suffix = 1
        while os.path.exists(file_path) or os.path.exists(norm_path):
            candidate = f"{base_min}-{sec}s-{suffix:02d}"
            file_path, norm_path = make_paths(candidate)
            suffix += 1

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        with open(norm_path, 'w', encoding='utf-8') as f:
            json.dump(normalize_data(data), f, ensure_ascii=False, indent=2)

        repo.index.add([file_path, norm_path])
        if repo.is_dirty(index=True, working_tree=True, untracked_files=True):
            author = Actor("RATP Status Bot", "bot@example.com")
            committer = Actor("RATP Status Bot", "bot@example.com")
            repo.index.commit(f"Data update: {candidate}", author=author, committer=committer)
            try:
                origin = repo.remote(name='origin')
                origin.push()
            except Exception as pe:
                print(f"Git push skipped/failed: {pe}")
        else:
            print("Archive: no changes to commit.")
    except Exception as e:
        print(f"Error during GitHub archival: {e}")


def get_ratp_status():
    with cache_lock:
        now = time.time()
        if now - data_cache["timestamp"] > CACHE_DURATION:
            api_data = get_api_data()
            if api_data:
                data_cache["data"] = api_data
                data_cache["timestamp"] = now
                try:
                    update_history(normalize_data(api_data))
                except Exception as e:
                    print(f"Warning: failed to update history: {e}")
        return data_cache["data"]


@app.route('/')
def index():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8"/>
        <title>RATP Status</title>
        <meta http-equiv="refresh" content="30">
        <style>
            :root { --ok:#2e7d32; --warn:#f9a825; --err:#c62828; --bg:#0b1020; --fg:#e5e7eb; --muted:#9aa5b1; --card:#0f1528; --border:#1e293b; }
            body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, 'Helvetica Neue', Arial, 'Noto Sans'; margin: 0; background: var(--bg); color: var(--fg); }
            header { padding: 16px 24px; background:#0a0f1e; border-bottom: 1px solid var(--border); display:flex; align-items:center; justify-content:space-between; position:sticky; top:0; }
            h1 { margin: 0; font-size: 18px; letter-spacing: .3px; }
            .meta { color: var(--muted); font-size: 12px; }
            .container { padding: 16px; }
            .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 14px; }
            .card { border: 1px solid var(--border); background:var(--card); border-radius: 10px; padding: 14px; }
            .card-header { display:flex; align-items:center; justify-content:space-between; margin-bottom:6px; }
            .line { font-weight: 700; display:flex; align-items:center; gap:8px; }
            .msg { font-size: 13px; color: var(--fg); }
            .sev { display:inline-block; padding:2px 8px; border-radius: 999px; font-size: 11px; margin-left: 6px; border:1px solid #0003; }
            .sev-BLOQUANTE,.sev-CRITIQUE { background: var(--err); color:#fff; }
            .sev-PERTURBEE,.sev-MAJEURE,.sev-MINEURE { background: var(--warn); color:#111; }
            .sev-NORMALE,.sev-INFO { background: var(--ok); color:#fff; }
            .actions a { color:#60a5fa; text-decoration:none; margin-right: 12px; font-size: 13px; }
            .actions { margin: 8px 0 16px; }
            .empty { color: var(--muted); font-style: italic; padding: 16px; }
            .chip { display:inline-flex; align-items:center; gap:6px; padding:4px 10px; border-radius: 999px; font-weight:700; color:#fff; text-shadow:0 1px 0 #0006; border:1px solid #0004; font-size:13px; }
            .chip small { font-weight:600; opacity:.95; }
            .chip.metro { background:#003CA6; }
            .chip.rer { background:#E2001A; }
            .chip.tram { background:#78BE20; }
            .chip.bus { background:#6C3; color:#111; text-shadow:none; }
            .chip.noir { background:#000; }
            .chip.m1{background:#FFCD00;color:#111;text-shadow:none}
            .chip.m2{background:#003CA6}
            .chip.m3{background:#837902}
            .chip.m4{background:#CF009E}
            .chip.m5{background:#FF7E2E;color:#111;text-shadow:none}
            .chip.m6{background:#6ECA97;color:#111;text-shadow:none}
            .chip.m7{background:#FA9ABA;color:#111;text-shadow:none}
            .chip.m8{background:#D5C900;color:#111;text-shadow:none}
            .chip.m9{background:#B6BD00;color:#111;text-shadow:none}
            .chip.m10{background:#C9910D;color:#111;text-shadow:none}
            .chip.m11{background:#704B1C;color:#fff}
            .chip.m12{background:#007852}
            .chip.m13{background:#6EC4E8;color:#111;text-shadow:none}
            .chip.m14{background:#62259D}
            .chip.ra{background:#ED7D31}
            .chip.rb{background:#3AAA35}
            .chip.rc{background:#FFD200;color:#111;text-shadow:none}
            .chip.rd{background:#E41E25}
            .chip.re{background:#0072BC}
        </style>
    </head>
    <body>
        <header>
            <h1>RATP Status</h1>
            <div class="meta" id="meta">chargement…</div>
        </header>
        <div class="container">
            <div class="actions">
                <a href="/status_normalized.json" target="_blank">JSON normalisé</a>
                <a href="/history.json" target="_blank">Mémoire par ligne</a>
                <a href="/health" target="_blank">Diagnostics</a>
            </div>
            <div id="grid" class="grid"></div>
            <div id="empty" class="empty" style="display:none">Aucune perturbation active.</div>
        </div>
        <script>
            function sevClass(s){ return s ? ('sev sev-' + s.replace(/\\s+/g,'_')) : 'sev'; }
            function lineToken(s){ if(!s) return {code:'',net:'',cls:''};
                const t = String(s).toUpperCase();
                const m = t.match(/(METRO|M)\s*(\d{1,2})/);
                if(m) return {code:m[2], net:'M', cls:('chip metro m'+m[2])};
                const r = t.match(/RER\s*([A-E])/);
                if(r) return {code:r[1], net:'RER', cls:('chip rer r'+r[1].toLowerCase())};
                const tr = t.match(/T(RAM)?\s*(\d{1,2})/);
                if(tr) return {code:('T'+tr[2]), net:'TRAM', cls:'chip tram'};
                const b = t.match(/BUS\s*(\d+)/);
                if(b) return {code:b[1], net:'BUS', cls:'chip bus'};
                return {code:t, net:'', cls:'chip noir'};
            }
            function lineBadge(name){ const tk=lineToken(name); return `<span class="${tk.cls}"><span>${tk.net||''}</span><small>${tk.code||name||''}</small></span>`; }
            function render(data){
                const grid = document.getElementById('grid');
                const empty = document.getElementById('empty');
                const meta = document.getElementById('meta');
                grid.innerHTML='';
                let items = (data && data.items) || [];
                meta.textContent = (data && data.updatedAt) ? ('Maj: ' + data.updatedAt) : '';
                if(!items.length){ empty.style.display='block'; return; }
                empty.style.display='none';
                // Afficher une carte par perturbation pour éviter la fusion en une seule ligne "Inconnue"
                // Tri simple: BLOQUANTE/CRITIQUE en premier
                const sevRank = s => ({'BLOQUANTE':0,'CRITIQUE':1,'MAJEURE':2,'MINEURE':3,'PERTURBEE':4,'INFO':5,'NORMALE':6})[s||'INFO'] ?? 9;
                items = items.slice().sort((a,b)=> sevRank(a.severity)-sevRank(b.severity));
                for(const it of items){
                    const line = it.line || 'Inconnue';
                    const sev = it.severity || '';
                    const msg = it.message || '';
                    const card = document.createElement('div');
                    card.className='card';
                    card.innerHTML = `<div class="card-header"><div class="line">${lineBadge(line)}<span class="${sevClass(sev)}">${sev || ''}</span></div>
                        <a style="color:#93c5fd; font-size:12px; text-decoration:none;" href="/history.json#${encodeURIComponent(line)}" target="_blank">Historique</a></div>
                        <div class="msg">${msg}</div>`;
                    grid.appendChild(card);
                }
            }
            fetch('/status_normalized.json').then(r=>r.json()).then(render).catch(()=>{ document.getElementById('meta').textContent='Erreur de chargement';});
        </script>
    </body>
    </html>
    """
    return render_template_string(html)


@app.route('/status.json')
def status_json():
    return jsonify(get_ratp_status() or {})


@app.route('/status_normalized.json')
def status_normalized_json():
    return jsonify(normalize_data(get_ratp_status()))


@app.route('/health')
def health():
    data = get_ratp_status()
    norm = normalize_data(data)
    return jsonify({"ok": bool(norm.get("items")), "items_count": len(norm.get("items", [])), "meta": norm.get("meta")})


@app.route('/history.json')
def history_json():
    return jsonify(load_history())


@app.route('/admin/force-archive')
def admin_force_archive():
    data = get_ratp_status()
    if not data:
        return jsonify({"ok": False, "error": "no data"}), 503
    try:
        archive_to_github(data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def main_loop(archive=False):
    while True:
        data = get_ratp_status()
        if data and archive:
            archive_to_github(data)
        time.sleep(CACHE_DURATION)


def display_in_console(data, network=None, line=None):
    norm = normalize_data(data)
    items = norm.get("items", [])
    print(f"Mise à jour: {norm.get('updatedAt')}")
    if not items:
        print("Aucune perturbation active.")
        return
    for it in items:
        ln = it.get("line") or "N/A"
        sev = it.get("severity") or ""
        msg = it.get("message") or ""
        print(f"- [{sev}] {ln}: {msg}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RATP Status Checker")
    parser.add_argument("--json", action="store_true", help="Output in JSON format.")
    parser.add_argument("--server", action="store_true", help="Run as an HTTP server.")
    parser.add_argument("--archive", action="store_true", help="Enable GitHub archiving (used with --server).")
    parser.add_argument("--network", type=str, help="Filter by network (e.g., 'metro').")
    parser.add_argument("--line", type=str, help="Filter by line (e.g., '1').")
    parser.add_argument("--api-key", type=str, help="Override API key (or set IDFM_API_KEY env var).")
    parser.add_argument("--api-url", type=str, help="Override API base URL (or set IDFM_API_URL env var).")

    args = parser.parse_args()

    if args.api_key:
        API_KEY = args.api_key
    if args.api_url:
        API_URL_BASE = args.api_url
        API_URL_CANDIDATES = _build_api_url_candidates(API_URL_BASE)

    if args.server:
        if args.archive:
            Thread(target=main_loop, args=(True,), daemon=True).start()
        app.run(host='0.0.0.0', port=3000)
    else:
        data = get_ratp_status()
        if args.json:
            print(json.dumps(data, indent=2))
        else:
            display_in_console(data, args.network, args.line)
