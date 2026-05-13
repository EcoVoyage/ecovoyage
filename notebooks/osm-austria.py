import marimo

__generated_with = "0.23.6"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import os
    import time
    import textwrap
    import requests
    from pathlib import Path
    import polars as pl
    import folium

    return Path, folium, mo, os, pl, requests, textwrap, time


@app.cell
def _resolved_urls(os, pl):
    # Diagnostic cell — same shape as osm-monaco-viz.py's `_resolved_urls`.
    # Single source of truth for every external URL this notebook touches;
    # downstream map cells consume `martin` as a parameter instead of
    # re-reading os.environ (R3).
    _entries = [
        ("Airflow DAGs folder",                  "AIRFLOW_DAGS_DIR",            "/workspace/dags",                  "kernel"),
        ("Airflow REST API (kernel-side)",       "AIRFLOW_API_INTERNAL_URL",    "http://localhost:8080",            "kernel"),
        ("Airflow UI (browser-side)",            "AIRFLOW_PUBLIC_URL",          "http://127.0.0.1:28080",           "browser"),
        ("Martin tile server (browser-side)",    "MARTIN_PUBLIC_URL",           "http://127.0.0.1:23000",           "browser"),
        ("Versatiles serve (browser-side)",      "VERSATILES_PUBLIC_URL",       "http://127.0.0.1:28090",           "browser"),
        ("Versatiles style bundle (browser)",    "VERSATILES_STYLE_PUBLIC_URL", "http://127.0.0.1:28002/style",     "browser"),
        ("Versatiles assets root (browser)",     "VERSATILES_ASSETS_PUBLIC_URL","http://127.0.0.1:28002",           "browser"),
        ("PMTiles Viewer (browser-side)",        "PMTILES_VIEWER_PUBLIC_URL",   "http://127.0.0.1:28001",           "browser"),
    ]
    _resolved = {e[1]: os.environ.get(e[1], e[2]) for e in _entries}
    urls = pl.DataFrame({
        "purpose":   [e[0] for e in _entries],
        "env_var":   [e[1] for e in _entries],
        "value":     [_resolved[e[1]] for e in _entries],
        "side":      [e[3] for e in _entries],
        "is_default":[os.environ.get(e[1]) is None for e in _entries],
    })
    martin                = _resolved["MARTIN_PUBLIC_URL"]
    airflow_public        = _resolved["AIRFLOW_PUBLIC_URL"]
    urls
    return airflow_public, martin


@app.cell
def _(airflow_public, martin, mo):
    # f-string interpolation against the resolved-URL parameters from
    # the diagnostic cell so port numbers stay accurate when
    # `port: [auto]` rotates host ports across rebuilds. NEVER hardcode
    # 127.0.0.1:<port> here.
    mo.md(
        f"""
        # Austria — OSM + GTFS marimo demo (Pipeline 4 only)

        Sibling to `osm-monaco-viz.py`, scoped to Austria and pared down to
        a single tile-generation pipeline. This notebook **writes its own
        Airflow DAGs** to `${{AIRFLOW_DAGS_DIR}}` (two DAGs — OSM+freestiler
        and GTFS), **triggers** them via the Airflow REST API at
        <{airflow_public}>, **polls until each succeeds**, then renders:

        - **Austria vector-tile map** — PMTiles produced by
          `notebook_austria_pipeline` (PBF → quackosm GeoParquet →
          freestiler's Rust tiling engine → PMTiles archive), served by
          martin at <{martin}> as the `austria-duckdb-freestiler` source.
        - **Transit map** — Austrian railway stops rendered as a folium
          `FastMarkerCluster` (Leaflet.markercluster) on default
          OpenStreetMap raster tiles. ~7.6k stops cluster at country
          zoom and explode into individual markers on zoom-in. Produced
          by `notebook_austria_gtfs_pipeline` from the transitous.org
          `at_Railway-Current-Reference-Data-2026` feed.

        ## Download policy — monthly-cached, idempotent

        Both DAGs run on `schedule="@monthly"` (Airflow's cron alias for
        `0 0 1 * *`) so the scheduler auto-fires them on the 1st of each
        month at 00:00 UTC. Each download task additionally short-circuits
        when the cached file exists AND its mtime falls in the current
        calendar month — so ad-hoc / notebook-triggered re-runs within a
        month skip the network fetch entirely. Together: the file
        materializes at most once per month, exactly when stale.

        ## Data sources

        | Source | URL |
        |---|---|
        | OSM PBF | `https://download.geofabrik.de/europe/austria-latest.osm.pbf` (~750 MB) |
        | GTFS    | `https://api.transitous.org/gtfs/at_Railway-Current-Reference-Data-2026.gtfs.zip` |

        ## URL strategy — server-side vs browser-side

        Same two-space split as the Monaco notebook: kernel-side calls
        (notebook → Airflow REST) use `AIRFLOW_API_INTERNAL_URL`; the
        MapLibre map cell embeds `MARTIN_PUBLIC_URL` into its iframe so
        the browser can reach martin via the published host port. The
        diagnostic table above resolves both at runtime — the values
        rotate when `port: [auto]` rotates host ports on rebuild.
        """
    )
    return


@app.cell
def _(Path, os, textwrap):
    # Self-author BOTH pipeline DAGs (OSM+freestiler consolidated, GTFS).
    # Idempotent — overwriting on every notebook run keeps both DAG
    # bodies in sync with this notebook (single source of truth: this
    # cell IS each DAG spec).
    dags_dir = Path(os.environ.get(
        "AIRFLOW_DAGS_DIR",
        os.path.expanduser("/workspace/dags"),
    ))
    dags_dir.mkdir(parents=True, exist_ok=True)

    # ---- Austria OSM + freestiler DAG (consolidated) ----
    # The freestiler step needs the PBF→parquet output, so chaining
    # download_pbf → pbf_to_geoparquet → freestiler_convert →
    # reload_martin in a single DAG eliminates the cross-DAG wait
    # loop the Monaco notebook needs (where freestiler is a separate
    # DAG that polls for monaco.parquet).
    austria_dag_id = "notebook_austria_pipeline"
    austria_dag_file = dags_dir / f"{austria_dag_id}.py"
    austria_dag_file.write_text(textwrap.dedent('''
        """Austria OSM → DuckDB-front-end → freestiler PMTiles pipeline.

        Self-authored by osm-austria.py. Downloads austria-latest.osm.pbf
        from Geofabrik, converts to GeoParquet via quackosm, then hands
        the parquet to freestiler's Rust tiling engine (DuckDB SQL
        front-end + in-process MVT encoding + PMTiles archive packing
        in one library call). Output lands under the workspace volume
        at the path martin auto-discovers.

        Download policy: skip-if-cached-this-month + schedule="@monthly".
        See _needs_download() below.
        """
        import os
        import subprocess
        from datetime import datetime, timezone
        from pathlib import Path

        from airflow.sdk import dag, task

        WORK = Path(os.path.expanduser("/workspace/tiles/work"))
        TILES = Path(os.path.expanduser("/workspace/tiles/pmtiles"))


        def _needs_download(path: Path) -> bool:
            """Skip if the file exists, is non-empty, AND was fetched
            in the current calendar month (UTC). Otherwise re-fetch.

            Combined with schedule='@monthly' on the DAG, this gives two
            independent guards:
              * Airflow auto-fires the DAG on the 1st of each month →
                the file's mtime will be from last month → re-download.
              * Ad-hoc / notebook-triggered runs within the same month
                skip the download (cached).
            """
            if not path.exists() or path.stat().st_size == 0:
                return True
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            return (mtime.year, mtime.month) != (now.year, now.month)


        @dag(
            dag_id="notebook_austria_pipeline",
            schedule="@monthly",
            start_date=datetime(2026, 1, 1),
            catchup=False,
            max_active_runs=1,
            tags=["osm", "austria", "notebook", "duckdb-freestiler"],
        )
        def notebook_austria_pipeline():
            @task
            def download_pbf() -> str:
                # Skip if cached this calendar month; otherwise stream-
                # download with atomic .part rename so a partial fetch
                # never corrupts the cache.
                import shutil
                import urllib.request
                WORK.mkdir(parents=True, exist_ok=True)
                out = WORK / "austria.osm.pbf"
                if not _needs_download(out):
                    return str(out)
                url = "https://download.geofabrik.de/europe/austria-latest.osm.pbf"
                # 900s timeout: Austria PBF is ~750 MB; the 300s used for
                # 12 MB Monaco doesn't leave headroom for typical
                # Geofabrik throughput.
                tmp = out.with_suffix(".pbf.part")
                try:
                    with urllib.request.urlopen(url, timeout=900) as resp:
                        with open(tmp, "wb") as f:
                            shutil.copyfileobj(resp, f)
                    tmp.replace(out)
                finally:
                    if tmp.exists():
                        tmp.unlink()
                return str(out)

            @task
            def pbf_to_geoparquet(pbf_path: str) -> str:
                import quackosm as qosm
                out = WORK / "austria.parquet"
                qosm.convert_pbf_to_parquet(pbf_path, result_file_path=str(out))
                return str(out)

            @task
            def freestiler_convert(parquet_path: str) -> str:
                # freestiler accepts either a file path OR a DuckDB SQL
                # query as input. Use the SQL form to demonstrate the
                # DuckDB-front-end pathway. API surface (function name +
                # kwargs) is verified at runtime — if the upstream
                # library renames things we surface the actual public
                # surface instead of an opaque AttributeError.
                #
                # max_zoom=12 vs Monaco's 14: at country scale (~750 MB
                # PBF, ~84k km^2), z14 produces tens of millions of tiles
                # and a multi-GB PMTiles archive. z12 keeps the archive
                # at single-GB scale while still giving city-level detail
                # for Vienna / Salzburg / Innsbruck. Bump back to 14 if
                # you need building-footprint zoom for a specific city
                # — single-constant tunable.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-duckdb-freestiler.pmtiles"
                query = f"SELECT * FROM read_parquet('{parquet_path}')"
                if hasattr(freestiler, "freestile_query"):
                    freestiler.freestile_query(
                        query=query,
                        output=str(out),
                        layer_name="austria",
                        min_zoom=0,
                        max_zoom=12,
                    )
                elif hasattr(freestiler, "freestile"):
                    freestiler.freestile(
                        input=query,
                        output=str(out),
                        layer_name="austria",
                        min_zoom=0,
                        max_zoom=12,
                    )
                else:
                    public = sorted(n for n in dir(freestiler) if not n.startswith("_"))
                    raise RuntimeError(
                        f"freestiler public API: {public} — expected "
                        "freestile_query or freestile; adapt this task."
                    )
                return str(out)

            @task
            def reload_martin(pmtiles_path: str) -> str:
                # Identical sync primitives as the Monaco DAGs:
                #   1. flock — serializes the supervisorctl invocations
                #      so only one restart runs at a time globally.
                #   2. TCP readiness probe + /catalog membership check —
                #      verifies the END STATE (martin RUNNING + our
                #      source listed) instead of trusting supervisorctl's
                #      exit code (which can be non-zero even when martin
                #      ends up healthy).
                import fcntl
                import json as _json
                import socket
                import time as _time
                import urllib.request
                source_name = pmtiles_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                with open("/tmp/ov-martin-restart.lock", "w") as _lock:
                    fcntl.flock(_lock.fileno(), fcntl.LOCK_EX)
                    subprocess.run(
                        ["supervisorctl", "restart", "martin"],
                        check=False,
                    )
                _deadline = _time.monotonic() + 30
                while _time.monotonic() < _deadline:
                    try:
                        with socket.create_connection(
                            ("localhost", 3000), timeout=2,
                        ):
                            break
                    except (ConnectionRefusedError, OSError, socket.timeout):
                        _time.sleep(0.5)
                else:
                    raise RuntimeError(
                        "martin port 3000 not reachable 30s after restart",
                    )
                with urllib.request.urlopen(
                    "http://localhost:3000/catalog", timeout=10,
                ) as _resp:
                    _catalog = _json.load(_resp)
                if source_name not in _catalog.get("tiles", {}):
                    raise RuntimeError(
                        f"martin /catalog missing source '{source_name}' "
                        f"after reload; available="
                        f"{sorted(_catalog.get('tiles', {}).keys())}",
                    )
                return pmtiles_path

            reload_martin(freestiler_convert(pbf_to_geoparquet(download_pbf())))


        notebook_austria_pipeline()
    ''').lstrip())

    # ---- Austria GTFS DAG ----
    gtfs_dag_id = "notebook_austria_gtfs_pipeline"
    gtfs_dag_file = dags_dir / f"{gtfs_dag_id}.py"
    gtfs_dag_file.write_text(textwrap.dedent('''
        """Austria railway GTFS pipeline self-authored by osm-austria.py.

        Downloads the at_Railway-Current-Reference-Data-2026 GTFS feed
        from transitous.org and parses it into Parquet via gtfs-parquet
        (one .parquet per GTFS table — stops, routes, trips, stop_times,
        etc.).

        Download policy: skip-if-cached-this-month + schedule="@monthly".
        """
        import os
        from datetime import datetime, timezone
        from pathlib import Path

        from airflow.sdk import dag, task

        # Per-feed subdir under /workspace/gtfs/ so Austria's parquet
        # output never overwrites Monaco's (or any other feed's). Same
        # pattern for `raw/` and `parquet/`.
        RAW = Path(os.path.expanduser("/workspace/gtfs/austria/raw"))
        PARQUET = Path(os.path.expanduser("/workspace/gtfs/austria/parquet"))


        def _needs_download(path: Path) -> bool:
            """Same policy as the OSM DAG — exists + this-month-mtime."""
            if not path.exists() or path.stat().st_size == 0:
                return True
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            return (mtime.year, mtime.month) != (now.year, now.month)


        @dag(
            dag_id="notebook_austria_gtfs_pipeline",
            schedule="@monthly",
            start_date=datetime(2026, 1, 1),
            catchup=False,
            max_active_runs=1,
            tags=["gtfs", "austria", "transit", "notebook"],
        )
        def notebook_austria_gtfs_pipeline():
            @task
            def download_gtfs() -> str:
                import shutil
                import urllib.request
                RAW.mkdir(parents=True, exist_ok=True)
                url = "https://api.transitous.org/gtfs/at_Railway-Current-Reference-Data-2026.gtfs.zip"
                out = RAW / "austria.gtfs.zip"
                if not _needs_download(out):
                    return str(out)
                tmp = out.with_suffix(".zip.part")
                try:
                    with urllib.request.urlopen(url, timeout=300) as resp:
                        with open(tmp, "wb") as f:
                            shutil.copyfileobj(resp, f)
                    tmp.replace(out)
                finally:
                    if tmp.exists():
                        tmp.unlink()
                return str(out)

            @task
            def gtfs_to_parquet(zip_path: str) -> str:
                from gtfs_parquet import parse_gtfs, write_parquet
                PARQUET.mkdir(parents=True, exist_ok=True)
                feed = parse_gtfs(zip_path)
                write_parquet(feed, str(PARQUET))
                return str(PARQUET)

            gtfs_to_parquet(download_gtfs())


        notebook_austria_gtfs_pipeline()
    ''').lstrip())

    dag_ids = [austria_dag_id, gtfs_dag_id]
    dag_files = {
        austria_dag_id: austria_dag_file,
        gtfs_dag_id: gtfs_dag_file,
    }
    return dag_files, dag_ids


@app.cell
def _(dag_files, dag_ids, os, requests, time):
    # Adopt-or-trigger DAG runs, then poll to terminal state. Aligns
    # with schedule="@monthly" + the per-task month-bucket cache:
    # exactly one execution per DAG per calendar month is the desired
    # semantic. Re-running this cell mid-pipeline ADOPTS the in-flight
    # DagRun (whether triggered by the @monthly scheduler or by a
    # previous run of this cell) and polls — it does NOT fire a
    # redundant parallel run. That's why the cell signature carries
    # `dag_files`/`dag_ids` from the writer cell but never re-triggers
    # if a usable run already exists.
    _api = os.environ.get("AIRFLOW_API_INTERNAL_URL", "http://localhost:8080")
    _pwd = os.environ["AIRFLOW_ADMIN_PASSWORD"]

    # Airflow under LocalExecutor + SQLite serializes ALL writes through
    # one writer; a concurrent dagRun POST or state-poll GET can collide
    # on the SQLite lock and surface HTTP 500. Retry 5xx with 1s/2s/4s
    # backoff — principled back-pressure handling, not a magic-sleep
    # workaround (R4 distinction).
    def _http_with_retry(method, url, *, headers, json=None, timeout=10, retries=3):
        _backoff = 1
        for _attempt in range(retries):
            _resp = requests.request(
                method, url, headers=headers, json=json, timeout=timeout,
            )
            if _resp.status_code < 500:
                _resp.raise_for_status()
                return _resp
            if _attempt == retries - 1:
                _resp.raise_for_status()
            time.sleep(_backoff)
            _backoff *= 2
        return _resp  # unreachable

    _token = _http_with_retry(
        "POST",
        f"{_api}/auth/token",
        headers={},
        json={"username": "admin", "password": _pwd},
        timeout=10,
    ).json()["access_token"]
    _auth = {"Authorization": f"Bearer {_token}"}

    from datetime import datetime, timezone
    _now = datetime.now(timezone.utc)
    _this_month = (_now.year, _now.month)

    # Phase 1 — wait for scheduler registration + unpause if needed.
    # The dag-processor scans the dags folder every 10s
    # (AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10); 90s gives ~9 scan
    # opportunities per DAG.
    for _did in dag_ids:
        _reg_deadline = time.monotonic() + 90
        while time.monotonic() < _reg_deadline:
            _r = requests.get(
                f"{_api}/api/v2/dags/{_did}", headers=_auth, timeout=5,
            )
            if _r.status_code == 200:
                if _r.json().get("is_paused"):
                    requests.patch(
                        f"{_api}/api/v2/dags/{_did}",
                        headers=_auth, json={"is_paused": False}, timeout=5,
                    )
                    time.sleep(1)
                    continue
                break
            time.sleep(2)
        else:
            raise RuntimeError(
                f"Airflow never registered DAG {_did} from {dag_files[_did]}"
            )

    # Phase 2 — pick the target run for each DAG. Decision rules:
    #
    #   * non-terminal run exists (running/queued) → ADOPT it. The
    #     scheduler is already working on this DAG; firing another
    #     trigger would just queue a redundant run behind
    #     max_active_runs=1.
    #
    #   * terminal-success run THIS calendar month → ADOPT. The
    #     artifacts are already on disk; downstream cells see
    #     state=success and proceed without any wall-clock cost.
    #
    #   * neither → TRIGGER a new manual run. First-ever execution of
    #     a fresh deploy, or a re-run after a non-success terminal.
    #
    # The Airflow REST returns most-recent dagRuns first when sorted
    # by descending logical_date, so we scan up to 10 and pick the
    # first match by the rules above.
    dag_run_ids = {}
    for _did in dag_ids:
        _runs = _http_with_retry(
            "GET",
            f"{_api}/api/v2/dags/{_did}/dagRuns?limit=10&order_by=-logical_date",
            headers=_auth,
        ).json().get("dag_runs", [])
        _adopt = None
        for _r in _runs:
            _state = _r.get("state")
            if _state in ("running", "queued"):
                _adopt = _r
                break
            if _state == "success":
                _end = _r.get("end_date") or _r.get("logical_date")
                if _end:
                    _dt = datetime.fromisoformat(_end.replace("Z", "+00:00"))
                    if (_dt.year, _dt.month) == _this_month:
                        _adopt = _r
                        break
            # else (failed / upstream_failed / removed / …) → keep scanning
        if _adopt is None:
            _new = _http_with_retry(
                "POST",
                f"{_api}/api/v2/dags/{_did}/dagRuns",
                headers=_auth,
                json={
                    "conf": {},
                    "logical_date": _now.isoformat(),
                },
                timeout=10,
            ).json()
            dag_run_ids[_did] = _new["dag_run_id"]
        else:
            dag_run_ids[_did] = _adopt["dag_run_id"]

    # Phase 3 — poll BOTH DAGs' target runs concurrently until each
    # reaches terminal state. 2400s (40 min) covers a cold-cache
    # Austria run: 750 MB PBF + quackosm GeoParquet + freestiler MVT/
    # PMTiles encoding at z12 on a country extent. Warm-cache reuse
    # (adopted success run) short-circuits to ~0s. State-poll GETs
    # also go through the retry helper since they compete with worker
    # writes on the same SQLite lock.
    dag_run_states = {}
    _poll_deadline = time.monotonic() + 2400
    while time.monotonic() < _poll_deadline and len(dag_run_states) < len(dag_ids):
        for _did in dag_ids:
            if _did in dag_run_states:
                continue
            _state = _http_with_retry(
                "GET",
                f"{_api}/api/v2/dags/{_did}/dagRuns/{dag_run_ids[_did]}",
                headers=_auth,
                timeout=5,
            ).json()["state"]
            if _state in ("success", "failed"):
                dag_run_states[_did] = _state
        if len(dag_run_states) < len(dag_ids):
            time.sleep(3)
    if len(dag_run_states) < len(dag_ids):
        _missing = [d for d in dag_ids if d not in dag_run_states]
        raise TimeoutError(f"DAGs {_missing} did not finish in 40 min")

    _failed = [d for d, s in dag_run_states.items() if s != "success"]
    if _failed:
        raise RuntimeError(f"DAG(s) ended non-success: {dag_run_states}")

    dag_run_states
    return (dag_run_states,)


@app.function
# Helper shared with the Pipeline 4 map cell. `@app.function` makes
# the name visible to every cell without an explicit dependency
# claim. Parameterized on `center` / `zoom` / `layer_name` so the
# same template renders Austria correctly (vs Monaco's hardcoded
# 7.4246/43.7384/13/'monaco' in osm-monaco-viz.py).
def build_pipeline_maplibre_html(
    martin: str,
    source_name: str,
    *,
    layer_name: str,
    center: list,
    zoom: int,
) -> str:
    """MapLibre HTML template for a martin vector-tile source.

    `layer_prefix` carries hyphens (valid in HTML id attributes,
    invalid in JS identifiers); `js_var` is the underscore-only twin
    used for `const map_<var>` and any JS-identifier position. HTML
    ids keep the hyphenated form for stable DOM-side selectors.
    Without the split, source names like `austria-duckdb-freestiler`
    would generate `const map_austria-duckdb-freestiler = …` which
    the JS parser silently mis-tokenises as a subtraction expression
    and throws ReferenceError. Same R3 derivation as the Monaco
    notebook.
    """
    layer_prefix = source_name
    js_var = source_name.replace("-", "_")
    return f"""<!DOCTYPE html>
<html><head>
<link href="https://unpkg.com/maplibre-gl@5.24.0/dist/maplibre-gl.css" rel="stylesheet"/>
<script src="https://unpkg.com/maplibre-gl@5.24.0/dist/maplibre-gl.js"></script>
<style>html,body{{margin:0;padding:0;}}#map-{layer_prefix}{{height:500px;width:100%;}}</style>
</head><body>
<div id="map-{layer_prefix}"></div>
<script>
const map_{js_var} = new maplibregl.Map({{
  container: 'map-{layer_prefix}',
  style: {{
version: 8,
sources: {{ src: {{ type: 'vector', url: '{martin}/{source_name}' }} }},
layers: [
  {{ id: 'bg-{layer_prefix}', type: 'background',
     paint: {{ 'background-color': '#f6f3ec' }} }},
  {{ id: 'fill-{layer_prefix}', type: 'fill', source: 'src', 'source-layer': '{layer_name}',
     filter: ['==', ['geometry-type'], 'Polygon'],
     paint: {{ 'fill-color': '#a4c0a8', 'fill-outline-color': '#5e7060',
               'fill-opacity': 0.55 }} }},
  {{ id: 'line-{layer_prefix}', type: 'line', source: 'src', 'source-layer': '{layer_name}',
     filter: ['==', ['geometry-type'], 'LineString'],
     paint: {{ 'line-color': '#3a3a3a', 'line-width': 0.8 }} }},
  {{ id: 'circ-{layer_prefix}', type: 'circle', source: 'src', 'source-layer': '{layer_name}',
     filter: ['==', ['geometry-type'], 'Point'],
     paint: {{ 'circle-color': '#b04a3d', 'circle-radius': 1.5 }} }}
]
  }},
  center: [{center[0]}, {center[1]}],
  zoom: {zoom},
  attributionControl: false
}});
map_{js_var}.addControl(new maplibregl.NavigationControl({{ showZoom: true, showCompass: true }}), 'top-right');
</script>
</body></html>"""


@app.cell
def _(dag_run_states, martin, mo):
    # Pipeline 4 — DuckDB → freestiler. Renders the Austria PMTiles
    # archive that martin auto-discovered after the consolidated DAG's
    # freestiler_convert + reload_martin tasks completed.
    #
    # center = [lon, lat] (MapLibre convention; opposite of folium's
    # [lat, lon]). 13.3, 47.7 = geographic center of Austria.
    # zoom 7 fits the whole country in a 500px-tall iframe.
    #
    # mo.stop() halts the cell gracefully when the upstream DAG hasn't
    # succeeded yet — shows a marimo callout instead of a red
    # exception. The dataflow guard means this cell only runs after
    # the trigger cell's poll completes, so the only path through is
    # state == "success".
    mo.stop(
        dag_run_states.get("notebook_austria_pipeline") != "success",
        f"Waiting for notebook_austria_pipeline (state="
        f"{dag_run_states.get('notebook_austria_pipeline')!r})",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-duckdb-freestiler",
            layer_name="austria",
            center=[13.3, 47.7],
            zoom=7,
        ),
        height="500px",
    )
    return


@app.cell
def _(dag_run_states, mo, os, pl):
    # Class A — server-side polars on the GTFS parquet directory the
    # Austria GTFS DAG produced. Reports stop/route counts plus the top
    # routes by distinct-stop count. mo.stop() lets this cell wait on
    # the GTFS DAG gracefully (callout instead of red exception) when
    # the DAG hasn't yet succeeded.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    # Austria-specific GTFS dir (set by the DAG to /workspace/gtfs/austria/parquet)
    # so this notebook never reads Monaco's parquet by accident.
    gtfs_dir = os.path.expanduser("/workspace/gtfs/austria/parquet")

    df_stops = pl.read_parquet(f"{gtfs_dir}/stops.parquet")
    df_routes = pl.read_parquet(f"{gtfs_dir}/routes.parquet")
    df_trips = pl.read_parquet(f"{gtfs_dir}/trips.parquet")
    df_stop_times = pl.read_parquet(f"{gtfs_dir}/stop_times.parquet")

    df_route_stops = (
        df_trips.lazy()
        .join(df_stop_times.lazy(), on="trip_id")
        .join(df_routes.lazy(), on="route_id")
        .group_by(["route_short_name", "route_long_name"])
        .agg(pl.col("stop_id").n_unique().alias("n_stops"))
        .sort("n_stops", descending=True)
        .head(15)
        .collect()
    )
    gtfs_summary = pl.DataFrame({
        "metric": ["stops", "routes", "trips", "stop_times"],
        "count":  [df_stops.height, df_routes.height,
                   df_trips.height, df_stop_times.height],
    })
    gtfs_summary
    return df_route_stops, df_stops


@app.cell
def _(df_route_stops):
    df_route_stops
    return


@app.cell
def _(df_stops, folium):
    # Transit map — Austrian railway stops on default OpenStreetMap
    # raster tiles. The Austria rail feed has ~7,600 stops which the
    # naive per-stop CircleMarker pattern serializes to ~20 MB of HTML
    # (each marker emits its own JS instantiation + popup binding).
    # That blows past marimo's default output_max_bytes=10_000_000.
    #
    # FastMarkerCluster solves this with one call: it ships the raw
    # [lat, lon] array as JSON to the browser, where Leaflet.markercluster
    # builds cluster bubbles client-side. Total HTML drops to ~1 MB
    # (linear in stops × ~16 bytes each instead of × ~2.5 KB each).
    # Clusters auto-explode on zoom-in, so the country-overview UX
    # stays clean and zoomed-in users see individual stops.
    from folium.plugins import FastMarkerCluster
    transit_map = folium.Map(
        location=[47.7, 13.3],
        zoom_start=7,
        tiles="OpenStreetMap",
    )
    # df_stops has potential nulls in stop_lat/stop_lon for placeholder
    # rows; filter them out before passing to FastMarkerCluster. Cast
    # to plain Python floats — JS-Math-compatible types only.
    _coords = [
        [float(_row["stop_lat"]), float(_row["stop_lon"])]
        for _row in df_stops.iter_rows(named=True)
        if _row.get("stop_lat") is not None and _row.get("stop_lon") is not None
    ]
    FastMarkerCluster(_coords, name="Railway stops").add_to(transit_map)
    # Set the Figure height (NOT the Map height) — branca emits a
    # "Make this Notebook Trusted" wrapper when figure.height is None,
    # which hides the map content in non-Jupyter renderers like marimo.
    transit_map.get_root().height = "500px"
    transit_map
    return


if __name__ == "__main__":
    app.run()
