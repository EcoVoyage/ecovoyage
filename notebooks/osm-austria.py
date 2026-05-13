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
            def freestiler_railway_convert(parquet_path: str) -> str:
                # ORM-aligned freestiler SQL: filters the country-scale parquet
                # to railway-related features only, projects the exact tag set
                # OpenRailwayMap's osm2pgsql import + rendering views consume
                # (orm-simple.style + sql/osm_carto_views.sql in
                # OpenRailwayMap-CartoCSS), drops the raw tags Map<String,String>
                # entirely. Output: MLT-encoded tiles inside a PMTiles archive
                # at austria-railway.pmtiles.
                #
                # max_zoom=14 (vs the all-features pipeline's 12): the railway
                # subset is a small fraction of the full PBF, so z14 tiles stay
                # cheap to generate. z14 matches the live ORM site's max zoom
                # so MapLibre styles transferred from there look right.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-railway.pmtiles"
                query = f"""
                    SELECT
                      feature_id                                       AS osm_id,
                      geometry,
                      tags['railway']                               AS railway,
                      tags['public_transport']                      AS public_transport,
                      tags['usage']                                 AS usage,
                      tags['service']                               AS service,
                      tags['construction']                          AS construction,
                      tags['tunnel']                                AS tunnel,
                      tags['bridge']                                AS bridge,
                      tags['cutting']                               AS cutting,
                      tags['embankment']                            AS embankment,
                      tags['abandoned']                             AS abandoned,
                      tags['disused']                               AS disused,
                      tags['razed']                                 AS razed,
                      tags['proposed']                              AS proposed,
                      tags['man_made']                              AS man_made,
                      tags['power']                                 AS power,
                      tags['area']                                  AS area,
                      TRY_CAST(tags['layer'] AS INTEGER)            AS layer,
                      TRY_CAST(tags['ele'] AS DOUBLE)               AS ele,
                      tags['name']                                  AS name,
                      tags['ref']                                   AS ref,
                      tags['electrified']                           AS electrified,
                      TRY_CAST(tags['frequency'] AS DOUBLE)         AS frequency,
                      TRY_CAST(tags['voltage'] AS INTEGER)          AS voltage,
                      tags['deelectrified']                         AS deelectrified,
                      tags['construction:electrified']              AS construction_electrified,
                      TRY_CAST(tags['construction:frequency'] AS DOUBLE)  AS construction_frequency,
                      TRY_CAST(tags['construction:voltage'] AS INTEGER)   AS construction_voltage,
                      tags['proposed:electrified']                  AS proposed_electrified,
                      TRY_CAST(tags['proposed:frequency'] AS DOUBLE)      AS proposed_frequency,
                      TRY_CAST(tags['proposed:voltage'] AS INTEGER)       AS proposed_voltage,
                      tags['abandoned:electrified']                 AS abandoned_electrified,
                      tags['maxspeed']                              AS maxspeed,
                      tags['maxspeed:forward']                      AS maxspeed_forward,
                      tags['maxspeed:backward']                     AS maxspeed_backward,
                      tags['railway:preferred_direction']           AS preferred_direction,
                      tags['railway:position']                      AS railway_position,
                      tags['railway:position:detail']               AS railway_position_detail,
                      tags['railway:local_operated']                AS railway_local_operated,
                      tags['railway:signal:direction']              AS signal_direction,
                      tags['railway:signal:speed_limit']            AS signal_speed_limit,
                      tags['railway:signal:speed_limit:form']       AS signal_speed_limit_form,
                      tags['railway:signal:speed_limit:speed']      AS signal_speed_limit_speed,
                      tags['railway:signal:speed_limit_distant']    AS signal_speed_limit_distant,
                      tags['railway:signal:speed_limit_distant:form']  AS signal_speed_limit_distant_form,
                      tags['railway:signal:speed_limit_distant:speed'] AS signal_speed_limit_distant_speed,
                      CASE WHEN ST_GeometryType(geometry) IN ('POLYGON','MULTIPOLYGON')
                           THEN ST_Area(geometry) ELSE NULL END        AS way_area,
                      COALESCE(TRY_CAST(tags['layer'] AS INTEGER), 0) * 10
                        + CASE WHEN tags['tunnel'] IS NOT NULL THEN -10
                               WHEN tags['bridge'] IS NOT NULL THEN  10
                               ELSE 0 END
                        + CASE WHEN tags['railway'] = 'rail' THEN 5
                               WHEN tags['railway'] IN ('light_rail','subway','tram','narrow_gauge','monorail','funicular') THEN 3
                               WHEN tags['railway'] IN ('preserved','miniature') THEN 1
                               ELSE 0 END                              AS z_order
                    FROM read_parquet('{parquet_path}')
                    WHERE
                      tags['railway'] IS NOT NULL
                      OR tags['public_transport'] IN ('station','stop_position','platform','halt')
                      OR (tags['power'] = 'line' AND tags['line'] = 'busbar')
                      OR (tags['man_made'] IN ('mast','tower')
                          AND tags['tower:type'] = 'communication'
                          AND tags['railway'] IS NOT NULL)
                """
                # Inner-tile encoding: MVT (Mapbox Vector Tile, protobuf).
                # The original plan was tile_format="mlt" (MapLibre Tile spec)
                # for smaller line/polygon tiles. freestiler 0.1+ accepts the
                # kwarg AND emits valid MLT-inside-PMTiles, BUT martin v1.9.0
                # rejects the archive at startup with
                #   "Format Mlt and compression Gzip are not yet supported"
                # — a fatal warning that halts the entire tile server, not just
                # the unsupported source. Until martin grows MLT decode support
                # (upstream issue scope), MVT is the only encoding that survives
                # the tile-server boot path. Flip back to "mlt" once the next
                # martin release lands the decoder.
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-railway",
                    min_zoom=0,
                    max_zoom=14,
                )
                return str(out)

            @task
            def reload_martin(pmtiles_paths: list[str]) -> list[str]:
                # Identical sync primitives as the Monaco DAGs:
                #   1. flock — serializes the supervisorctl invocations
                #      so only one restart runs at a time globally.
                #   2. TCP readiness probe + /catalog membership check —
                #      verifies the END STATE (martin RUNNING + our
                #      source listed) instead of trusting supervisorctl's
                #      exit code (which can be non-zero even when martin
                #      ends up healthy).
                #
                # Takes a list of pmtiles paths to fan-in over the parallel
                # freestiler_convert + freestiler_railway_convert tasks. One
                # restart serves both sources.
                import fcntl
                import json as _json
                import socket
                import time as _time
                import urllib.request
                expected_sources = [
                    p.rsplit("/", 1)[-1].rsplit(".", 1)[0]
                    for p in pmtiles_paths
                ]
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
                available = sorted(_catalog.get("tiles", {}).keys())
                missing = [s for s in expected_sources if s not in available]
                if missing:
                    raise RuntimeError(
                        f"martin /catalog missing sources {missing} "
                        f"after reload; available={available}",
                    )
                return pmtiles_paths

            parquet = pbf_to_geoparquet(download_pbf())
            reload_martin([
                freestiler_convert(parquet),
                freestiler_railway_convert(parquet),
            ])


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
    style_layers: list | None = None,
    mlt: bool = False,
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

    When `style_layers` is provided it replaces the default
    fill/line/circle triplet (the background layer is kept). Each
    entry is a MapLibre style-layer dict passed through verbatim;
    `source: 'src'` and `source-layer: layer_name` are injected when
    absent so callers can omit them.

    `mlt=True` records the inner-encoding hint in the source
    declaration. Martin currently emits MVT bytes regardless of
    inner format; the flag is here so that when MapLibre GL JS adds
    MLT decode support upstream we can flip a single bit. The
    MapLibre version is captured in a DOM `data-maplibre-version`
    attribute so a CDP probe can assert the decoder side too.
    """
    import json as _json
    layer_prefix = source_name
    js_var = source_name.replace("-", "_")

    default_layers = [
        {"id": f"fill-{layer_prefix}", "type": "fill",
         "source": "src", "source-layer": layer_name,
         "filter": ["==", ["geometry-type"], "Polygon"],
         "paint": {"fill-color": "#a4c0a8",
                   "fill-outline-color": "#5e7060",
                   "fill-opacity": 0.55}},
        {"id": f"line-{layer_prefix}", "type": "line",
         "source": "src", "source-layer": layer_name,
         "filter": ["==", ["geometry-type"], "LineString"],
         "paint": {"line-color": "#3a3a3a", "line-width": 0.8}},
        {"id": f"circ-{layer_prefix}", "type": "circle",
         "source": "src", "source-layer": layer_name,
         "filter": ["==", ["geometry-type"], "Point"],
         "paint": {"circle-color": "#b04a3d", "circle-radius": 1.5}},
    ]
    data_layers = style_layers if style_layers is not None else default_layers
    # Inject source + source-layer defaults where the caller omitted
    # them — saves repetition in long style lists.
    for _layer in data_layers:
        if _layer.get("type") != "background":
            _layer.setdefault("source", "src")
            if _layer.get("type") != "background":
                _layer.setdefault("source-layer", layer_name)

    all_layers = [
        {"id": f"bg-{layer_prefix}", "type": "background",
         "paint": {"background-color": "#f6f3ec"}},
        *data_layers,
    ]
    layers_js = _json.dumps(all_layers, indent=2)
    source_dict = {"type": "vector", "url": f"{martin}/{source_name}"}
    if mlt:
        # Informational marker — the actual decode path depends on the
        # MapLibre GL JS version's MLT support; martin's content-type
        # response is what drives the decoder. Captured in the DOM via
        # data-mlt for the CDP probe.
        source_dict["mlt"] = True
    sources_js = _json.dumps({"src": source_dict})
    mlt_attr = ' data-mlt="true"' if mlt else ''
    return f"""<!DOCTYPE html>
<html><head>
<link href="https://unpkg.com/maplibre-gl@5.24.0/dist/maplibre-gl.css" rel="stylesheet"/>
<script src="https://unpkg.com/maplibre-gl@5.24.0/dist/maplibre-gl.js"></script>
<style>html,body{{margin:0;padding:0;}}#map-{layer_prefix}{{height:500px;width:100%;}}</style>
</head><body>
<div id="map-{layer_prefix}"{mlt_attr} data-maplibre-version=""></div>
<script>
document.getElementById('map-{layer_prefix}').dataset.maplibreVersion = maplibregl.version || '';
const map_{js_var} = new maplibregl.Map({{
  container: 'map-{layer_prefix}',
  style: {{
    version: 8,
    sources: {sources_js},
    layers: {layers_js}
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
def _(dag_run_states, martin, mo):
    # OpenRailwayMap-aligned render of the austria-railway PMTiles
    # archive (filtered to railway features only, projected to ORM's
    # tag schema, encoded as MLT inside PMTiles when freestiler
    # supports it — falls back to MVT transparently).
    #
    # Style mirrors the categorical color choices in OpenRailwayMap-
    # CartoCSS/standard.mss at a coarse level: mainline rail in blue,
    # branch lines in orange, urban transit (tram/light_rail/subway)
    # in purple, freight-yard service tracks in gray, stations as
    # filled circles. ORM's full electrification + signal styling
    # ladder would land here in a follow-up.
    mo.stop(
        dag_run_states.get("notebook_austria_pipeline") != "success",
        f"Waiting for notebook_austria_pipeline (state="
        f"{dag_run_states.get('notebook_austria_pipeline')!r})",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-railway",
            layer_name="austria-railway",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=[
                # Tunnels (rendered below everything else via order)
                {"id": "rail-tunnel", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["!=", ["get", "tunnel"], None]],
                 "paint": {"line-color": "#888888", "line-width": 1.0,
                           "line-dasharray": [2, 2]}},
                # Construction / proposed
                {"id": "rail-construction", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["!=", ["get", "construction"], None]],
                 "paint": {"line-color": "#aaaaaa", "line-width": 1.0,
                           "line-dasharray": [4, 2]}},
                # Abandoned / disused / razed
                {"id": "rail-disused", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["any",
                             ["!=", ["get", "abandoned"], None],
                             ["!=", ["get", "disused"], None],
                             ["!=", ["get", "razed"], None]]],
                 "paint": {"line-color": "#cccccc", "line-width": 0.8}},
                # Service tracks (sidings, yards, spurs)
                {"id": "rail-service", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["==", ["get", "railway"], "rail"],
                            ["!=", ["get", "service"], None]],
                 "paint": {"line-color": "#888888", "line-width": 0.8}},
                # Branch lines (rail without usage=main)
                {"id": "rail-branch", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["==", ["get", "railway"], "rail"],
                            ["!=", ["get", "usage"], "main"],
                            ["==", ["get", "service"], None]],
                 "paint": {"line-color": "#cc6633", "line-width": 1.2}},
                # Mainline rail (usage=main) — top of the line hierarchy
                {"id": "rail-main", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["==", ["get", "railway"], "rail"],
                            ["==", ["get", "usage"], "main"]],
                 "paint": {"line-color": "#3366cc", "line-width": 1.6}},
                # Urban transit
                {"id": "rail-transit", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["in", ["get", "railway"],
                             ["literal", ["tram", "light_rail", "subway", "monorail"]]]],
                 "paint": {"line-color": "#883388", "line-width": 1.0}},
                # Narrow gauge / funicular / preserved / miniature
                {"id": "rail-narrow", "type": "line",
                 "filter": ["all",
                            ["==", ["geometry-type"], "LineString"],
                            ["in", ["get", "railway"],
                             ["literal", ["narrow_gauge", "funicular", "preserved", "miniature"]]]],
                 "paint": {"line-color": "#5a8c2a", "line-width": 1.0}},
                # Stations + halts (point and polygon)
                {"id": "stations-fill", "type": "fill",
                 "filter": ["all",
                            ["==", ["geometry-type"], "Polygon"],
                            ["any",
                             ["==", ["get", "railway"], "station"],
                             ["==", ["get", "public_transport"], "station"]]],
                 "paint": {"fill-color": "#3366cc", "fill-opacity": 0.25,
                           "fill-outline-color": "#3366cc"}},
                {"id": "stations-pt", "type": "circle",
                 "filter": ["all",
                            ["==", ["geometry-type"], "Point"],
                            ["any",
                             ["==", ["get", "railway"], "station"],
                             ["==", ["get", "railway"], "halt"],
                             ["==", ["get", "public_transport"], "station"]]],
                 "paint": {"circle-color": "#3366cc",
                           "circle-radius": 4,
                           "circle-stroke-color": "#ffffff",
                           "circle-stroke-width": 1}},
                # Signals
                {"id": "signals", "type": "circle",
                 "filter": ["all",
                            ["==", ["geometry-type"], "Point"],
                            ["==", ["get", "railway"], "signal"]],
                 "paint": {"circle-color": "#cc3333",
                           "circle-radius": 2}},
            ],
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
