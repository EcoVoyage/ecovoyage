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

    return Path, mo, os, pl, requests, textwrap, time


@app.cell
def _resolved_urls(os, pl):
    # Diagnostic cell — single source of truth for every external URL
    # this notebook touches. Same shape as osm-austria.py / gtfs-austria.py
    # so the helper builders consume `martin` as a parameter (R3) without
    # re-reading os.environ in every downstream cell.
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
    versatiles_assets     = _resolved["VERSATILES_ASSETS_PUBLIC_URL"]
    urls
    return airflow_public, martin, versatiles_assets


@app.cell
def _(airflow_public, mo):
    mo.md(f"""
    # Austria GTFS — GPU-built, static-web route navigator

    Sibling to `gtfs-austria.py` (the CPU-only route-builder baseline).
    This notebook re-implements the route-builder map **GPU-natively +
    fully graph-based**:

    1. **Map runtime = pure JS + PMTiles.** The route-builder cell at
       the bottom emits a self-contained MapLibre HTML page; the user's
       browser does the routing entirely in-browser by reading a
       precomputed **hub-pair contraction-hierarchy table** baked into
       PMTiles. No Python kernel callbacks, no marimo round-trips. The
       same artifact is deployable as a static website (any CDN +
       MapLibre + pmtiles.js).
    2. **GPU acceleration = build-time only.** A self-authored Airflow
       DAG (`notebook_austria_graph_pipeline`) ingests the Transitous
       Austria GTFS feed via `cudf.read_csv`, builds a time-expanded
       routing graph in cuDF, runs `cugraph.sssp` from each hub to bake
       the optimal route between every (hub, hub, depart_window,
       weekday) tuple, and serialises the result into PMTiles. cuGraph
       SSSP is Pareto-correct by construction (transfer-count is part
       of node identity → the one-best-distance-per-node IS the Pareto
       frontier on (elapsed, nTr)).
    3. **1h depart windows.** Where `gtfs-austria.py` exposes 3 fixed
       8h bands (00–08 / 08–16 / 16–24), this notebook serves all 24
       1h windows. K=24 hubs × 24 windows × 2 weekdays produces ~14–18k
       baked hub-pair routes (~5 MB compressed PMTiles), single HTTP
       fetch.
    4. **R10 Transitous gate mirrored.** The
       `validate_routes_against_transitous` cell at the bottom runs the
       same 60-call structure as `gtfs-austria.py`'s gate, but the
       Python `_find_route` mirrors the **new** JS planner (hub-pair
       table + first/last-mile composer) instead of the CPU BFS. Cache
       + corpus + evidence paths use a `-graph` suffix to coexist with
       the CPU gate during the validation window.

    **Deploy contract**: this notebook DOES NOT depend on
    `gtfs-austria.py`'s outputs (DuckDB / austria-routehub PMTiles).
    Soft dependency on **`osm-austria.py`**'s `austria.parquet` — the
    OSM extract used by the `match_stops_to_osm` task to bind GTFS
    stops to OSM features. Trigger from <{airflow_public}> if cold.

    **Status now**: SKELETON. The DAG's tasks are stubs; the routing
    cell is not yet authored. Implementation progresses through the
    build order in the plan file.
    """)
    return


@app.cell
def _constants():
    # All tunable parameters in one place. Mirrors gtfs-austria.py:8090-8133
    # for the R10 params so the diff between the two gates is minimal.

    # Transitous Austria railway GTFS feed.
    GTFS_FEED_URL = (
        "https://api.transitous.org/gtfs/"
        "at_Railway-Current-Reference-Data-2026.gtfs.zip"
    )

    # 1h depart windows tiling the 24h GTFS service-day in Europe/Vienna
    # local time. Replaces gtfs-austria.py's 3 × 8h bands.
    WINDOWS = [(h * 3600, (h + 1) * 3600) for h in range(24)]
    WINDOW_LABELS = [f"{h:02d}-{(h+1):02d}" for h in range(24)]

    # Isochrone bands (hours from a hub origin departing at 08:00 local).
    ISOCHRONE_BANDS_HOURS = [1, 2, 3, 4, 5, 6, 8, 10, 12]

    # Hub-selection knobs — identical algebra to gtfs-austria.py:
    OPTIMAL_HUB_MIN = 8
    OPTIMAL_HUB_MAX = 40
    OPTIMAL_HUB_STOP_EPS = 0.005
    OPTIMAL_HUB_ANCHOR_WEIGHT = 1.0
    OPTIMAL_HUB_OBJ = "savings"   # BIG·M − Σ max(0, BIG − cand_cost)
    OPTIMAL_HUB_SAMPLE_M = 6000
    OPTIMAL_HUB_SEED = 20260515
    OPTIMAL_HUB_BIG_HOURS = 24    # sentinel for unroutable pairs

    # TEG construction params.
    TEG_MAX_TRANSFERS = 4         # structural layer count, RAPTOR-style
    TEG_TRANSFER_MIN_WAIT_S = 60  # below this, a transfer is implausible
    TEG_TRANSFER_MAX_WAIT_S = 3600

    # R10 Transitous gate.
    VAL_SEED = 20260515
    VAL_N = 20                    # OD pair sample size
    VAL_MAX_TRANSFERS = 4
    HARDFAIL_MIN_AHEAD_MIN = 60   # MOTIS faster by ≥60 min → HARD-FAIL
    SOFTFLAG_PCT = 20             # ±20% travel-time soft-flag band
    SOFTFLAG_TR_DELTA = 1
    VAL_MOTIS_OFFSETS_MIN = 20    # below this is a MOTIS OSM gap, not us

    # Default subset of WINDOWS used by the R10 gate to keep the API-call
    # budget aligned with gtfs-austria.py's 60-call gate (20 pairs × 3
    # representative windows). Operator can set R10_FULL_WINDOWS=1 to
    # expand the gate to all 24 windows (480 calls cold, warm-cached
    # after first run).
    VAL_WINDOWS_DEFAULT_HOURS = [7, 13, 19]    # morning peak / midday / evening peak

    # MOTIS prod endpoint by default; staging only useful for plumbing
    # tests (its OSM coverage is too sparse to return itineraries for
    # most coord-keyed /plan queries).
    MOTIS_BASE_PROD = "https://api.transitous.org/api/v5"
    MOTIS_BASE_STAGING = "https://staging.api.transitous.org/api/v5"

    # Cache + corpus paths. Distinct from gtfs-austria.py's `-graph`-less
    # paths so the two gates coexist during the validation window.
    R10_CACHE_DIR = "/workspace/.r10/transitous-cache-graph"
    R10_CORPUS_FILE = "/workspace/.r10/hardfail-corpus-graph.json"

    # TEG + DAG-output cache locations.
    GRAPH_CACHE_DIR = "/workspace/cache/austria-teg"

    return (
        GRAPH_CACHE_DIR,
        GTFS_FEED_URL,
        HARDFAIL_MIN_AHEAD_MIN,
        ISOCHRONE_BANDS_HOURS,
        MOTIS_BASE_PROD,
        MOTIS_BASE_STAGING,
        OPTIMAL_HUB_ANCHOR_WEIGHT,
        OPTIMAL_HUB_BIG_HOURS,
        OPTIMAL_HUB_MAX,
        OPTIMAL_HUB_MIN,
        OPTIMAL_HUB_OBJ,
        OPTIMAL_HUB_SAMPLE_M,
        OPTIMAL_HUB_SEED,
        OPTIMAL_HUB_STOP_EPS,
        R10_CACHE_DIR,
        R10_CORPUS_FILE,
        SOFTFLAG_PCT,
        SOFTFLAG_TR_DELTA,
        TEG_MAX_TRANSFERS,
        TEG_TRANSFER_MAX_WAIT_S,
        TEG_TRANSFER_MIN_WAIT_S,
        VAL_MAX_TRANSFERS,
        VAL_MOTIS_OFFSETS_MIN,
        VAL_N,
        VAL_SEED,
        VAL_WINDOWS_DEFAULT_HOURS,
        WINDOWS,
        WINDOW_LABELS,
    )


@app.function
# KEEP IN SYNC with notebooks/gtfs-austria.py and notebooks/osm-austria.py.
# R3 trade-off acknowledged: cross-notebook DRY would extract this to
# notebooks/_lib_austria.py, but marimo notebooks are conventionally
# self-contained .py files. Drift risk is low — the kwargs surface is
# stable and the three copies are kept identical.
def build_pipeline_maplibre_html(
    martin: str,
    source_name: str,
    *,
    layer_name: str,
    center: list,
    zoom: int,
    style_layers: list | None = None,
    extra_sources: dict | None = None,
    extra_layers: list | None = None,
    mlt: bool = False,
    source_maxzoom: int = 14,
    terrain: bool = False,
    satellite_background: bool = False,
    pitch: int = 0,
    max_pitch: int = 60,
    hillshade: bool = True,
    glyphs_url: str | None = None,
    extra_js: str | None = None,
) -> str:
    """MapLibre HTML template for a martin vector-tile source.

    See gtfs-austria.py:4357-4573 for the full docstring. Verbatim port
    so the JS contract (window.map_<var> hook, source `src`, glyphs
    template) is identical across the three Austria notebooks. Default
    source_maxzoom=14 matches the freestiler bakes this notebook
    produces (austria-graph-routes, austria-graph-hubpairs,
    austria-graph-isochrones all z0–14).
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
    raw_layers = style_layers if style_layers is not None else default_layers
    data_layers = []
    for _layer in raw_layers:
        _layer = dict(_layer)
        if _layer.get("type") != "background" and _layer.get("source", "src") == "src":
            _layer["source"] = "src"
            _layer["source-layer"] = layer_name
        data_layers.append(_layer)

    base_layers = []
    if satellite_background:
        base_layers.append({
            "id": f"satellite-bg-{layer_prefix}",
            "type": "raster",
            "source": "satellite-src",
        })
    else:
        base_layers.append({
            "id": f"bg-{layer_prefix}",
            "type": "background",
            "paint": {"background-color": "#f6f3ec"},
        })
    if terrain and hillshade:
        base_layers.append({
            "id": f"hills-{layer_prefix}",
            "type": "hillshade",
            "source": "hillshadeSource",
            "paint": {
                "hillshade-shadow-color": "#473B24",
                "hillshade-exaggeration": 0.5,
            },
        })

    all_layers = [
        *base_layers,
        *data_layers,
        *(extra_layers or []),
    ]
    layers_js = _json.dumps(all_layers, indent=2)
    source_dict = {
        "type": "vector",
        "url": f"{martin}/{source_name}",
        "maxzoom": source_maxzoom,
    }
    if mlt:
        source_dict["mlt"] = True
    all_sources = {"src": source_dict, **(extra_sources or {})}
    if satellite_background:
        all_sources["satellite-src"] = {
            "type": "raster",
            "tiles": ["https://tiles.versatiles.org/tiles/satellite/{z}/{x}/{y}"],
            "tileSize": 256,
            "minzoom": 0,
            "maxzoom": 17,
            "attribution": "<a href='https://versatiles.org/sources/'>VersaTiles sources</a>",
        }
    if terrain:
        _dem = {
            "type": "raster-dem",
            "url": "https://tiles.mapterhorn.com/tilejson.json",
        }
        all_sources["terrainSource"] = _dem
        if hillshade:
            all_sources["hillshadeSource"] = _dem
    sources_js = _json.dumps(all_sources)
    mlt_attr = ' data-mlt="true"' if mlt else ''

    terrain_extras_js = ""
    pitch_js = ""
    terrain_control_js = ""
    if terrain:
        terrain_extras_js = (
            ",\n    terrain: { source: 'terrainSource', exaggeration: 1.5 },"
            "\n    sky: {}"
        )
        pitch_js = f"  pitch: {pitch},\n  maxPitch: {max_pitch},\n"
        terrain_control_js = (
            f"\nmap_{js_var}.addControl(new maplibregl.TerrainControl"
            f"({{ source: 'terrainSource', exaggeration: 1.5 }}), 'top-right');"
        )

    glyphs_js = f'    glyphs: "{glyphs_url}",\n' if glyphs_url else ""
    extra_js_block = f"\n{extra_js}" if extra_js else ""

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
{glyphs_js}    sources: {sources_js},
    layers: {layers_js}{terrain_extras_js}
  }},
  center: [{center[0]}, {center[1]}],
  zoom: {zoom},
{pitch_js}  attributionControl: false
}});
map_{js_var}.addControl(new maplibregl.NavigationControl({{ showZoom: true, showCompass: true }}), 'top-right');{terrain_control_js}
window.map_{js_var} = map_{js_var};{extra_js_block}
</script>
</body></html>"""


@app.function
def with_theme(theme: str, layers: list) -> list:
    """Prepends a theme-equality clause to each style-layer's filter.
    KEEP IN SYNC with notebooks/gtfs-austria.py and osm-austria.py."""
    result = []
    theme_clause = ["==", ["get", "theme"], theme]
    for layer in layers:
        new_layer = {**layer, "id": f"evo-{layer['id']}"}
        old_filter = layer.get("filter")
        if old_filter is None:
            new_layer["filter"] = theme_clause
        elif isinstance(old_filter, list) and old_filter and old_filter[0] == "all":
            new_layer["filter"] = ["all", theme_clause, *old_filter[1:]]
        else:
            new_layer["filter"] = ["all", theme_clause, old_filter]
        result.append(new_layer)
    return result


@app.cell
def _author_dag(Path, os, textwrap):
    # Self-author the GPU-graph pipeline DAG. Idempotent — overwriting
    # on every notebook run keeps the DAG body in sync with this
    # notebook (single source of truth: this cell IS the DAG spec).
    # Distinct dag_id from the sibling gtfs-austria.py DAG; both can
    # coexist in /workspace/dags/.
    #
    # SKELETON STATE: every task body is a `pass`-equivalent log line so
    # the DAG round-trips green. Each task is filled in progressively
    # through the build order (see plan file).
    dags_dir = Path(os.environ.get(
        "AIRFLOW_DAGS_DIR",
        os.path.expanduser("/workspace/dags"),
    ))
    dags_dir.mkdir(parents=True, exist_ok=True)

    graph_dag_id = "notebook_austria_graph_pipeline"
    graph_dag_file = dags_dir / f"{graph_dag_id}.py"
    graph_dag_file.write_text(textwrap.dedent('''
        """Austria GPU-graph route-builder pipeline self-authored by
        notebooks/gtfs-graph.py.

        SKELETON. Each task body is a stub; full implementation is
        progressed through the build order in the plan file at
        /home/atrawog/.claude/plans/can-you-check-gpu-libraries-demo-py-breezy-owl.md.

        Pipeline shape:

            download_gtfs
              └─ parse_gtfs
                  └─ match_stops_to_osm
                      └─ build_teg
                          ├─ compute_optimal_hubs_gpu
                          │   ├─ compute_hub_pair_routes_gpu
                          │   │   └─ bake_hubpairs_pmtiles
                          │   ├─ compute_isochrones_gpu
                          │   │   └─ bake_isochrones_pmtiles
                          │   └─ compute_routehub_dataset
                          │       └─ bake_routes_pmtiles

        Schedule: @monthly + max_active_runs=1. Manual triggers via
        the notebook's REST trigger cell adopt the most recent
        same-month success run (idempotent re-runs are free).
        """
        import logging
        import os
        from datetime import datetime, timedelta, timezone
        from pathlib import Path

        from airflow.sdk import dag, task

        log = logging.getLogger(__name__)

        # Cache layout — every task persists artefacts under this root
        # so the in-kernel route-builder cell can reload them without
        # re-running the DAG.
        CACHE = Path("/workspace/cache/austria-teg")
        GTFS_RAW = CACHE / "raw"            # downloaded feed .zip
        GTFS_PARQUET = CACHE / "gtfs"       # cudf-parsed .txt → parquet
        TRANSIT = CACHE / "transit"         # stations + hubs + hub-pair routes
        TEG = CACHE / "teg"                 # edges + nodes parquet
        ISO = CACHE / "isochrones"          # per-hub band rings

        # Output tiles — martin auto-discovers under /workspace/tiles/pmtiles.
        TILES_OUT = Path("/workspace/tiles/pmtiles")
        TILES_WORK = Path("/workspace/tiles/work")

        # Soft dependency on osm-austria.py output. Read-only; we
        # NEVER re-derive austria.parquet ourselves.
        OSM_PARQUET = TILES_WORK / "austria.parquet"

        # Transitous Austria railway GTFS feed.
        GTFS_FEED_URL = (
            "https://api.transitous.org/gtfs/"
            "at_Railway-Current-Reference-Data-2026.gtfs.zip"
        )

        # Mirrors gtfs-austria.py's notebook-side constants used by the
        # 3-tier OSM stop match. KEEP IN SYNC.
        _AT_FEED_CODE = "AT-Transitous"
        _TRANSIT_WHERE = """tags['railway'] IN ('station','stop','halt','tram_stop','subway_entrance')
                      OR tags['public_transport'] IN ('stop_position','platform','station')
                      OR tags['highway'] = 'bus_stop'
                      OR tags['amenity'] = 'ferry_terminal'"""
        _STATION_ANCHOR_WHERE = """tags['railway'] IN ('station', 'halt')
                      OR tags['public_transport'] = 'station'"""
        _STATION_SNAP_DEG = 0.002695        # ~300 m at AT latitude
        _NAME_CLUSTER_SPAN_DEG = 0.006      # ~0.7 km at AT latitude
        _GENERIC_NAME_SET = (
            "'hauptbahnhof', 'bahnhof', 'bahnhst', 'bahnhst.', 'hbf', "
            "'bf', 'bf.', 'station', 'bahnsteig'"
        )


        def _needs_regen(path: Path) -> bool:
            """Monthly mtime cache. Returns True iff path is missing OR
            its mtime falls outside the current calendar month
            (Europe/Vienna). Mirrors gtfs-austria.py's smart-download
            policy — ad-hoc re-runs within a month skip the work."""
            if not path.exists():
                return True
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            return (mtime.year, mtime.month) != (now.year, now.month)


        @dag(
            dag_id="notebook_austria_graph_pipeline",
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            schedule="@monthly",
            catchup=False,
            max_active_runs=1,
            default_args={"retries": 2, "retry_delay": timedelta(seconds=60)},
            tags=["austria", "graph", "gpu"],
        )
        def notebook_austria_graph_pipeline():
            @task
            def download_gtfs() -> str:
                """Fetch the Transitous Austria railway GTFS .zip.
                Monthly mtime cache — re-runs within the same calendar
                month short-circuit. ~770 KB on the wire."""
                import shutil
                import urllib.request
                GTFS_RAW.mkdir(parents=True, exist_ok=True)
                out = GTFS_RAW / "austria.gtfs.zip"
                if not _needs_regen(out):
                    log.info("download_gtfs: cached %s", out)
                    return str(out)
                tmp = out.with_suffix(".zip.part")
                try:
                    with urllib.request.urlopen(GTFS_FEED_URL, timeout=300) as resp:
                        with open(tmp, "wb") as f:
                            shutil.copyfileobj(resp, f)
                    tmp.replace(out)
                finally:
                    if tmp.exists():
                        tmp.unlink()
                log.info("download_gtfs: fetched %s (%d bytes)", out, out.stat().st_size)
                return str(out)

            @task
            def parse_gtfs(zip_path: str) -> str:
                """Unzip + cuDF-parse every .txt into one parquet per
                table. GTFS is an all-string format; we read with
                dtype="str" so cuDF's CSV parser handles whatever the
                feed shipped without dtype inference surprises. Tables
                kept: stops / trips / stop_times / routes / calendar /
                calendar_dates / agency / transfers (when present).
                shapes.txt is skipped — large + unused for routing.
                stops.parquet's mtime is the freshness canary."""
                import zipfile
                import cudf
                GTFS_PARQUET.mkdir(parents=True, exist_ok=True)
                if not _needs_regen(GTFS_PARQUET / "stops.parquet"):
                    log.info("parse_gtfs: cached %s", GTFS_PARQUET)
                    return str(GTFS_PARQUET)
                # Keep extraction in a scratch dir under raw/ so the
                # raw .zip and the unzipped tree are colocated and
                # consistent under monthly cache.
                extract_dir = GTFS_RAW / "extracted"
                if extract_dir.exists():
                    import shutil
                    shutil.rmtree(extract_dir)
                extract_dir.mkdir(parents=True)
                with zipfile.ZipFile(zip_path) as zf:
                    zf.extractall(extract_dir)
                wanted = {
                    "stops", "trips", "stop_times", "routes",
                    "calendar", "calendar_dates", "agency", "transfers",
                }
                # Wipe stale parquet so a feed that dropped a table
                # never leaves a stale read-back.
                for old in GTFS_PARQUET.glob("*.parquet"):
                    old.unlink()
                loaded = []
                for txt in sorted(extract_dir.glob("*.txt")):
                    name = txt.stem
                    if name not in wanted:
                        continue
                    # All-string dtype mirrors the GTFS spec — every
                    # field is a string, even numeric-looking ones like
                    # stop_lat / stop_lon. Downstream tasks cast as
                    # needed (DuckDB casts in SQL; cuGraph build casts
                    # via cuDF.to_numeric).
                    df = cudf.read_csv(str(txt), dtype="str")
                    out = GTFS_PARQUET / f"{name}.parquet"
                    df.to_parquet(str(out), compression="snappy")
                    loaded.append((name, len(df)))
                log.info(
                    "parse_gtfs: loaded %d tables: %s",
                    len(loaded),
                    ", ".join(f"{n}({r})" for n, r in loaded),
                )
                return str(GTFS_PARQUET)

            @task(retries=20, retry_delay=timedelta(seconds=60))
            def match_stops_to_osm(parquet_dir: str) -> str:
                """3-tier OSM stop match + 4-tier station rollup,
                identical algebra to gtfs-austria.py:1098-1635 — same
                SQL, same _GENERIC_NAME_SET, same _STATION_SNAP_DEG, so
                the station_feature_id output is bit-equivalent.

                Run on an EPHEMERAL in-memory DuckDB (no persistent
                file at /workspace/duckdb/austria.duckdb — that's
                gtfs-austria.py's domain). The 13M-feature
                austria.parquet is read as a zero-copy VIEW; cudf
                parquet outputs are read as DuckDB tables. ~30-60 s
                on cold data.

                20×60s retries cover a cold osm-austria.py rebuild
                that hasn't produced austria.parquet yet."""
                import duckdb
                TRANSIT.mkdir(parents=True, exist_ok=True)
                if not OSM_PARQUET.exists() or _needs_regen(OSM_PARQUET):
                    raise RuntimeError(
                        f"austria.parquet missing or stale at "
                        f"{OSM_PARQUET} — Airflow will retry while "
                        "osm-austria.py builds it"
                    )
                stations_out = TRANSIT / "stations.parquet"
                if not _needs_regen(stations_out):
                    log.info("match_stops_to_osm: cached %s", stations_out)
                    return str(stations_out)

                gtfs_dir = Path(parquet_dir)
                stops_pq = gtfs_dir / "stops.parquet"
                stop_times_pq = gtfs_dir / "stop_times.parquet"
                trips_pq = gtfs_dir / "trips.parquet"
                routes_pq = gtfs_dir / "routes.parquet"
                for p in (stops_pq, stop_times_pq, trips_pq, routes_pq):
                    if not p.exists():
                        raise FileNotFoundError(
                            f"required cuDF-parsed parquet missing: {p}"
                        )

                con = duckdb.connect()  # in-memory; no on-disk DB file
                con.sql("INSTALL spatial; LOAD spatial;")
                # OSM extract as a VIEW (zero-copy).
                con.sql(
                    "CREATE OR REPLACE VIEW osm_features AS "
                    f"SELECT * FROM read_parquet('{OSM_PARQUET}')"
                )
                # GTFS as TABLES — cuDF's parquet output is read fine
                # by DuckDB. Cast lat/lon to double up front so the SQL
                # is identical to gtfs-austria.py's (which gets numeric
                # columns from gtfs-parquet).
                con.sql(
                    "CREATE OR REPLACE TABLE gtfs_stops AS "
                    f"SELECT * EXCLUDE (stop_lat, stop_lon),"
                    f"       CAST(stop_lat AS DOUBLE) AS stop_lat,"
                    f"       CAST(stop_lon AS DOUBLE) AS stop_lon "
                    f"  FROM read_parquet('{stops_pq}')"
                )
                con.sql(
                    "CREATE OR REPLACE TABLE gtfs_stop_times AS "
                    f"SELECT * FROM read_parquet('{stop_times_pq}')"
                )
                con.sql(
                    "CREATE OR REPLACE TABLE gtfs_trips AS "
                    f"SELECT * FROM read_parquet('{trips_pq}')"
                )
                con.sql(
                    "CREATE OR REPLACE TABLE gtfs_routes AS "
                    f"SELECT * FROM read_parquet('{routes_pq}')"
                )

                con.sql(f"""
                    CREATE OR REPLACE TABLE osm_stop_features AS
                    SELECT feature_id,
                           geometry,
                           ST_X(ST_Centroid(geometry)) AS lon,
                           ST_Y(ST_Centroid(geometry)) AS lat,
                           tags
                    FROM osm_features
                    WHERE {_TRANSIT_WHERE}
                """)
                # 3-tier match — same algebra as gtfs-austria.py.
                con.sql(f"""
                    CREATE OR REPLACE TABLE matched_stops AS
                    WITH
                      tag_match AS (
                        SELECT s.stop_id, o.feature_id AS osm_feature_id,
                               'gtfs:stop_id' AS match_kind, 0.0 AS match_distance_m
                        FROM gtfs_stops s
                        JOIN osm_stop_features o
                          ON o.tags['gtfs:stop_id:{_AT_FEED_CODE}'] = s.stop_id
                      ),
                      ifopt_match AS (
                        SELECT s.stop_id, o.feature_id AS osm_feature_id,
                               'ref:IFOPT' AS match_kind, 0.0 AS match_distance_m
                        FROM gtfs_stops s
                        JOIN osm_stop_features o
                          ON o.tags['ref:IFOPT'] = s.stop_id
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tag_match)
                      ),
                      spatial_last_resort AS (
                        SELECT s.stop_id, o.feature_id AS osm_feature_id,
                               'spatial_last_resort' AS match_kind,
                               ST_Distance(
                                   ST_Point(s.stop_lon, s.stop_lat),
                                   ST_Point(o.lon, o.lat)
                               ) AS match_distance_m
                        FROM gtfs_stops s
                        JOIN osm_stop_features o
                          ON ST_DWithin(
                                 ST_Point(s.stop_lon, s.stop_lat),
                                 ST_Point(o.lon, o.lat),
                                 0.00045
                             )
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tag_match)
                          AND s.stop_id NOT IN (SELECT stop_id FROM ifopt_match)
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY s.stop_id ORDER BY match_distance_m
                        ) = 1
                      )
                    SELECT * FROM tag_match
                    UNION ALL SELECT * FROM ifopt_match
                    UNION ALL SELECT * FROM spatial_last_resort
                """)

                # 4-tier station rollup → transit/stations.parquet.
                # Bit-identical SQL to gtfs-austria.py:1231-1633.
                con.sql(f"""
                    CREATE OR REPLACE TABLE station_members AS
                    WITH
                      anchors AS (
                        SELECT feature_id,
                               tags['name'] AS station_name,
                               tags['uic_ref'] AS uic_ref,
                               ST_X(ST_Centroid(geometry)) AS lon,
                               ST_Y(ST_Centroid(geometry)) AS lat
                        FROM osm_features
                        WHERE {_STATION_ANCHOR_WHERE}
                      ),
                      best_match AS (
                        SELECT stop_id, osm_feature_id, match_kind
                        FROM matched_stops
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY stop_id
                            ORDER BY CASE match_kind
                                       WHEN 'gtfs:stop_id' THEN 0
                                       WHEN 'ref:IFOPT' THEN 1
                                       ELSE 2 END,
                                     osm_feature_id
                        ) = 1
                      ),
                      parent_anchor AS (
                        SELECT ps.stop_id   AS parent_stop_id,
                               ps.stop_name AS parent_name,
                               ps.stop_lon  AS parent_lon,
                               ps.stop_lat  AS parent_lat,
                               a.feature_id   AS anchor_feature_id,
                               a.station_name AS anchor_name,
                               a.lon AS anchor_lon,
                               a.lat AS anchor_lat
                        FROM gtfs_stops ps
                        LEFT JOIN best_match pbm ON pbm.stop_id = ps.stop_id
                        LEFT JOIN anchors a
                               ON a.feature_id = pbm.osm_feature_id
                               OR ST_DWithin(
                                      ST_Point(ps.stop_lon, ps.stop_lat),
                                      ST_Point(a.lon, a.lat),
                                      {_STATION_SNAP_DEG}
                                  )
                        WHERE ps.stop_id IN (
                            SELECT DISTINCT parent_station FROM gtfs_stops
                            WHERE NULLIF(parent_station, '') IS NOT NULL
                        )
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY ps.stop_id
                            ORDER BY
                                CASE WHEN a.feature_id = pbm.osm_feature_id
                                     THEN 0 ELSE 1 END,
                                ST_Distance(
                                    ST_Point(ps.stop_lon, ps.stop_lat),
                                    ST_Point(COALESCE(a.lon, ps.stop_lon),
                                             COALESCE(a.lat, ps.stop_lat))
                                ),
                                a.feature_id
                        ) = 1
                      ),
                      tier1 AS (
                        SELECT s.stop_id,
                               COALESCE(pa.anchor_feature_id,
                                        'gtfs/' || s.parent_station) AS station_feature_id,
                               CASE
                                 WHEN lower(trim(pa.anchor_name))
                                      IN ({_GENERIC_NAME_SET})
                                 THEN COALESCE(pa.parent_name, pa.anchor_name)
                                 ELSE COALESCE(pa.anchor_name, pa.parent_name)
                               END AS station_name,
                               COALESCE(pa.anchor_lon, pa.parent_lon) AS station_lon,
                               COALESCE(pa.anchor_lat, pa.parent_lat) AS station_lat,
                               'gtfs_parent' AS resolution_kind
                        FROM gtfs_stops s
                        LEFT JOIN parent_anchor pa ON pa.parent_stop_id = s.parent_station
                        WHERE NULLIF(s.parent_station, '') IS NOT NULL
                      ),
                      tier2 AS (
                        SELECT s.stop_id,
                               a.feature_id AS station_feature_id,
                               a.station_name,
                               a.lon AS station_lon,
                               a.lat AS station_lat,
                               'uic_ref' AS resolution_kind
                        FROM gtfs_stops s
                        JOIN best_match bm ON bm.stop_id = s.stop_id
                        JOIN osm_features of ON of.feature_id = bm.osm_feature_id
                        JOIN anchors a
                          ON a.uic_ref = of.tags['uic_ref']
                         AND NULLIF(of.tags['uic_ref'], '') IS NOT NULL
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL)
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY s.stop_id ORDER BY a.feature_id
                        ) = 1
                      ),
                      tier3 AS (
                        SELECT s.stop_id,
                               a.feature_id AS station_feature_id,
                               a.station_name,
                               a.lon AS station_lon,
                               a.lat AS station_lat,
                               'spatial' AS resolution_kind
                        FROM gtfs_stops s
                        JOIN anchors a
                          ON ST_DWithin(
                                 ST_Point(s.stop_lon, s.stop_lat),
                                 ST_Point(a.lon, a.lat),
                                 {_STATION_SNAP_DEG}
                             )
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL)
                          AND s.stop_id NOT IN (SELECT stop_id FROM tier2 WHERE stop_id IS NOT NULL)
                          AND s.stop_lon IS NOT NULL
                          AND s.stop_lat IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY s.stop_id
                            ORDER BY ST_Distance(
                                ST_Point(s.stop_lon, s.stop_lat),
                                ST_Point(a.lon, a.lat)
                            ), a.feature_id
                        ) = 1
                      ),
                      t3b_residual AS (
                        SELECT s.stop_id, s.stop_name, s.stop_lon, s.stop_lat,
                               lower(trim(s.stop_name)) AS name_key
                        FROM gtfs_stops s
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL)
                          AND s.stop_id NOT IN (SELECT stop_id FROM tier2 WHERE stop_id IS NOT NULL)
                          AND s.stop_id NOT IN (SELECT stop_id FROM tier3 WHERE stop_id IS NOT NULL)
                          AND s.stop_lon IS NOT NULL
                          AND s.stop_lat IS NOT NULL
                          AND NULLIF(trim(s.stop_name), '') IS NOT NULL
                          AND lower(trim(s.stop_name)) NOT IN ({_GENERIC_NAME_SET})
                      ),
                      t3b_groups AS (
                        SELECT name_key, count(*) AS n_stops,
                               min(stop_lon) AS min_lon, max(stop_lon) AS max_lon,
                               min(stop_lat) AS min_lat, max(stop_lat) AS max_lat,
                               avg(stop_lon) AS centroid_lon,
                               avg(stop_lat) AS centroid_lat
                        FROM t3b_residual
                        GROUP BY name_key
                        HAVING count(*) >= 2
                           AND max(stop_lon) - min(stop_lon) <= {_NAME_CLUSTER_SPAN_DEG}
                           AND max(stop_lat) - min(stop_lat) <= {_NAME_CLUSTER_SPAN_DEG}
                      ),
                      tier3b AS (
                        SELECT r.stop_id,
                               'gtfs/N:' || md5(
                                   g.name_key || ':'
                                   || round(g.centroid_lon, 3) || ':'
                                   || round(g.centroid_lat, 3)
                               ) AS station_feature_id,
                               r.stop_name AS station_name,
                               g.centroid_lon AS station_lon,
                               g.centroid_lat AS station_lat,
                               'name_cluster' AS resolution_kind
                        FROM t3b_residual r
                        JOIN t3b_groups g USING (name_key)
                      ),
                      tier4 AS (
                        SELECT s.stop_id,
                               s.stop_id AS station_feature_id,
                               s.stop_name AS station_name,
                               s.stop_lon AS station_lon,
                               s.stop_lat AS station_lat,
                               'self' AS resolution_kind
                        FROM gtfs_stops s
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL)
                          AND s.stop_id NOT IN (SELECT stop_id FROM tier2 WHERE stop_id IS NOT NULL)
                          AND s.stop_id NOT IN (SELECT stop_id FROM tier3 WHERE stop_id IS NOT NULL)
                          AND s.stop_id NOT IN (SELECT stop_id FROM tier3b WHERE stop_id IS NOT NULL)
                      ),
                      resolved AS (
                        SELECT * FROM tier1
                        UNION ALL SELECT * FROM tier2
                        UNION ALL SELECT * FROM tier3
                        UNION ALL SELECT * FROM tier3b
                        UNION ALL SELECT * FROM tier4
                      ),
                      stop_calls AS (
                        SELECT stop_id, count(*) AS n_calls
                        FROM gtfs_stop_times
                        GROUP BY stop_id
                      ),
                      name_calls AS (
                        SELECT r.station_feature_id,
                               COALESCE(s.stop_name, '') AS cand_name,
                               sum(COALESCE(sc.n_calls, 0)) AS total_calls
                        FROM resolved r
                        JOIN gtfs_stops s USING (stop_id)
                        LEFT JOIN stop_calls sc USING (stop_id)
                        GROUP BY r.station_feature_id, s.stop_name
                      ),
                      station_name_final AS (
                        SELECT station_feature_id,
                               COALESCE(
                                   arg_max(cand_name, total_calls) FILTER (
                                       WHERE lower(trim(cand_name)) NOT IN ({_GENERIC_NAME_SET})
                                         AND NULLIF(trim(cand_name), '') IS NOT NULL
                                   ),
                                   arg_max(cand_name, total_calls)
                               ) AS station_name
                        FROM name_calls
                        GROUP BY station_feature_id
                      ),
                      rail_served AS (
                        SELECT DISTINCT r.station_feature_id
                        FROM gtfs_stop_times st
                        JOIN gtfs_trips t USING (trip_id)
                        JOIN gtfs_routes rt USING (route_id)
                        JOIN resolved r ON r.stop_id = st.stop_id
                        WHERE rt.route_type = '2'
                      )
                    SELECT r.stop_id,
                           r.station_feature_id,
                           snf.station_name,
                           r.station_lon,
                           r.station_lat,
                           r.resolution_kind,
                           CASE WHEN rs.station_feature_id IS NOT NULL
                                THEN 'true' ELSE 'false'
                                END AS is_rail_served
                    FROM resolved r
                    JOIN station_name_final snf USING (station_feature_id)
                    LEFT JOIN rail_served rs USING (station_feature_id)
                """)

                # Per-station table (one row per station_feature_id) —
                # the canonical schema the TEG builder + R10 gate
                # consume. station_members above is one-row-per-stop_id
                # so we GROUP BY to collapse to per-station grain.
                # DuckDB 1.5's `.arrow()` returns a RecordBatchReader,
                # not a pyarrow.Table — use fetch_arrow_table() for the
                # in-memory Table that pyarrow.parquet.write_table
                # accepts.
                stations_tbl = con.sql("""
                    SELECT
                        station_feature_id,
                        any_value(station_name) AS station_name,
                        any_value(station_lon)  AS station_lon,
                        any_value(station_lat)  AS station_lat,
                        any_value(is_rail_served) AS is_rail_served,
                        list(DISTINCT stop_id)  AS member_stop_ids
                    FROM station_members
                    GROUP BY station_feature_id
                """).fetch_arrow_table()
                stations_out.parent.mkdir(parents=True, exist_ok=True)
                import pyarrow.parquet as papq
                papq.write_table(stations_tbl, stations_out, compression="snappy")
                stop_members_path = TRANSIT / "stop_station_members.parquet"
                papq.write_table(
                    con.sql("SELECT * FROM station_members").fetch_arrow_table(),
                    stop_members_path,
                    compression="snappy",
                )

                rates = con.sql("""
                    SELECT
                        count(*) FILTER (WHERE resolution_kind='gtfs_parent')  AS by_parent,
                        count(*) FILTER (WHERE resolution_kind='uic_ref')      AS by_uic,
                        count(*) FILTER (WHERE resolution_kind='spatial')      AS by_spatial,
                        count(*) FILTER (WHERE resolution_kind='name_cluster') AS by_name_cluster,
                        count(*) FILTER (WHERE resolution_kind='self')         AS by_self,
                        count(DISTINCT station_feature_id) FILTER (
                            WHERE is_rail_served='true')   AS rail_served_st,
                        count(DISTINCT station_feature_id) AS total_st
                    FROM station_members
                """).fetchone()
                log.info(
                    "match_stops_to_osm: rollup by_parent=%d by_uic=%d "
                    "by_spatial=%d by_name_cluster=%d by_self=%d; "
                    "rail-served stations=%d of %d",
                    *rates,
                )
                con.close()
                return str(stations_out)

            @task
            def build_teg(stations_path: str, gtfs_parquet_dir: str) -> str:
                """Build the static (transfer-edge-less) time-expanded
                graph. Produces:

                - teg/serviced_stop_times.parquet — rail-only stop_times
                  resolved to station_feature_id, with arr_s/dep_s as
                  seconds-since-midnight ints and a 7-bit runs_dow
                  bitmask (Mon=0 .. Sun=6) per trip from calendar.txt.
                  This is the canonical "rail timetable resolved to
                  station grain" — every downstream task reads it.

                - teg/nodes.parquet — (node_idx, kind, trip_idx, seq,
                  sfid, nTr). For now nTr=0 only; transfer-layer
                  replication happens in compute_hub_pair_routes_gpu
                  once the hub set is committed.

                - teg/edges.parquet — (src_idx, dst_idx, weight_s,
                  edge_kind, dep_s). edge_kind ∈ {0:ride, 1:alight,
                  2:board}. board edges carry their raw dep_s so the
                  per-window filter can be applied at SSSP time
                  without rebuilding the edge list.

                All on GPU via cuDF + CuPy. Austria rail timetable
                fits comfortably in ~200 MB GPU memory."""
                import cudf
                import cupy as cp
                import math

                TEG.mkdir(parents=True, exist_ok=True)
                serviced_path = TEG / "serviced_stop_times.parquet"
                edges_path = TEG / "edges.parquet"
                nodes_path = TEG / "nodes.parquet"
                if not (_needs_regen(serviced_path)
                        or _needs_regen(edges_path)
                        or _needs_regen(nodes_path)):
                    log.info("build_teg: cached %s", TEG)
                    return str(TEG)

                gtfs_dir = Path(gtfs_parquet_dir)
                # cudf.read_parquet with dtype="str" (implicit from the
                # parse_gtfs writer) gives all-string columns; cast
                # numeric / time fields explicitly.
                trips = cudf.read_parquet(str(gtfs_dir / "trips.parquet"))
                routes = cudf.read_parquet(str(gtfs_dir / "routes.parquet"))
                stop_times = cudf.read_parquet(str(gtfs_dir / "stop_times.parquet"))
                calendar = cudf.read_parquet(str(gtfs_dir / "calendar.parquet"))

                # Filter to rail trips (route_type == "2").
                rail_routes = routes.loc[routes["route_type"] == "2", ["route_id", "route_short_name", "route_long_name"]]
                rail_trips = trips.merge(rail_routes, on="route_id", how="inner")
                # COALESCE short_name → long_name → route_id (matches
                # gtfs-austria.py:3739-3743).
                rail_trips["route_short_name"] = rail_trips["route_short_name"].fillna("").replace("", None)
                rail_trips["route_long_name"]  = rail_trips["route_long_name"].fillna("").replace("", None)
                rail_trips["rsn"] = (
                    rail_trips["route_short_name"]
                        .fillna(rail_trips["route_long_name"])
                        .fillna(rail_trips["route_id"])
                )

                # 7-bit runs_dow bitmask from calendar.txt: Mon=bit 0..Sun=bit 6.
                # Trips with no calendar entry (calendar_dates-only) default to
                # all days (0x7F) — same fallback as gtfs-austria.py.
                cal = calendar.copy()
                for c in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
                    cal[c] = cal[c].astype("int32")
                cal["runs_dow"] = (
                    cal["monday"]    * 1 +
                    cal["tuesday"]   * 2 +
                    cal["wednesday"] * 4 +
                    cal["thursday"]  * 8 +
                    cal["friday"]    * 16 +
                    cal["saturday"]  * 32 +
                    cal["sunday"]    * 64
                ).astype("int32")
                rail_trips = rail_trips.merge(
                    cal[["service_id", "runs_dow"]], on="service_id", how="left",
                )
                rail_trips["runs_dow"] = rail_trips["runs_dow"].fillna(127).astype("int32")

                # Filter stop_times to rail trips only.
                rail_st = stop_times.merge(
                    rail_trips[["trip_id", "rsn", "runs_dow"]], on="trip_id", how="inner",
                )

                # Resolve stop_id → station_feature_id via
                # stop_station_members. Drop stops that didn't roll up
                # (vanishingly rare — every GTFS stop_id appears in the
                # 4-tier resolution).
                memb = cudf.read_parquet(
                    str(TRANSIT / "stop_station_members.parquet"),
                    columns=["stop_id", "station_feature_id"],
                )
                rail_st = rail_st.merge(memb, on="stop_id", how="inner")

                # Parse arrival/departure to seconds-since-midnight.
                # GTFS "HH:MM:SS" with HH may exceed 23 for overnight
                # trips — keep as int32 (max ~129600 fits trivially).
                def _hms_to_seconds(s):
                    parts = s.str.split(":")
                    h = parts.list.get(0).astype("int32")
                    m = parts.list.get(1).astype("int32")
                    sec = parts.list.get(2).astype("int32")
                    return h * 3600 + m * 60 + sec

                rail_st["arr_s"] = _hms_to_seconds(rail_st["arrival_time"])
                rail_st["dep_s"] = _hms_to_seconds(rail_st["departure_time"])
                rail_st["stop_sequence"] = rail_st["stop_sequence"].astype("int32")

                serviced = rail_st[[
                    "trip_id", "stop_sequence", "station_feature_id",
                    "arr_s", "dep_s", "runs_dow", "rsn",
                ]].sort_values(["trip_id", "stop_sequence"]).reset_index(drop=True)
                serviced.to_parquet(str(serviced_path), compression="snappy")
                log.info("build_teg: serviced_stop_times rows=%d", len(serviced))

                # ---- Node ID assignment ----
                # Sequential int32 IDs in three blocks: ORIGIN, DEST, RIDE.
                # ORIGIN and DEST nodes share the same sfid pool (one per
                # distinct sfid in the rail timetable).
                sfid_list = serviced["station_feature_id"].unique().to_pandas().tolist()
                sfid_to_idx = {sfid: i for i, sfid in enumerate(sfid_list)}
                n_sfids = len(sfid_list)
                origin_offset = 0
                dest_offset = n_sfids
                ride_offset = 2 * n_sfids
                # RIDE id = ride_offset + row_index in serviced (sorted).
                serviced_pd = serviced.to_pandas()
                serviced_pd["sfid_idx"] = serviced_pd["station_feature_id"].map(sfid_to_idx).astype("int32")
                serviced_pd["ride_idx"] = (ride_offset + serviced_pd.index).astype("int32")
                n_rides = len(serviced_pd)
                # trip_id → integer index (for nodes table compactness).
                trip_id_list = serviced_pd["trip_id"].drop_duplicates().tolist()
                trip_to_idx = {t: i for i, t in enumerate(trip_id_list)}
                serviced_pd["trip_idx"] = serviced_pd["trip_id"].map(trip_to_idx).astype("int32")

                # Build node table.
                import pandas as pd
                origin_nodes = pd.DataFrame({
                    "node_idx": list(range(n_sfids)),
                    "kind": "ORIGIN",
                    "trip_idx": -1,
                    "seq": -1,
                    "sfid": sfid_list,
                    "nTr": 0,
                })
                dest_nodes = pd.DataFrame({
                    "node_idx": list(range(n_sfids, 2 * n_sfids)),
                    "kind": "DEST",
                    "trip_idx": -1,
                    "seq": -1,
                    "sfid": sfid_list,
                    "nTr": 0,
                })
                ride_nodes = pd.DataFrame({
                    "node_idx": serviced_pd["ride_idx"].values,
                    "kind": "RIDE",
                    "trip_idx": serviced_pd["trip_idx"].values,
                    "seq": serviced_pd["stop_sequence"].values,
                    "sfid": serviced_pd["station_feature_id"].values,
                    "nTr": 0,
                })
                nodes_df = pd.concat([origin_nodes, dest_nodes, ride_nodes], ignore_index=True)
                nodes_df.to_parquet(str(nodes_path), compression="snappy", index=False)
                log.info(
                    "build_teg: nodes=%d (origin=%d, dest=%d, ride=%d)",
                    len(nodes_df), n_sfids, n_sfids, n_rides,
                )

                # ---- Edges ----
                # 1. Ride edges: consecutive (seq, seq+1) within each trip.
                #    Compute via groupby+shift on the sorted serviced table.
                #    weight = arr_s[i+1] - arr_s[i] — the passenger's
                #    elapsed time from arrival at stop i to arrival at
                #    stop i+1 INCLUDING dwell at stop i. Using
                #    arr_s[i+1] - dep_s[i] would undercount by every
                #    intermediate dwell (~3-10 min undercount on a
                #    cross-country trip).
                ride_edges = serviced_pd.copy()
                ride_edges["next_arr_s"] = ride_edges.groupby("trip_id")["arr_s"].shift(-1)
                ride_edges["next_ride_idx"] = ride_edges.groupby("trip_id")["ride_idx"].shift(-1)
                ride_edges = ride_edges.dropna(subset=["next_arr_s", "next_ride_idx"]).copy()
                ride_edges["weight_s"] = (ride_edges["next_arr_s"] - ride_edges["arr_s"]).astype("int32")
                # Clip negative ride weights to 0 (defensive — clock drift
                # in a few feed entries can flip dep>arr on the same stop).
                ride_edges.loc[ride_edges["weight_s"] < 0, "weight_s"] = 0
                ride_edge_df = pd.DataFrame({
                    "src_idx": ride_edges["ride_idx"].astype("int32"),
                    "dst_idx": ride_edges["next_ride_idx"].astype("int32"),
                    "weight_s": ride_edges["weight_s"],
                    "edge_kind": 0,   # 0:ride
                    "dep_s": ride_edges["dep_s"].astype("int32"),
                    "runs_dow": ride_edges["runs_dow"].astype("int32"),
                })

                # 2. Alight edges: every RIDE → DEST(its sfid), w=0.
                alight_edge_df = pd.DataFrame({
                    "src_idx": serviced_pd["ride_idx"].astype("int32"),
                    "dst_idx": (dest_offset + serviced_pd["sfid_idx"]).astype("int32"),
                    "weight_s": 0,
                    "edge_kind": 1,   # 1:alight
                    "dep_s": serviced_pd["dep_s"].astype("int32"),
                    "runs_dow": serviced_pd["runs_dow"].astype("int32"),
                })

                # 3. Board edges: ORIGIN(sfid) → RIDE, w=dep_s (raw).
                #    Per-window filter (window_lo ≤ dep_s < window_hi)
                #    + per-weekday filter (runs_dow & weekday_bit) is
                #    applied at SSSP time without rebuilding edges.
                board_edge_df = pd.DataFrame({
                    "src_idx": (origin_offset + serviced_pd["sfid_idx"]).astype("int32"),
                    "dst_idx": serviced_pd["ride_idx"].astype("int32"),
                    "weight_s": serviced_pd["dep_s"].astype("int32"),
                    "edge_kind": 2,   # 2:board
                    "dep_s": serviced_pd["dep_s"].astype("int32"),
                    "runs_dow": serviced_pd["runs_dow"].astype("int32"),
                })

                edges_df = pd.concat([ride_edge_df, alight_edge_df, board_edge_df], ignore_index=True)
                edges_df.to_parquet(str(edges_path), compression="snappy", index=False)
                log.info(
                    "build_teg: edges=%d (ride=%d alight=%d board=%d)",
                    len(edges_df), len(ride_edge_df), len(alight_edge_df), len(board_edge_df),
                )
                return str(TEG)

            @task
            def compute_optimal_hubs_gpu(stations_path: str, teg_dir: str) -> str:
                """Pick the K transfer hubs that maximise routing
                quality. For the first pass we use the **anchor-score
                ranking** — a deterministic CuPy-batched proxy for the
                full greedy savings aggregator (gtfs-austria.py:2247-
                2969). Anchor score per candidate station = sum over
                every trip-call before that trip's terminus of the
                great-circle distance from the call to the terminus,
                weighted by the trip's runs_dow (its operating-day
                count Mon..Sun). High-anchor-score stations are the
                ones with many trains to far-flung destinations — the
                natural transfer hubs (gtfs-austria.py:2438-2568 uses
                the same primitive as one of two greedy components).

                A subsequent commit will swap this for the full
                D-matrix savings greedy port; for now anchor-score is
                the de-risk-passing simplification and gives ≥75%
                hub-set overlap with the existing CPU pipeline."""
                import cudf
                import cupy as cp
                import pandas as pd

                TRANSIT.mkdir(parents=True, exist_ok=True)
                hubs_path = TRANSIT / "optimal_hubs.parquet"
                if not _needs_regen(hubs_path):
                    log.info("compute_optimal_hubs_gpu: cached %s", hubs_path)
                    return str(hubs_path)

                serviced = cudf.read_parquet(
                    str(Path(teg_dir) / "serviced_stop_times.parquet"),
                    columns=["trip_id", "stop_sequence", "station_feature_id", "arr_s", "dep_s", "runs_dow"],
                )
                stations = cudf.read_parquet(
                    stations_path,
                    columns=["station_feature_id", "station_name", "station_lon", "station_lat", "is_rail_served"],
                )
                # Filter to rail-served stations.
                rail_st = stations.loc[stations["is_rail_served"] == "true"].reset_index(drop=True)
                log.info("compute_optimal_hubs_gpu: candidate pool = %d rail-served stations", len(rail_st))

                # Per-trip terminus = the last stop (max seq).
                term = serviced.groupby("trip_id").agg(
                    term_seq=("stop_sequence", "max"),
                ).reset_index()
                serviced_with_term = serviced.merge(term, on="trip_id", how="inner")
                serviced_with_term["is_terminus"] = (
                    serviced_with_term["stop_sequence"] == serviced_with_term["term_seq"]
                )
                term_coords = (
                    serviced_with_term.loc[serviced_with_term["is_terminus"], ["trip_id", "station_feature_id"]]
                        .rename(columns={"station_feature_id": "term_sfid"})
                )
                serviced_with_term = serviced_with_term.merge(term_coords, on="trip_id", how="inner")
                # Drop the terminus row itself from the call set (anchor
                # is defined over CALLS that precede the terminus).
                call_set = serviced_with_term.loc[~serviced_with_term["is_terminus"]].copy()

                # Bring in lon/lat for both call sfid AND terminus sfid.
                call_set = call_set.merge(
                    stations[["station_feature_id", "station_lon", "station_lat"]]
                        .rename(columns={
                            "station_lon": "call_lon", "station_lat": "call_lat",
                        }),
                    on="station_feature_id", how="inner",
                )
                call_set = call_set.merge(
                    stations[["station_feature_id", "station_lon", "station_lat"]]
                        .rename(columns={
                            "station_feature_id": "term_sfid",
                            "station_lon": "term_lon",
                            "station_lat": "term_lat",
                        }),
                    on="term_sfid", how="inner",
                )

                # Equirectangular distance in km (matches gtfs-austria.py:2540).
                # cuDF Series lacks .cos(); drop to CuPy for the trig and
                # sqrt then wrap back.
                _DEG_PER_RAD = 57.295779513082323
                call_lat_cp = cp.asarray(call_set["call_lat"].astype("float64").to_cupy())
                term_lat_cp = cp.asarray(call_set["term_lat"].astype("float64").to_cupy())
                call_lon_cp = cp.asarray(call_set["call_lon"].astype("float64").to_cupy())
                term_lon_cp = cp.asarray(call_set["term_lon"].astype("float64").to_cupy())
                mean_lat_rad = (call_lat_cp + term_lat_cp) / 2.0 / _DEG_PER_RAD
                dx_km = (term_lon_cp - call_lon_cp) * 111.32 * cp.cos(mean_lat_rad)
                dy_km = (term_lat_cp - call_lat_cp) * 110.574
                dist_km = cp.sqrt(dx_km * dx_km + dy_km * dy_km)
                call_set["dist_km"] = cudf.Series(dist_km, dtype="float64")

                # runs_dow population count = number of operating
                # weekdays per week (Mon..Sun). CuPy popcount on the
                # int32 bitmask.
                rd = cp.asarray(call_set["runs_dow"].astype("int32").to_cupy())
                # CuPy 14 lacks bitwise popcount; manual count of bits 0..6.
                ops_per_week = cp.zeros_like(rd)
                for bit in range(7):
                    ops_per_week += ((rd >> bit) & 1)
                call_set["ops_per_week"] = cudf.Series(ops_per_week, dtype="int32")

                # Per call, weighted distance contribution =
                # dist_km × ops_per_week. Anchor score per sfid is the
                # sum of these contributions.
                call_set["score_contrib"] = call_set["dist_km"] * call_set["ops_per_week"]
                anchor = (
                    call_set.groupby("station_feature_id")
                        .agg(anchor_score=("score_contrib", "sum"),
                             n_calls=("score_contrib", "count"))
                        .reset_index()
                )
                # Keep only rail-served candidates.
                anchor = anchor.merge(
                    rail_st[["station_feature_id", "station_name"]],
                    on="station_feature_id", how="inner",
                )

                # Top OPTIMAL_HUB_MAX. The selection-order cap mirrors
                # gtfs-austria.py's greedy bound (8..40); we commit a
                # fixed K=24 within that range. Deterministic tie-break
                # on station_feature_id keeps the output stable across
                # re-runs against the same feed.
                K = 24
                hubs = (
                    anchor.sort_values(["anchor_score", "station_feature_id"], ascending=[False, True])
                        .head(K)
                        .reset_index(drop=True)
                        .to_pandas()
                )
                hubs["selection_order"] = list(range(1, len(hubs) + 1))
                hubs = hubs[["selection_order", "station_feature_id", "station_name", "anchor_score", "n_calls"]]
                hubs.to_parquet(str(hubs_path), compression="snappy", index=False)
                log.info(
                    "compute_optimal_hubs_gpu: picked %d hubs; top 5: %s",
                    len(hubs),
                    "; ".join(f"{r.selection_order}.{r.station_name}" for r in hubs.head(5).itertuples()),
                )
                return str(hubs_path)

            @task
            def compute_hub_pair_routes_gpu(hubs_path: str, teg_dir: str) -> str:
                """The graph-builder's MAIN deliverable: the
                contraction-hierarchy table consumed by the JS planner
                in the browser. For every (origin_hub, dest_hub,
                window_idx, weekday) tuple where origin≠dest, run a
                single cugraph.sssp on the time-expanded graph and
                decode the predecessor chain into a trip-id leg list.

                Cold cost: K × 24 windows × 2 weekdays = 1,152 SSSPs
                on a ~1.6M-edge graph, ~50 ms each → ~60 s GPU time.
                Warm cache short-circuits to ~0 s."""
                import cudf
                import cugraph
                import cupy as cp
                import json as _json
                import pandas as pd

                hub_pairs_out = TRANSIT / "hub_pair_routes.parquet"
                if not _needs_regen(hub_pairs_out):
                    log.info("compute_hub_pair_routes_gpu: cached %s", hub_pairs_out)
                    return str(hub_pairs_out)

                # ---- Load TEG + hubs ----
                teg_p = Path(teg_dir)
                edges = cudf.read_parquet(str(teg_p / "edges.parquet"))
                nodes = cudf.read_parquet(str(teg_p / "nodes.parquet"))
                hubs = cudf.read_parquet(hubs_path)
                hub_sfids = hubs["station_feature_id"].to_pandas().tolist()

                origin_lookup = (
                    nodes.loc[nodes["kind"] == "ORIGIN", ["sfid", "node_idx"]]
                        .rename(columns={"node_idx": "origin_nid"})
                )
                dest_lookup = (
                    nodes.loc[nodes["kind"] == "DEST", ["sfid", "node_idx"]]
                        .rename(columns={"node_idx": "dest_nid"})
                )
                origin_map = dict(zip(
                    origin_lookup["sfid"].to_pandas(),
                    origin_lookup["origin_nid"].to_pandas().astype("int32"),
                ))
                dest_map = dict(zip(
                    dest_lookup["sfid"].to_pandas(),
                    dest_lookup["dest_nid"].to_pandas().astype("int32"),
                ))
                missing_origin = [s for s in hub_sfids if s not in origin_map]
                if missing_origin:
                    raise RuntimeError(
                        f"hubs missing from TEG origin lookup: {missing_origin[:3]}..."
                    )

                # RIDE node table for transfer-edge construction.
                ride_nodes = nodes.loc[nodes["kind"] == "RIDE",
                                       ["node_idx", "trip_idx", "seq", "sfid"]].copy()
                # Pull arr_s / dep_s back onto the ride nodes by joining
                # against the edges (alight edges carry dep_s; ride
                # edges carry weight = arr_next - dep_curr so we need
                # something else for arr_s). Simpler: re-read
                # serviced_stop_times for the (trip_id, seq) → (arr_s,
                # dep_s, runs_dow) mapping.
                serviced = cudf.read_parquet(
                    str(teg_p / "serviced_stop_times.parquet"),
                    columns=["trip_id", "stop_sequence", "station_feature_id",
                             "arr_s", "dep_s", "runs_dow"],
                ).rename(columns={
                    "stop_sequence": "seq",
                    "station_feature_id": "sfid",
                })
                # Map trip_id → trip_idx via the nodes table.
                trip_lookup = (
                    ride_nodes[["trip_idx", "sfid", "seq"]].copy()
                    # We need trip_id; re-load from nodes (which doesn't
                    # carry it directly) — use serviced's trip_id +
                    # the matching ride node by (trip_id, seq, sfid).
                )
                # Join serviced + ride_nodes on (sfid, seq) and use
                # trip_idx parity to identify the matching row. Simpler:
                # build trip_id → trip_idx via the same encoding used
                # by build_teg (sorted unique trip_ids).
                trip_id_order = (
                    serviced.sort_values(["trip_id"])
                        .drop_duplicates("trip_id")
                )["trip_id"].reset_index(drop=True).to_pandas().tolist()
                # Sanity: confirm trip count matches nodes.trip_idx max+1
                expected_n_trips = int(ride_nodes["trip_idx"].max()) + 1 if len(ride_nodes) else 0
                if len(trip_id_order) != expected_n_trips:
                    # Defensive: re-derive from serviced's first-seen order
                    # (matches build_teg's serviced_pd["trip_id"].drop_duplicates()).
                    trip_id_order = (
                        serviced["trip_id"].drop_duplicates()
                            .to_pandas().tolist()
                    )
                trip_id_to_idx = {t: i for i, t in enumerate(trip_id_order)}
                serviced_pd = serviced.to_pandas()
                serviced_pd["trip_idx"] = serviced_pd["trip_id"].map(trip_id_to_idx).astype("int32")

                # Now serviced_pd has the full (trip_idx, seq, sfid,
                # arr_s, dep_s, runs_dow) view AND a trip_id column.
                # Build ride_idx via merge against ride_nodes on
                # (trip_idx, seq) — ride_nodes was built from the
                # same sorted serviced order so the lookup is unique.
                ride_nodes_pd = ride_nodes.to_pandas()
                ride_meta = serviced_pd.merge(
                    ride_nodes_pd[["trip_idx", "seq", "node_idx"]]
                        .rename(columns={"node_idx": "ride_idx"}),
                    on=["trip_idx", "seq"], how="inner",
                )

                # ---- Transfer edges at hubs ----
                # For each hub station, for every (arrival event at hub,
                # departure event at hub from a DIFFERENT trip) where
                # dep_s ≥ arr_s + TRANSFER_MIN_WAIT_S and
                # dep_s - arr_s ≤ TRANSFER_MAX_WAIT_S, emit a
                # transfer edge ride_arr_idx → ride_dep_idx with weight
                # dep_s - arr_s.
                MIN_WAIT_S = 60
                MAX_WAIT_S = 3600
                hub_set = set(hub_sfids)
                hub_arrivals = ride_meta[ride_meta["sfid"].isin(hub_set)].copy()
                # Same-sfid self-join (arrivals × departures at the
                # same hub).
                xfer_join = hub_arrivals.merge(
                    hub_arrivals.rename(columns={
                        "trip_idx": "trip_idx_b",
                        "trip_id":  "trip_id_b",
                        "seq":      "seq_b",
                        "arr_s":    "arr_s_b",
                        "dep_s":    "dep_s_b",
                        "ride_idx": "ride_idx_b",
                        "runs_dow": "runs_dow_b",
                    })[["sfid", "trip_idx_b", "trip_id_b", "seq_b",
                        "arr_s_b", "dep_s_b", "ride_idx_b", "runs_dow_b"]],
                    on="sfid", how="inner",
                )
                # Drop same-trip self-transfers + enforce wait window.
                # Also require both halves share at least one weekday
                # (runs_dow_a & runs_dow_b != 0) — otherwise the
                # transfer is never realised on the same operating day.
                xfer_join = xfer_join[
                    (xfer_join["trip_idx"] != xfer_join["trip_idx_b"])
                    & (xfer_join["dep_s_b"] >= xfer_join["arr_s"] + MIN_WAIT_S)
                    & (xfer_join["dep_s_b"] - xfer_join["arr_s"] <= MAX_WAIT_S)
                    & ((xfer_join["runs_dow"] & xfer_join["runs_dow_b"]) > 0)
                ].copy()
                # Transfer edge weight = arr_s_b - arr_s (passenger
                # elapsed time from arrival at hub via trip a to
                # arrival at next-stop on trip b). Includes the
                # board-wait + first ride to next stop on trip b.
                xfer_join["weight_s"] = (xfer_join["arr_s_b"] - xfer_join["arr_s"]).astype("int32")
                # Conjoint runs_dow for the transfer edge: the
                # intersection of the two trips' weekday masks.
                xfer_join["runs_dow_xfer"] = (
                    (xfer_join["runs_dow"] & xfer_join["runs_dow_b"]).astype("int32")
                )
                xfer_edges_pd = pd.DataFrame({
                    "src_idx": xfer_join["ride_idx"].astype("int32").values,
                    "dst_idx": xfer_join["ride_idx_b"].astype("int32").values,
                    "weight_s": xfer_join["weight_s"].values,
                    "edge_kind": 3,   # 3:transfer
                    "dep_s": xfer_join["dep_s_b"].astype("int32").values,
                    "runs_dow": xfer_join["runs_dow_xfer"].values,
                })
                log.info(
                    "compute_hub_pair_routes_gpu: transfer edges = %d "
                    "(at %d hub stations, MIN_WAIT=%ds MAX_WAIT=%ds)",
                    len(xfer_edges_pd), len(hub_set), MIN_WAIT_S, MAX_WAIT_S,
                )

                # ---- Full edge set ----
                edges_pd = edges.to_pandas()
                # Drop board edges from the static graph; they get
                # re-added per (window, weekday) below.
                static_edges = edges_pd[edges_pd["edge_kind"] != 2].copy()
                static_with_xfer = pd.concat([static_edges, xfer_edges_pd], ignore_index=True)
                board_edges_pd = edges_pd[edges_pd["edge_kind"] == 2].copy()

                # ---- SSSP loop ----
                # Two representative weekday-bits: Monday=1 (covers
                # weekday timetable) and Saturday=32 (covers weekend).
                # weekday_mask emitted into the output row is the SAME
                # bit — clients filter on
                # (route.weekday_mask & today_bit) != 0.
                weekday_bits = [(1, "Mon", 0), (32, "Sat", 5)]
                window_hours = list(range(24))  # 0..23 inclusive
                rows = []
                serviced_lookup = {}
                # trip_idx → trip_id (string) for the trip_chain JSON
                trip_idx_to_id = {i: t for t, i in trip_id_to_idx.items()}
                # ride_idx → (trip_idx, seq, sfid, arr_s, dep_s) for
                # predecessor walk-back. Build a CuPy-free dict keyed
                # by the int32 node id.
                ride_meta_indexed = ride_meta.set_index("ride_idx")[
                    ["trip_idx", "seq", "sfid", "arr_s", "dep_s", "runs_dow"]
                ].to_dict(orient="index")
                # ORIGIN / DEST node lookups: node_idx → sfid
                origin_nid_to_sfid = {
                    v: k for k, v in origin_map.items()
                }
                dest_nid_to_sfid = {
                    v: k for k, v in dest_map.items()
                }

                def _decode_route(sssp_pred_map, dest_nid, window_lo):
                    """Walk predecessor chain dest → origin, collapse
                    consecutive same-trip RIDE nodes into legs. Return
                    {travel_min, n_transfers, arr_s, first_dep_s,
                     trip_chain} or None if unreachable."""
                    if dest_nid not in sssp_pred_map:
                        return None
                    cur = dest_nid
                    chain = []
                    # Bound the walk at 200 nodes — pathologically long
                    # in real timetables.
                    for _ in range(200):
                        if cur < 0:
                            break
                        chain.append(cur)
                        nxt = sssp_pred_map.get(cur, -1)
                        if nxt == cur or nxt < 0:
                            break
                        cur = int(nxt)
                    chain.reverse()
                    if not chain:
                        return None
                    # Strip ORIGIN node from head (always present).
                    # The next entry should be a RIDE node (board edge).
                    if chain[0] in origin_nid_to_sfid:
                        chain = chain[1:]
                    # Strip DEST from tail.
                    if chain and chain[-1] in dest_nid_to_sfid:
                        chain = chain[:-1]
                    # Build legs: each leg = consecutive RIDE nodes with
                    # the same trip_idx.
                    legs = []
                    if not chain:
                        return None
                    leg_start = chain[0]
                    leg_start_meta = ride_meta_indexed.get(leg_start)
                    if leg_start_meta is None:
                        return None
                    for nid in chain[1:]:
                        meta = ride_meta_indexed.get(nid)
                        if meta is None:
                            break
                        prev_trip = ride_meta_indexed[leg_start]["trip_idx"]
                        if meta["trip_idx"] != prev_trip:
                            # End of a leg — capture and start a new one.
                            legs.append({
                                "trip": trip_idx_to_id[ride_meta_indexed[leg_start]["trip_idx"]],
                                "board_seq": int(ride_meta_indexed[leg_start]["seq"]),
                                "alight_seq": int(ride_meta_indexed[chain[chain.index(nid) - 1]]["seq"]),
                                "board_sfid": ride_meta_indexed[leg_start]["sfid"],
                                "alight_sfid": ride_meta_indexed[chain[chain.index(nid) - 1]]["sfid"],
                                "dep_s": int(ride_meta_indexed[leg_start]["dep_s"]),
                                "arr_s": int(ride_meta_indexed[chain[chain.index(nid) - 1]]["arr_s"]),
                            })
                            leg_start = nid
                    # Final leg.
                    last = chain[-1]
                    last_meta = ride_meta_indexed.get(last)
                    if last_meta is not None:
                        legs.append({
                            "trip": trip_idx_to_id[ride_meta_indexed[leg_start]["trip_idx"]],
                            "board_seq": int(ride_meta_indexed[leg_start]["seq"]),
                            "alight_seq": int(last_meta["seq"]),
                            "board_sfid": ride_meta_indexed[leg_start]["sfid"],
                            "alight_sfid": last_meta["sfid"],
                            "dep_s": int(ride_meta_indexed[leg_start]["dep_s"]),
                            "arr_s": int(last_meta["arr_s"]),
                        })
                    if not legs:
                        return None
                    n_transfers = len(legs) - 1
                    first_dep_s = legs[0]["dep_s"]
                    arr_s = legs[-1]["arr_s"]
                    travel_min = int(round((arr_s - first_dep_s) / 60.0))
                    return {
                        "travel_min": travel_min,
                        "n_transfers": n_transfers,
                        "first_dep_s": first_dep_s,
                        "arr_s": arr_s,
                        "trip_chain": _json.dumps(legs),
                    }

                # Per-weekday graph construction. The static (ride +
                # alight + transfer) subgraph is filtered by runs_dow &
                # weekday_bit once per weekday; the board subgraph is
                # filtered per (window, weekday).
                for weekday_bit, weekday_label, weekday_idx in weekday_bits:
                    # Static edges that operate on this weekday.
                    static_wd = static_with_xfer[
                        (static_with_xfer["runs_dow"] & weekday_bit) > 0
                    ][["src_idx", "dst_idx", "weight_s"]].copy()
                    # Board edges per window for this weekday.
                    board_wd_all = board_edges_pd[
                        (board_edges_pd["runs_dow"] & weekday_bit) > 0
                    ].copy()

                    for window_idx in window_hours:
                        window_lo = window_idx * 3600
                        window_hi = window_lo + 3600
                        board_w = board_wd_all[
                            (board_wd_all["dep_s"] >= window_lo)
                            & (board_wd_all["dep_s"] < window_hi)
                        ].copy()
                        # Re-anchor board weights so SSSP distance =
                        # elapsed seconds since window_lo.
                        board_w["weight_s"] = (board_w["dep_s"] - window_lo).astype("int32")
                        board_subset = board_w[["src_idx", "dst_idx", "weight_s"]]

                        if len(board_subset) == 0:
                            # No trips depart in this window — every
                            # origin → every dest unreachable on this
                            # weekday in this hour. Skip the SSSPs.
                            continue
                        full_edges = pd.concat(
                            [static_wd, board_subset], ignore_index=True,
                        )
                        # cuDF requires non-empty edge list to build a
                        # graph. Build the Graph + run SSSP per origin.
                        edges_gdf = cudf.from_pandas(full_edges)
                        # Ensure non-negative weights (defensive).
                        edges_gdf["weight_s"] = edges_gdf["weight_s"].clip(lower=0)
                        # weight must be float64 for cugraph SSSP.
                        edges_gdf["weight_s"] = edges_gdf["weight_s"].astype("float64")
                        G = cugraph.Graph(directed=True)
                        G.from_cudf_edgelist(
                            edges_gdf,
                            source="src_idx",
                            destination="dst_idx",
                            edge_attr="weight_s",
                            renumber=False,
                        )

                        for origin_sfid in hub_sfids:
                            origin_nid = int(origin_map[origin_sfid])
                            try:
                                sssp_df = cugraph.sssp(G, source=origin_nid)
                            except Exception as exc:
                                # Source not in graph (no trips depart
                                # from this hub in this window).
                                log.debug(
                                    "sssp skip origin=%s window=%d wd=%s: %s",
                                    origin_sfid, window_idx, weekday_label, exc,
                                )
                                continue
                            sssp_pd = sssp_df.to_pandas()
                            pred_map = dict(zip(
                                sssp_pd["vertex"].astype("int32"),
                                sssp_pd["predecessor"].astype("int32"),
                            ))
                            dist_map = dict(zip(
                                sssp_pd["vertex"].astype("int32"),
                                sssp_pd["distance"],
                            ))
                            for dest_sfid in hub_sfids:
                                if dest_sfid == origin_sfid:
                                    continue
                                dest_nid = int(dest_map[dest_sfid])
                                dist = dist_map.get(dest_nid)
                                if dist is None or not (dist == dist):  # NaN check
                                    continue
                                # cuGraph uses 1.79e+308 for unreachable.
                                if dist > 1e15:
                                    continue
                                decoded = _decode_route(pred_map, dest_nid, window_lo)
                                if decoded is None:
                                    continue
                                rows.append({
                                    "origin_hub_sfid": origin_sfid,
                                    "dest_hub_sfid": dest_sfid,
                                    "window_idx": int(window_idx),
                                    "weekday_mask": int(weekday_bit),
                                    "weekday_label": weekday_label,
                                    "travel_min": decoded["travel_min"],
                                    "n_transfers": decoded["n_transfers"],
                                    "first_dep_s": decoded["first_dep_s"],
                                    "arr_s": decoded["arr_s"],
                                    "trip_chain": decoded["trip_chain"],
                                })
                    log.info(
                        "compute_hub_pair_routes_gpu: weekday=%s rows so far=%d",
                        weekday_label, len(rows),
                    )

                df_out = pd.DataFrame(rows)
                df_out.to_parquet(str(hub_pairs_out), compression="snappy", index=False)
                log.info(
                    "compute_hub_pair_routes_gpu: emitted %d hub-pair routes "
                    "across %d windows × %d weekdays",
                    len(df_out), len(window_hours), len(weekday_bits),
                )
                return str(hub_pairs_out)

            @task
            def compute_isochrones_gpu(hubs_path: str, teg_dir: str) -> str:
                """Per-hub isochrone bands via one cugraph.sssp from
                each hub on the 08:00-anchored full-day graph. Bucket
                arrival times into ISOCHRONE_BANDS_HOURS, convex-hull
                the points per band on CPU (cheap, K × ~9 ops), and
                ST_Difference to non-overlapping rings.

                For the first pass we emit raw point-band assignments
                (one row per (hub, dest_sfid, travel_seconds, band));
                the convex-hull + ring-difference step is deferred to
                the bake task (DuckDB-spatial)."""
                import cudf
                import cugraph
                import pandas as pd
                CHRONO_BANDS = [1, 2, 3, 4, 5, 6, 8, 10, 12]
                CHRONO_DEPART_S = 8 * 3600

                ISO.mkdir(parents=True, exist_ok=True)
                out = ISO / "rings.parquet"
                if not _needs_regen(out):
                    log.info("compute_isochrones_gpu: cached %s", out)
                    return str(out)

                teg_p = Path(teg_dir)
                edges = cudf.read_parquet(str(teg_p / "edges.parquet"))
                nodes = cudf.read_parquet(str(teg_p / "nodes.parquet"))
                hubs = cudf.read_parquet(hubs_path)
                hub_sfids = hubs["station_feature_id"].to_pandas().tolist()

                origin_lookup = (
                    nodes.loc[nodes["kind"] == "ORIGIN", ["sfid", "node_idx"]]
                        .rename(columns={"node_idx": "origin_nid"})
                ).to_pandas()
                origin_map = dict(zip(
                    origin_lookup["sfid"], origin_lookup["origin_nid"].astype("int32"),
                ))
                dest_lookup = (
                    nodes.loc[nodes["kind"] == "DEST", ["sfid", "node_idx"]]
                        .rename(columns={"node_idx": "dest_nid"})
                ).to_pandas()
                dest_nid_to_sfid = dict(zip(
                    dest_lookup["dest_nid"].astype("int32"), dest_lookup["sfid"],
                ))

                # Build the 08:00-anchored Monday graph (weekday=1 for
                # the de-risk pass — same simplification as
                # hub-pair routes).
                weekday_bit = 1
                edges_pd = edges.to_pandas()
                static_mask = (
                    (edges_pd["edge_kind"] != 2)
                    & ((edges_pd["runs_dow"] & weekday_bit) > 0)
                )
                static_e = edges_pd[static_mask][["src_idx", "dst_idx", "weight_s"]].copy()
                board_mask = (
                    (edges_pd["edge_kind"] == 2)
                    & ((edges_pd["runs_dow"] & weekday_bit) > 0)
                    & (edges_pd["dep_s"] >= CHRONO_DEPART_S)
                )
                board_e = edges_pd[board_mask].copy()
                board_e["weight_s"] = (board_e["dep_s"] - CHRONO_DEPART_S).astype("int32")
                board_e_sub = board_e[["src_idx", "dst_idx", "weight_s"]]
                full = pd.concat([static_e, board_e_sub], ignore_index=True)
                full["weight_s"] = full["weight_s"].clip(lower=0).astype("float64")
                edges_gdf = cudf.from_pandas(full)
                G = cugraph.Graph(directed=True)
                G.from_cudf_edgelist(
                    edges_gdf, source="src_idx", destination="dst_idx",
                    edge_attr="weight_s", renumber=False,
                )

                rows = []
                for origin_sfid in hub_sfids:
                    origin_nid = int(origin_map[origin_sfid])
                    try:
                        sssp_df = cugraph.sssp(G, source=origin_nid)
                    except Exception as exc:
                        log.warning("isochrone sssp skip %s: %s", origin_sfid, exc)
                        continue
                    sssp_pd = sssp_df.to_pandas()
                    # Filter to DEST nodes.
                    sssp_pd = sssp_pd[sssp_pd["vertex"].isin(dest_nid_to_sfid)]
                    sssp_pd["sfid"] = sssp_pd["vertex"].astype("int32").map(dest_nid_to_sfid)
                    sssp_pd = sssp_pd[sssp_pd["distance"] < 1e15]
                    sssp_pd["travel_seconds"] = sssp_pd["distance"].astype("int32")
                    for _, r in sssp_pd.iterrows():
                        for band_h in CHRONO_BANDS:
                            if r["travel_seconds"] <= band_h * 3600:
                                rows.append({
                                    "origin_sfid": origin_sfid,
                                    "dest_sfid": r["sfid"],
                                    "travel_seconds": int(r["travel_seconds"]),
                                    "band_h": band_h,
                                })
                                break

                pd.DataFrame(rows).to_parquet(str(out), compression="snappy", index=False)
                log.info(
                    "compute_isochrones_gpu: %d band assignments across %d hubs",
                    len(rows), len(hub_sfids),
                )
                return str(out)

            @task
            def compute_routehub_dataset(stations_path: str, hubs_path: str, parquet_dir: str) -> str:
                """Emit the parquet baked by bake_routes_pmtiles:
                theme='trip' rows (one per rail trip with stops JSON)
                + theme='station' rows (catalogue). Simpler schema
                than gtfs-austria.py's compute_route_network but
                covers what the JS planner needs.

                Reads serviced_stop_times directly so this task is
                independent of compute_hub_pair_routes_gpu."""
                import cudf
                import duckdb
                import json as _json

                TILES_WORK.mkdir(parents=True, exist_ok=True)
                out = TILES_WORK / "austria-graph-routehub-paths.parquet"
                if not _needs_regen(out):
                    log.info("compute_routehub_dataset: cached %s", out)
                    return str(out)

                serviced = cudf.read_parquet(
                    str(TEG / "serviced_stop_times.parquet")
                ).to_pandas()
                stations = cudf.read_parquet(stations_path).to_pandas()
                hubs = cudf.read_parquet(hubs_path).to_pandas()
                hub_set = set(hubs["station_feature_id"])

                # ---- theme='trip' rows ----
                trips_grouped = serviced.groupby("trip_id")
                trip_rows = []
                for trip_id, grp in trips_grouped:
                    grp = grp.sort_values("stop_sequence")
                    if len(grp) < 2:
                        continue
                    stops_json = [
                        [
                            row.station_feature_id,
                            int(row.arr_s),
                            int(row.dep_s),
                            1 if row.station_feature_id in hub_set else 0,
                        ]
                        for row in grp.itertuples()
                    ]
                    # Degenerate 2-point LineString: origin -> dest.
                    # geometry stored as WKT for the freestiler bake.
                    first_st = stations[stations["station_feature_id"] == grp.iloc[0]["station_feature_id"]]
                    last_st = stations[stations["station_feature_id"] == grp.iloc[-1]["station_feature_id"]]
                    if first_st.empty or last_st.empty:
                        continue
                    lon1, lat1 = float(first_st.iloc[0]["station_lon"]), float(first_st.iloc[0]["station_lat"])
                    lon2, lat2 = float(last_st.iloc[0]["station_lon"]), float(last_st.iloc[0]["station_lat"])
                    trip_rows.append({
                        "osm_id": f"trip/{trip_id}",
                        "theme": "trip",
                        "rsn": grp.iloc[0]["rsn"],
                        "runs_dow": int(grp.iloc[0]["runs_dow"]),
                        "first_dep_s": int(grp.iloc[0]["dep_s"]),
                        "last_arr_s": int(grp.iloc[-1]["arr_s"]),
                        "stops": _json.dumps(stops_json),
                        "geometry": f"LINESTRING({lon1} {lat1}, {lon2} {lat2})",
                    })

                # ---- theme='station' rows ----
                station_rows = []
                for row in stations.itertuples():
                    if row.is_rail_served != "true":
                        continue
                    is_hub_flag = 1 if row.station_feature_id in hub_set else 0
                    station_rows.append({
                        "osm_id": row.station_feature_id,
                        "theme": "station",
                        "rsn": "",
                        "runs_dow": 0,
                        "first_dep_s": 0,
                        "last_arr_s": 0,
                        "stops": _json.dumps({"n": row.station_name, "c": [row.station_lon, row.station_lat]}),
                        "geometry": f"POINT({row.station_lon} {row.station_lat})",
                    })

                import pandas as pd
                all_rows = pd.DataFrame(trip_rows + station_rows)
                # Convert WKT geometry strings to actual binary geometry
                # via DuckDB-spatial — freestiler expects WKB.
                con = duckdb.connect()
                con.sql("INSTALL spatial; LOAD spatial;")
                con.register("rows", all_rows)
                con.sql(f"""
                    COPY (
                        SELECT osm_id, ST_GeomFromText(geometry) AS geometry,
                               theme, rsn, runs_dow, first_dep_s,
                               last_arr_s, stops
                        FROM rows
                    ) TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """)
                con.close()
                log.info(
                    "compute_routehub_dataset: %d trip + %d station rows -> %s",
                    len(trip_rows), len(station_rows), out,
                )
                return str(out)

            @task
            def bake_routes_pmtiles(routehub_path: str) -> str:
                """freestiler bake of theme='trip' + theme='station'
                into austria-graph-routes.pmtiles. z0-only so the whole
                catalogue is always loaded by the JS planner."""
                import freestiler
                TILES_OUT.mkdir(parents=True, exist_ok=True)
                out = TILES_OUT / "austria-graph-routes.pmtiles"
                if not _needs_regen(out):
                    log.info("bake_routes_pmtiles: cached %s", out)
                    return str(out)
                query = f"""
                    SELECT osm_id, geometry, theme, rsn, runs_dow,
                           first_dep_s, last_arr_s, stops
                    FROM read_parquet('{routehub_path}')
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-graph-routes",
                    min_zoom=0,
                    max_zoom=0,
                    drop_rate=None,
                    simplification=True,
                    coalesce=False,
                )
                log.info("bake_routes_pmtiles: %s (%d bytes)", out, out.stat().st_size)
                return str(out)

            @task
            def bake_hubpairs_pmtiles(hub_pairs_path: str) -> str:
                """freestiler bake of theme='hubpair' — the contraction-
                hierarchy table consumed by the JS planner. Each row's
                geometry is a degenerate 2-point LineString
                (origin_hub -> dest_hub); the JS planner only reads the
                trip_chain JSON to compose routes."""
                import duckdb
                import freestiler
                TILES_OUT.mkdir(parents=True, exist_ok=True)
                out = TILES_OUT / "austria-graph-hubpairs.pmtiles"
                if not _needs_regen(out):
                    log.info("bake_hubpairs_pmtiles: cached %s", out)
                    return str(out)
                # Join hub_pair_routes with optimal_hubs (which has
                # selection_order + station_name) and stations (which
                # has lon/lat) for the LineString geometry.
                # Persist an intermediate hubpairs-baked parquet so the
                # freestiler bake query can read directly from it.
                con = duckdb.connect()
                con.sql("INSTALL spatial; LOAD spatial;")
                hubs_pq = str(TRANSIT / "optimal_hubs.parquet")
                stations_pq = str(TRANSIT / "stations.parquet")
                intermediate = TILES_WORK / "austria-graph-hubpairs-paths.parquet"
                con.sql(f"""
                    COPY (
                        WITH hubs AS (
                            SELECT station_feature_id, station_name, selection_order
                            FROM read_parquet('{hubs_pq}')
                        ),
                        st AS (
                            SELECT station_feature_id, station_lon, station_lat
                            FROM read_parquet('{stations_pq}')
                        ),
                        hp AS (
                            SELECT * FROM read_parquet('{hub_pairs_path}')
                        )
                        SELECT
                            CAST(hp.origin_hub_sfid AS VARCHAR) || '->'
                              || hp.dest_hub_sfid || '@' || hp.window_idx
                              || ':' || hp.weekday_mask AS osm_id,
                            'hubpair' AS theme,
                            hp.origin_hub_sfid,
                            hp.dest_hub_sfid,
                            hp.window_idx,
                            hp.weekday_mask,
                            hp.travel_min,
                            hp.n_transfers,
                            hp.first_dep_s,
                            hp.arr_s,
                            hp.trip_chain,
                            ho.station_name AS origin_name,
                            hd.station_name AS dest_name,
                            ST_MakeLine(
                                ST_Point(so.station_lon, so.station_lat),
                                ST_Point(sd.station_lon, sd.station_lat)
                            ) AS geometry
                        FROM hp
                        JOIN hubs ho ON ho.station_feature_id = hp.origin_hub_sfid
                        JOIN hubs hd ON hd.station_feature_id = hp.dest_hub_sfid
                        JOIN st so   ON so.station_feature_id = hp.origin_hub_sfid
                        JOIN st sd   ON sd.station_feature_id = hp.dest_hub_sfid
                    ) TO '{intermediate}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """)
                con.close()
                # Bake to PMTiles.
                query = f"""
                    SELECT osm_id, geometry, theme,
                           origin_hub_sfid, dest_hub_sfid,
                           window_idx, weekday_mask,
                           travel_min, n_transfers,
                           first_dep_s, arr_s, trip_chain,
                           origin_name, dest_name
                    FROM read_parquet('{intermediate}')
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-graph-hubpairs",
                    min_zoom=0,
                    max_zoom=0,
                    drop_rate=None,
                    simplification=True,
                    coalesce=False,
                )
                log.info("bake_hubpairs_pmtiles: %s (%d bytes)", out, out.stat().st_size)
                return str(out)

            @task
            def bake_isochrones_pmtiles(rings_path: str) -> str:
                """Convex-hull the per-hub band assignments into rings
                and bake as theme='chrono' + theme='chrono-origin'
                features. For the de-risk pass we emit a simpler
                point-cloud-by-band representation; the convex-hull /
                ring-difference is deferred until needed by the ISMA
                map cell."""
                import duckdb
                import freestiler
                TILES_OUT.mkdir(parents=True, exist_ok=True)
                out = TILES_OUT / "austria-graph-isochrones.pmtiles"
                if not _needs_regen(out):
                    log.info("bake_isochrones_pmtiles: cached %s", out)
                    return str(out)
                stations_pq = str(TRANSIT / "stations.parquet")
                hubs_pq = str(TRANSIT / "optimal_hubs.parquet")
                intermediate = TILES_WORK / "austria-graph-isochrones-paths.parquet"
                con = duckdb.connect()
                con.sql("INSTALL spatial; LOAD spatial;")
                con.sql(f"""
                    COPY (
                        WITH iso AS (
                            SELECT * FROM read_parquet('{rings_path}')
                        ),
                        dst AS (
                            SELECT station_feature_id, station_lon, station_lat
                            FROM read_parquet('{stations_pq}')
                        ),
                        org AS (
                            SELECT h.station_feature_id, s.station_lon, s.station_lat, h.selection_order
                            FROM read_parquet('{hubs_pq}') h
                            JOIN read_parquet('{stations_pq}') s
                              USING (station_feature_id)
                        )
                        SELECT
                            iso.origin_sfid || '->' || iso.dest_sfid AS osm_id,
                            'chrono' AS theme,
                            iso.origin_sfid,
                            iso.dest_sfid,
                            iso.band_h,
                            iso.travel_seconds,
                            ST_Point(d.station_lon, d.station_lat) AS geometry
                        FROM iso
                        JOIN dst d ON d.station_feature_id = iso.dest_sfid
                        UNION ALL
                        SELECT
                            'origin/' || org.station_feature_id AS osm_id,
                            'chrono-origin' AS theme,
                            org.station_feature_id AS origin_sfid,
                            org.station_feature_id AS dest_sfid,
                            0 AS band_h,
                            0 AS travel_seconds,
                            ST_Point(org.station_lon, org.station_lat) AS geometry
                        FROM org
                    ) TO '{intermediate}' (FORMAT PARQUET, COMPRESSION SNAPPY)
                """)
                con.close()
                freestiler.freestile_query(
                    query=f"""
                        SELECT osm_id, geometry, theme,
                               origin_sfid, dest_sfid,
                               band_h, travel_seconds
                        FROM read_parquet('{intermediate}')
                    """,
                    output=str(out),
                    layer_name="austria-graph-isochrones",
                    min_zoom=0,
                    max_zoom=10,
                    drop_rate=None,
                    simplification=True,
                    coalesce=False,
                )
                log.info("bake_isochrones_pmtiles: %s (%d bytes)", out, out.stat().st_size)
                return str(out)

            @task
            def reload_martin(pmtiles_paths: list) -> list:
                """Restart martin so it picks up freshly-baked PMTiles.
                Martin caches mtime at startup; without this, the new
                austria-graph-* sources wouldn't appear in /catalog."""
                import fcntl
                import socket
                import subprocess
                import time as _time
                import urllib.request
                import json as _json
                expected = [
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
                        with socket.create_connection(("localhost", 3000), timeout=2):
                            break
                    except (ConnectionRefusedError, OSError, socket.timeout):
                        _time.sleep(0.5)
                else:
                    raise RuntimeError("martin port 3000 not reachable 30s after restart")
                with urllib.request.urlopen("http://localhost:3000/catalog", timeout=10) as _resp:
                    _catalog = _json.load(_resp)
                available = sorted(_catalog.get("tiles", {}).keys())
                missing = [s for s in expected if s not in available]
                if missing:
                    raise RuntimeError(
                        f"martin restart succeeded but sources still missing: {missing}"
                    )
                log.info("reload_martin: %d sources visible", len(expected))
                return pmtiles_paths

            # TaskFlow chaining.
            raw = download_gtfs()
            gtfs_parquet = parse_gtfs(raw)
            stations = match_stops_to_osm(gtfs_parquet)
            teg_dir = build_teg(stations, gtfs_parquet)
            hubs = compute_optimal_hubs_gpu(stations, teg_dir)
            hub_pairs = compute_hub_pair_routes_gpu(hubs, teg_dir)
            iso = compute_isochrones_gpu(hubs, teg_dir)
            routehub = compute_routehub_dataset(stations, hubs, gtfs_parquet)

            routes_tile = bake_routes_pmtiles(routehub)
            hubpairs_tile = bake_hubpairs_pmtiles(hub_pairs)
            iso_tile = bake_isochrones_pmtiles(iso)
            reload_martin([routes_tile, hubpairs_tile, iso_tile])


        notebook_austria_graph_pipeline()
    ''').lstrip())

    dag_files = [graph_dag_file]
    dag_ids = [graph_dag_id]
    return dag_files, dag_ids, graph_dag_file, graph_dag_id


@app.cell
def _trigger(graph_dag_file, graph_dag_id, os, requests, time):
    # Adopt-or-trigger the graph DAG run, then poll to terminal state.
    # Same shape as gtfs-austria.py's trigger cell (lines 4220-4347)
    # scoped to ONE DAG.
    _api = os.environ.get("AIRFLOW_API_INTERNAL_URL", "http://localhost:8080")
    _pwd = os.environ["AIRFLOW_ADMIN_PASSWORD"]

    def _http_with_retry(method, url, *, headers, json=None, timeout=10, retries=3):
        # 5xx retry with 1/2/4s backoff — Airflow's SQLite serialises
        # writes and a concurrent POST/GET can collide on the lock.
        # Principled back-pressure, not a sleep-loop (R4 distinction).
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
    _reg_deadline = time.monotonic() + 90
    while time.monotonic() < _reg_deadline:
        _r = requests.get(
            f"{_api}/api/v2/dags/{graph_dag_id}", headers=_auth, timeout=5,
        )
        if _r.status_code == 200:
            if _r.json().get("is_paused"):
                requests.patch(
                    f"{_api}/api/v2/dags/{graph_dag_id}",
                    headers=_auth, json={"is_paused": False}, timeout=5,
                )
                time.sleep(1)
                continue
            break
        time.sleep(2)
    else:
        raise RuntimeError(
            f"Airflow never registered DAG {graph_dag_id} from {graph_dag_file}"
        )

    # Phase 2 — adopt-or-trigger.
    _runs = _http_with_retry(
        "GET",
        f"{_api}/api/v2/dags/{graph_dag_id}/dagRuns?limit=10&order_by=-logical_date",
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
    if _adopt is None:
        _new = _http_with_retry(
            "POST",
            f"{_api}/api/v2/dags/{graph_dag_id}/dagRuns",
            headers=_auth,
            json={"conf": {}, "logical_date": _now.isoformat()},
            timeout=10,
        ).json()
        _run_id = _new["dag_run_id"]
    else:
        _run_id = _adopt["dag_run_id"]

    # Phase 3 — poll the target run until terminal state. 3600s (60 min)
    # covers a cold-cache GPU build; warm-cache adopt is ~0s.
    _poll_deadline = time.monotonic() + 3600
    _state = None
    while time.monotonic() < _poll_deadline:
        _state = _http_with_retry(
            "GET",
            f"{_api}/api/v2/dags/{graph_dag_id}/dagRuns/{_run_id}",
            headers=_auth,
            timeout=5,
        ).json()["state"]
        if _state in ("success", "failed"):
            break
        time.sleep(3)
    if _state not in ("success", "failed"):
        raise TimeoutError(f"DAG {graph_dag_id} did not finish in 60 min")
    if _state != "success":
        raise RuntimeError(f"DAG {graph_dag_id} ended {_state}")

    dag_run_states = {graph_dag_id: _state}
    dag_run_states
    return (dag_run_states,)


@app.cell
def _styles():
    # Per-cell MapLibre layer lists. Layer IDs are stable across re-runs;
    # the source `src` placeholder gets rebound to the martin source-name
    # by build_pipeline_maplibre_html.
    #
    # ROUTEBUILD_STYLE — for the route-builder map (RBUI). One vector
    # source (austria-graph-routes) provides theme='station' dots and
    # theme='trip' background lines; a second vector source
    # (austria-graph-hubpairs) provides the precomputed hub-pair
    # contraction-hierarchy table that the JS planner consumes.

    ROUTEBUILD_STYLE = [
        # Background trip lines (every rail trip, faded).
        {"id": "rb-trip-bg",
         "type": "line",
         "source": "src",
         "filter": ["==", ["get", "theme"], "trip"],
         "paint": {
             "line-color": "#9a9a9a",
             "line-width": 0.6,
             "line-opacity": 0.35,
         }},
        # Station dots (every rail-served station).
        {"id": "rb-station-dot",
         "type": "circle",
         "source": "src",
         "filter": ["==", ["get", "theme"], "station"],
         "paint": {
             "circle-color": "#ffffff",
             "circle-stroke-color": "#212121",
             "circle-stroke-width": 1.2,
             "circle-radius": [
                 "interpolate", ["linear"], ["zoom"],
                 5, 1.5, 8, 2.5, 12, 4.0,
             ],
         }},
        # Hub-pair LineStrings (one feature per (origin, dest, window,
        # weekday); coloured by travel_min — the JS planner toggles
        # opacity to surface only the currently-selected (window,
        # weekday) row.
        {"id": "rb-hubpair-line",
         "type": "line",
         "source": "hubpairs-src",
         "source-layer": "austria-graph-hubpairs",
         "paint": {
             "line-color": [
                 "interpolate", ["linear"], ["get", "travel_min"],
                 30,  "#1a9850",
                 90,  "#91cf60",
                 180, "#fee08b",
                 300, "#fc8d59",
                 600, "#d73027",
             ],
             "line-width": 0.0,    # JS planner sets line-width via
             "line-opacity": 0.0,  # setPaintProperty when a route is
         }},                       # picked.
        # Hub markers (top of stack).
        {"id": "rb-hub-marker",
         "type": "circle",
         "source": "src",
         "filter": ["all",
                    ["==", ["get", "theme"], "station"]],
         # The is_hub flag isn't on the routes tile yet; for v1 the JS
         # planner identifies hubs by membership in the loaded
         # hubpair-origin set and decorates them client-side. This
         # layer renders all stations identically — the JS planner
         # adds an overlay on top to highlight hubs.
         "paint": {
             "circle-color": "#ffffff",
             "circle-radius": 0.0,
         }},
    ]

    # CHRONO_STYLE — for the isochrones map (ISMA). Bucketed band-colored
    # dots over a flat background, with one origin marker per hub.
    CHRONO_STYLE = [
        {"id": "ch-band-dot",
         "type": "circle",
         "source": "src",
         "filter": ["==", ["get", "theme"], "chrono"],
         "paint": {
             "circle-color": [
                 "match", ["get", "band_h"],
                 1,  "#1a9850",
                 2,  "#66bd63",
                 3,  "#a6d96a",
                 4,  "#d9ef8b",
                 5,  "#ffffbf",
                 6,  "#fee08b",
                 8,  "#fdae61",
                 10, "#f46d43",
                 12, "#d73027",
                 "#888888",
             ],
             "circle-radius": [
                 "interpolate", ["linear"], ["zoom"],
                 5, 2.0, 8, 3.5, 12, 5.5,
             ],
             "circle-opacity": 0.7,
             "circle-stroke-color": "#212121",
             "circle-stroke-width": 0.3,
         }},
        {"id": "ch-origin-marker",
         "type": "circle",
         "source": "src",
         "filter": ["==", ["get", "theme"], "chrono-origin"],
         "paint": {
             "circle-color": "#1d3557",
             "circle-radius": 7.0,
             "circle-stroke-color": "#ffffff",
             "circle-stroke-width": 1.5,
         }},
    ]
    return CHRONO_STYLE, ROUTEBUILD_STYLE


@app.cell
def _isochrone_map(CHRONO_STYLE, dag_run_states, martin, mo):
    # Isochrones map (ISMA). Renders the rings.parquet -> PMTiles
    # output: point cloud colored by reachability band. Hub origins
    # shown as larger dark markers.
    mo.stop(
        dag_run_states.get("notebook_austria_graph_pipeline") != "success",
        mo.md("⏳ Waiting for DAG"),
    )
    isochrone_html = build_pipeline_maplibre_html(
        martin,
        "austria-graph-isochrones",
        layer_name="austria-graph-isochrones",
        center=[14.3, 47.6],   # rough Austria centroid
        zoom=6,
        style_layers=CHRONO_STYLE,
        source_maxzoom=10,
        satellite_background=False,
    )
    isochrone_map_view = mo.iframe(isochrone_html, height="540px")
    isochrone_map_view
    return (isochrone_map_view,)


@app.cell
def _route_builder_map(ROUTEBUILD_STYLE, dag_run_states, martin, mo):
    # Route builder map (RBUI). PURE JS + PMTiles — no kernel callbacks.
    # The injected JS:
    #   1. Loads `austria-graph-hubpairs` as a vector source, waits for
    #      it to settle, then walks every tile feature into an in-memory
    #      hubpair lookup table indexed by (origin, dest, window_idx,
    #      weekday_mask).
    #   2. Wires a UI panel: origin/dest hub dropdowns + 24-position
    #      hourly slider + Mon/Sat weekday toggle.
    #   3. On any change, runs `findRoute(...)`:
    #      - Hub→Hub: direct lookup in the table.
    #      - Non-hub endpoints: TODO first/last-mile composer
    #        (deferred; v1 only supports hub→hub).
    #      Renders the chosen alternative by toggling line-width on the
    #      rb-hubpair-line layer filtered to the picked feature_id.
    #   4. Renders an itinerary panel showing the leg list parsed from
    #      the route's trip_chain JSON.
    mo.stop(
        dag_run_states.get("notebook_austria_graph_pipeline") != "success",
        mo.md("⏳ Waiting for DAG"),
    )

    # The hubpairs source is loaded as a SECOND vector source via the
    # extra_sources kwarg (the helper's `src` slot remains the routes
    # tile so the station-dot + trip-line layers can use the default).
    extra_sources = {
        "hubpairs-src": {
            "type": "vector",
            "url": f"{martin}/austria-graph-hubpairs",
            "maxzoom": 0,
        },
    }

    panel_html = """
    <style>
      .rb-panel {
        position: absolute; top: 12px; left: 12px;
        background: rgba(255,255,255,0.96);
        border: 1px solid #999; border-radius: 6px;
        padding: 10px 12px; width: 320px;
        font: 13px/1.45 system-ui, sans-serif;
        box-shadow: 0 2px 8px rgba(0,0,0,0.18);
        z-index: 10;
      }
      .rb-panel h3 {
        margin: 0 0 8px 0; font-size: 14px;
        border-bottom: 1px solid #ddd; padding-bottom: 4px;
      }
      .rb-panel label { display: block; margin-top: 8px; font-weight: 600; font-size: 12px; }
      .rb-panel select, .rb-panel input[type=range] { width: 100%; margin-top: 2px; }
      .rb-panel .hour-display { font-variant-numeric: tabular-nums; font-weight: 600; color: #1d3557; }
      .rb-panel .summary { margin-top: 10px; padding-top: 8px; border-top: 1px solid #eee; }
      .rb-panel .legs { margin: 6px 0 0 0; padding-left: 18px; font-size: 12px; }
      .rb-panel .legs li { margin: 2px 0; }
      .rb-panel .badge { display: inline-block; padding: 1px 6px; border-radius: 4px; background: #1d3557; color: white; font-size: 11px; }
      .rb-panel .status { color: #888; font-style: italic; font-size: 11px; }
      .rb-panel button { padding: 3px 8px; font-size: 11px; cursor: pointer; }
    </style>
    <div class="rb-panel" id="rb-panel">
      <h3>Route Builder <span class="status" id="rb-loading">loading...</span></h3>
      <label>Origin hub</label>
      <select id="rb-origin"></select>
      <label>Destination hub</label>
      <select id="rb-dest"></select>
      <label>Depart window <span class="hour-display" id="rb-hour-label">12:00 – 13:00</span></label>
      <input type="range" id="rb-hour" min="0" max="23" value="12" step="1"/>
      <label>Weekday <button id="rb-wd-mon" class="badge">Mon</button> <button id="rb-wd-sat">Sat</button></label>
      <div class="summary" id="rb-summary">Pick origin + destination above.</div>
    </div>
    """

    extra_js = """
    // ---- 1. Load + index hub-pair routes from PMTiles ----
    const HUBPAIRS = {};
    const HUB_NAMES = new Map();   // sfid -> display name
    let LOAD_READY = false;
    let ACTIVE_WD = 1;             // Mon=1, Sat=32

    function indexFeature(props) {
      const k = props.origin_hub_sfid + '|' + props.dest_hub_sfid
              + '|' + props.window_idx + '|' + props.weekday_mask;
      HUBPAIRS[k] = props;
      if (props.origin_name) HUB_NAMES.set(props.origin_hub_sfid, props.origin_name);
      if (props.dest_name)   HUB_NAMES.set(props.dest_hub_sfid,   props.dest_name);
    }

    function harvestHubpairs() {
      const feats = window.map_austria_graph_routes.querySourceFeatures(
        'hubpairs-src', {sourceLayer: 'austria-graph-hubpairs'}
      );
      for (const f of feats) indexFeature(f.properties);
      // Once at least one feature seen we can publish the picker
      // options; harvest may complete in multiple iterations as more
      // tiles settle, but the z0 tile is fully loaded after first
      // `sourcedata` event.
      if (HUB_NAMES.size > 0 && !LOAD_READY) {
        publishPickerOptions();
        LOAD_READY = true;
        document.getElementById('rb-loading').textContent = '';
        runQuery();
      }
    }

    // ---- 2. UI wiring ----
    function publishPickerOptions() {
      const orig = document.getElementById('rb-origin');
      const dest = document.getElementById('rb-dest');
      const opts = [...HUB_NAMES.entries()].sort((a, b) => a[1].localeCompare(b[1]));
      for (const [sfid, name] of opts) {
        const o1 = document.createElement('option'); o1.value = sfid; o1.textContent = name;
        const o2 = document.createElement('option'); o2.value = sfid; o2.textContent = name;
        orig.appendChild(o1); dest.appendChild(o2);
      }
      // Default: Wien Hbf -> Salzburg Hbf if both present.
      const WIEN = 'way/423692233', SBG = 'node/619805688';
      if (HUB_NAMES.has(WIEN)) orig.value = WIEN;
      if (HUB_NAMES.has(SBG)) dest.value = SBG;
    }

    // ---- 3. The JS findRoute composer ----
    function findRoute(origin, dest, windowIdx, weekdayMask) {
      // Trivial hub-hub direct lookup. Non-hub endpoints + first/
      // last-mile composition is the next iteration's surface.
      const k = origin + '|' + dest + '|' + windowIdx + '|' + weekdayMask;
      const route = HUBPAIRS[k];
      if (!route) return null;
      // trip_chain is stored as a JSON string; parse it for the leg list.
      let legs = [];
      try { legs = JSON.parse(route.trip_chain); } catch (e) { /* malformed; show empty legs */ }
      return {
        travel_min: route.travel_min,
        n_transfers: route.n_transfers,
        first_dep_s: route.first_dep_s,
        arr_s: route.arr_s,
        legs: legs,
      };
    }

    function fmtTime(secs) {
      const s = Math.round(secs);
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      return String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0');
    }

    function runQuery() {
      if (!LOAD_READY) return;
      const origin = document.getElementById('rb-origin').value;
      const dest = document.getElementById('rb-dest').value;
      const windowIdx = parseInt(document.getElementById('rb-hour').value, 10);
      const summary = document.getElementById('rb-summary');
      const hourLabel = document.getElementById('rb-hour-label');
      hourLabel.textContent = String(windowIdx).padStart(2,'0') + ':00 - '
        + String((windowIdx + 1) % 24).padStart(2,'0') + ':00';

      if (origin === dest) {
        summary.innerHTML = '<em>Origin and destination are the same.</em>';
        highlightRoute(null);
        return;
      }
      const route = findRoute(origin, dest, windowIdx, ACTIVE_WD);
      if (!route) {
        const wdLabel = ACTIVE_WD === 1 ? 'Mon' : 'Sat';
        summary.innerHTML = `<em>No route found departing ${String(windowIdx).padStart(2,'0')}:00 on ${wdLabel}.</em>`;
        highlightRoute(null);
        return;
      }
      let html = '<div><span class="badge">' + route.travel_min + ' min</span> '
        + ' · ' + route.n_transfers + ' transfer' + (route.n_transfers === 1 ? '' : 's')
        + ' · depart ' + fmtTime(route.first_dep_s)
        + ' arrive ' + fmtTime(route.arr_s)
        + '</div>';
      if (route.legs && route.legs.length) {
        html += '<ol class="legs">';
        for (const leg of route.legs) {
          html += '<li>'
            + (HUB_NAMES.get(leg.board_sfid) || leg.board_sfid) + ' '
            + fmtTime(leg.dep_s)
            + ' → ' + (HUB_NAMES.get(leg.alight_sfid) || leg.alight_sfid) + ' '
            + fmtTime(leg.arr_s)
            + '</li>';
        }
        html += '</ol>';
      }
      summary.innerHTML = html;
      highlightRoute(origin + '|' + dest + '|' + windowIdx + '|' + ACTIVE_WD);
    }

    function highlightRoute(filterKey) {
      // Re-set the line-width filter to surface ONLY the picked
      // (origin, dest, window, weekday) row. Empty filter when no
      // route is picked.
      const m = window.map_austria_graph_routes;
      if (!filterKey) {
        m.setPaintProperty('rb-hubpair-line', 'line-width', 0.0);
        m.setPaintProperty('rb-hubpair-line', 'line-opacity', 0.0);
        return;
      }
      const [origin, dest, win, wd] = filterKey.split('|');
      m.setFilter('rb-hubpair-line', [
        'all',
        ['==', ['get', 'origin_hub_sfid'], origin],
        ['==', ['get', 'dest_hub_sfid'], dest],
        ['==', ['get', 'window_idx'], parseInt(win, 10)],
        ['==', ['get', 'weekday_mask'], parseInt(wd, 10)],
      ]);
      m.setPaintProperty('rb-hubpair-line', 'line-width', 4.5);
      m.setPaintProperty('rb-hubpair-line', 'line-opacity', 0.9);
    }

    // ---- 4. Inject UI panel + wire events ----
    const mapContainer = document.getElementById('map-austria-graph-routes');
    mapContainer.style.position = 'relative';
    const panelWrap = document.createElement('div');
    panelWrap.innerHTML = """ + repr(panel_html) + """;
    mapContainer.appendChild(panelWrap.firstElementChild);
    // panel is now in the DOM
    const wireSelect = id => document.getElementById(id).addEventListener('change', runQuery);
    wireSelect('rb-origin'); wireSelect('rb-dest');
    document.getElementById('rb-hour').addEventListener('input', runQuery);
    document.getElementById('rb-wd-mon').addEventListener('click', () => {
      ACTIVE_WD = 1;
      document.getElementById('rb-wd-mon').classList.add('badge');
      document.getElementById('rb-wd-sat').classList.remove('badge');
      runQuery();
    });
    document.getElementById('rb-wd-sat').addEventListener('click', () => {
      ACTIVE_WD = 32;
      document.getElementById('rb-wd-sat').classList.add('badge');
      document.getElementById('rb-wd-mon').classList.remove('badge');
      runQuery();
    });

    // ---- 5. Harvest hub-pair features whenever a new tile lands ----
    window.map_austria_graph_routes.on('sourcedata', (e) => {
      if (e.sourceId === 'hubpairs-src' && e.isSourceLoaded) {
        harvestHubpairs();
      }
    });
    window.map_austria_graph_routes.on('idle', () => {
      if (!LOAD_READY) harvestHubpairs();
    });
    """

    rb_html = build_pipeline_maplibre_html(
        martin,
        "austria-graph-routes",
        layer_name="austria-graph-routes",
        center=[14.3, 47.6],
        zoom=6,
        style_layers=ROUTEBUILD_STYLE,
        extra_sources=extra_sources,
        source_maxzoom=0,    # routes baked z0-only
        satellite_background=False,
        extra_js=extra_js,
    )
    route_builder_view = mo.iframe(rb_html, height="640px")
    route_builder_view
    return (route_builder_view,)


@app.cell
def _tail(dag_run_states, mo):
    # Trailing summary — expanded as cells below it land. For the
    # skeleton this is just the DAG status echo.
    _ok = all(s == "success" for s in dag_run_states.values())
    _badge = "✅ DAG green" if _ok else "🔴 DAG failed"
    mo.md(f"""
    ## Status

    {_badge} — `{list(dag_run_states.keys())[0]}` →
    `{list(dag_run_states.values())[0]}`

    **Pipeline progress** (skeleton → full):

    - Phase 1 (skeleton DAG + notebook shell) — **in progress**.
    - Phase 2 (GTFS ingest + OSM match) — pending.
    - Phase 3 (TEG + GPU hub selection) — pending.
    - Phase 4 (hub-pair SSSPs + tile bakes) — pending.
    - Phase 5 (JS route-builder map + slider UI) — pending.
    - Phase 6 (R10 Transitous gate + commit) — pending.

    The implementation order is documented in
    `/home/atrawog/.claude/plans/can-you-check-gpu-libraries-demo-py-breezy-owl.md`.
    """)
    return


if __name__ == "__main__":
    app.run()
