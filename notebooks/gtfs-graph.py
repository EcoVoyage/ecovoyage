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
    return airflow_public, martin


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
    # All tunable parameters in one place. Europe-scale architecture:
    # every constant chosen so the algorithm works at 30+× Austria size
    # without code changes.

    # Transitous Austria railway GTFS feed (first deploy target).
    GTFS_FEED_URL = (
        "https://api.transitous.org/gtfs/"
        "at_Railway-Current-Reference-Data-2026.gtfs.zip"
    )
    # Geofabrik Austria PBF (standalone OSM source — no osm-austria.py dep).
    PBF_URL = "https://download.geofabrik.de/europe/austria-latest.osm.pbf"

    # 1h depart windows tiling the 24h GTFS service-day (Europe/Vienna).
    WINDOWS = [(h * 3600, (h + 1) * 3600) for h in range(24)]
    WINDOW_LABELS = [f"{h:02d}-{(h+1):02d}" for h in range(24)]

    # Isochrone bands (hours reachable from a hub at 08:00 local depart).
    ISOCHRONE_BANDS_HOURS = [1, 2, 3, 4, 5, 6, 8, 10, 12]

    # Hub selection (cuGraph PageRank + connectivity-guarantee BFS pass).
    K_HUBS_TARGET = 24            # top-K by PageRank for Austria;
                                  # bump to ~200 for Europe scale.
    K_HUBS_MAX = 60               # absolute cap after connectivity pass

    # Partial Hub-Labeling: per non-hub station, store the K_LOCAL nearest
    # hubs. JS planner intersects origin's K_LOCAL labels with dest's to
    # find a viable hub-pair without scanning all K² combinations.
    K_LOCAL_HUBS = 8              # Austria: 8 × 1129 = 9k labels; Europe:
                                  # bump to 20 if needed for coverage.

    # TEG (RAPTOR-style nTr layering — transfer cap is structural).
    TEG_MAX_TRANSFERS = 4         # → 5 layers (nTr ∈ [0..4])
    TEG_TRANSFER_MIN_WAIT_S = 60  # plausible interchange floor
    TEG_TRANSFER_MAX_WAIT_S = 3600

    # Pattern-group compression: trips with same (route_id, runs_dow,
    # stop_seq_hash) collapse to one representative. Austria expects
    # ~10× compression (~500 patterns / ~5k trips); Europe similar.
    PATTERN_COMPRESS_ENABLED = True

    # R10 Transitous gate.
    VAL_N = 20                    # fresh OD pair sample size per run
    R10_FRESH_PAIRS = 21          # random pairs × VAL_WEEKDAYS × 24 windows = 1512 tests
    R10_CACHE_ONLY = True         # until further notice: no fresh MOTIS calls
    VAL_MAX_TRANSFERS = 4
    HARDFAIL_MIN_AHEAD_MIN = 60   # MOTIS faster by ≥60 min → HARD-FAIL
    SOFTFLAG_PCT = 20             # ±20% travel-time soft-flag band
    SOFTFLAG_TR_DELTA = 1
    VAL_MOTIS_OFFSETS_MIN = 20    # below this is a MOTIS OSM gap, not us

    # 3 representative 1h windows for the default gate run. Set env
    # R10_FULL_WINDOWS=1 to expand to all 24.
    VAL_WINDOWS_DEFAULT_HOURS = [7, 13, 19]    # morning / midday / evening peaks

    # MOTIS prod endpoint by default.
    MOTIS_BASE_PROD = "https://api.transitous.org/api/v5"
    MOTIS_BASE_STAGING = "https://staging.api.transitous.org/api/v5"

    # Cache + corpus paths (graph-suffixed so gtfs-austria.py's gate's
    # files don't collide during the validation window).
    R10_CACHE_DIR = "/workspace/.r10/transitous-cache-graph"
    R10_CORPUS_FILE = "/workspace/.r10/hardfail-corpus-graph.json"
    R10_CACHE_SCHEMA_VERSION = 7  # bumped from gtfs-austria.py's v=6

    # TEG + DAG-output cache locations.
    GRAPH_CACHE_DIR = "/workspace/cache/austria-teg"
    return


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
    # Verify the committed DAG file is present + has the right dag_id.
    # The DAG body is committed as a SEPARATE file at
    # `dags/notebook_austria_graph_pipeline.py` (not embedded in this
    # notebook). Both files are committed atomically per the cutover
    # plan; on fresh deploy, `git pull` brings both in together.
    #
    # Rationale: the DAG body is ~2000 LoC of dense GPU + worker-
    # subprocess code with nested triple-quoted string literals;
    # embedding it as a textwrap.dedent('''...''') in this cell
    # creates quote-escape headaches AND obscures the actual code
    # in marimo's UI. Co-committed-file is cleaner.
    dags_dir = Path(os.environ.get(
        "AIRFLOW_DAGS_DIR",
        os.path.expanduser("/workspace/dags"),
    ))
    dags_dir.mkdir(parents=True, exist_ok=True)

    graph_dag_id = "notebook_austria_graph_pipeline"
    graph_dag_file = dags_dir / f"{graph_dag_id}.py"

    if not graph_dag_file.exists():
        raise RuntimeError(
            f"DAG file missing at {graph_dag_file}. "
            "This notebook expects the DAG body to be committed at "
            "`dags/notebook_austria_graph_pipeline.py`; restore it "
            "from git or copy from the cutover commit."
        )
    body = graph_dag_file.read_text()
    if graph_dag_id not in body:
        raise RuntimeError(
            f"DAG file at {graph_dag_file} does not contain expected "
            f"dag_id={graph_dag_id!r}"
        )
    # No-op write — kept for callability symmetry with prior versions
    # that DID author the DAG body. Returns the same values downstream
    # cells reference.
    return graph_dag_file, graph_dag_id



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
        # Loader: always-false filter — forces MapLibre to fetch the
        # src tiles so querySourceFeatures sees them.
        {"id": "rb-loader",
         "type": "line",
         "source": "src",
         "filter": ["==", ["get", "theme"], "__never__"],
         "paint": {"line-width": 0.0}},
        # Background trip lines (faded)
        {"id": "rb-trip-bg",
         "type": "line",
         "source": "src",
         "filter": ["==", ["get", "theme"], "trip"],
         "paint": {"line-color": "#9a9a9a",
                   "line-width": 0.6, "line-opacity": 0.32}},
        # Non-hub station dots — small, white-fill / dark-stroke
        {"id": "rb-station-dot",
         "type": "circle",
         "source": "src",
         "filter": ["all", ["==", ["get", "theme"], "station"],
                            ["==", ["get", "is_hub"], 0]],
         "paint": {"circle-color": "#ffffff",
                   "circle-stroke-color": "#1b3a5c",
                   "circle-stroke-width": 1.0,
                   "circle-radius": ["interpolate", ["linear"], ["zoom"],
                                     5, 1.4, 8, 2.4, 12, 4.0]}},
        # Station labels (minzoom 9, halo)
        {"id": "rb-station-label",
         "type": "symbol",
         "source": "src",
         "minzoom": 9,
         "filter": ["==", ["get", "theme"], "station"],
         "layout": {"text-field": ["get", "station_name"],
                    "text-font": ["Noto Sans Regular"],
                    "text-size": ["interpolate", ["linear"], ["zoom"],
                                  9, 9, 11, 11, 13, 13],
                    "text-offset": [0, 1.0],
                    "text-anchor": "top",
                    "text-padding": 4},
         "paint": {"text-color": "#1b3a5c",
                   "text-halo-color": "#ffffff",
                   "text-halo-width": 1.4}},
        # Hub markers — orange-filled, larger
        {"id": "rb-hub-marker",
         "type": "circle",
         "source": "src",
         "filter": ["all", ["==", ["get", "theme"], "station"],
                            ["==", ["get", "is_hub"], 1]],
         "paint": {"circle-color": "#e76f51",
                   "circle-stroke-color": "#1b3a5c",
                   "circle-stroke-width": 1.6,
                   "circle-radius": ["interpolate", ["linear"], ["zoom"],
                                     4, 3.2, 8, 5.0, 12, 8.0]}},
        # Hub-pair LineStrings — JS sets filter + paint to surface
        # selected route only.
        {"id": "rb-hubpair-line",
         "type": "line",
         "source": "hubpairs-src",
         "source-layer": "austria-graph-hubpairs",
         "filter": ["==", ["get", "osm_id"], "__never__"],
         "paint": {"line-color": ["interpolate", ["linear"], ["get", "travel_min"],
                                  30, "#1a9850", 90, "#91cf60", 180, "#fee08b",
                                  300, "#fc8d59", 600, "#d73027"],
                   "line-width": 0.0, "line-opacity": 0.0}},
        # Client-injected selected route (FeatureCollection from JS)
        {"id": "route-leg-casing",
         "type": "line",
         "source": "route-src",
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": "#ffffff",
                   "line-width": ["interpolate", ["linear"], ["zoom"],
                                  3, 4.0, 11, 9.0],
                   "line-opacity": 0.85}},
        {"id": "route-leg",
         "type": "line",
         "source": "route-src",
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": "#1b5fa8",
                   "line-width": ["interpolate", ["linear"], ["zoom"],
                                  3, 2.0, 11, 5.0],
                   "line-opacity": 0.95}},
        # Pick pins
        {"id": "route-pick",
         "type": "circle",
         "source": "route-pick-src",
         "paint": {"circle-color": "#ffcc00",
                   "circle-stroke-color": "#3a2700",
                   "circle-stroke-width": 2.6, "circle-radius": 9}},
        {"id": "route-pick-label",
         "type": "symbol",
         "source": "route-pick-src",
         "layout": {"text-field": ["get", "label"],
                    "text-font": ["Noto Sans Regular"],
                    "text-size": 12, "text-allow-overlap": True},
         "paint": {"text-color": "#3a2700"}},
    ]

    # CHRONO_STYLE — isochrones map (polygon fills + origin markers).
    CHRONO_STYLE = [
        {"id": "ch-band-fill",
         "type": "fill",
         "source": "src",
         "filter": ["==", ["get", "theme"], "chrono"],
         "paint": {"fill-color": ["match", ["get", "band_h"],
                                  1, "#1a9850", 2, "#66bd63", 3, "#a6d96a",
                                  4, "#d9ef8b", 5, "#ffffbf", 6, "#fee08b",
                                  8, "#fdae61", 10, "#f46d43", 12, "#d73027",
                                  "#888888"],
                   "fill-opacity": 0.55}},
        {"id": "ch-band-outline",
         "type": "line",
         "source": "src",
         "filter": ["==", ["get", "theme"], "chrono"],
         "paint": {"line-color": "#1b3a5c", "line-width": 0.4,
                   "line-opacity": 0.4}},
        {"id": "ch-origin-marker",
         "type": "circle",
         "source": "src",
         "filter": ["==", ["get", "theme"], "chrono-origin"],
         "paint": {"circle-color": "#1d3557", "circle-radius": 8.0,
                   "circle-stroke-color": "#ffffff", "circle-stroke-width": 1.8}},
    ]

    # FASTLINK_STYLE — fastest-connections map.
    FASTLINK_STYLE = [
        {"id": "fl-hubpair-bg",
         "type": "line",
         "source": "src",
         "filter": ["==", ["get", "theme"], "hubpair"],
         "paint": {"line-color": ["interpolate", ["linear"], ["get", "travel_min"],
                                  30, "#1a9850", 90, "#91cf60", 180, "#fee08b",
                                  300, "#fc8d59", 600, "#d73027"],
                   "line-width": 1.0, "line-opacity": 0.18}},
        {"id": "fl-hubpair-fg",
         "type": "line",
         "source": "src",
         "filter": ["all", ["==", ["get", "theme"], "hubpair"],
                            ["==", ["get", "osm_id"], "__never__"]],
         "paint": {"line-color": ["interpolate", ["linear"], ["get", "travel_min"],
                                  30, "#1a9850", 90, "#91cf60", 180, "#fee08b",
                                  300, "#fc8d59", 600, "#d73027"],
                   "line-width": 3.5, "line-opacity": 0.9}},
    ]
    return CHRONO_STYLE, FASTLINK_STYLE, ROUTEBUILD_STYLE


@app.cell
def _isochrone_map(CHRONO_STYLE, dag_run_states, martin, mo, versatiles_assets):
    # Isochrones map (ISMA). Renders austria-graph-isochrones PMTiles —
    # polygon ring fills coloured by band_h (1h..12h reachable from each
    # hub at depart=08:00 Wed). Hub-selector dropdown filters by origin.
    mo.stop(
        dag_run_states.get("notebook_austria_graph_pipeline") != "success",
        mo.md("⏳ Waiting for DAG"),
    )

    iso_panel_html = """
    <style>
      .iso-panel { position: absolute; top: 12px; left: 12px;
                   background: rgba(255,255,255,0.96);
                   border: 1px solid #999; border-radius: 6px;
                   padding: 10px 12px; width: 320px;
                   font: 13px/1.45 system-ui, sans-serif;
                   box-shadow: 0 2px 8px rgba(0,0,0,0.18); z-index: 10; }
      .iso-panel h3 { margin: 0 0 8px 0; font-size: 14px;
                      border-bottom: 1px solid #ddd; padding-bottom: 4px; }
      .iso-panel label { display: block; margin-top: 8px;
                         font-weight: 600; font-size: 12px; }
      .iso-panel select { width: 100%; }
      .iso-panel .bands { font-family: monospace; font-size: 11px;
                          margin-top: 8px; }
      .iso-panel .band { display: inline-block; width: 12px; height: 12px;
                         vertical-align: middle; margin-right: 4px;
                         border: 1px solid #333; }
    </style>
    <div class="iso-panel">
      <h3>Isochrones — depart 08:00 Wed</h3>
      <label>Hub origin</label>
      <select id="iso-origin"><option value="">All hubs</option></select>
      <div class="bands">
        <span class="band" style="background:#1a9850"></span>1h
        <span class="band" style="background:#a6d96a"></span>3h
        <span class="band" style="background:#fee08b"></span>6h
        <span class="band" style="background:#fdae61"></span>8h
        <span class="band" style="background:#d73027"></span>12h
      </div>
    </div>
    """

    iso_extra_js = """
    const ORIGINS = new Map();
    let ISO_LOADED = false;
    function harvestIsoOrigins() {
      const m = window.map_austria_graph_isochrones;
      const feats = m.querySourceFeatures('src', {sourceLayer: 'austria-graph-isochrones'});
      for (const f of feats) {
        if (f.properties.theme === 'chrono-origin') {
          ORIGINS.set(f.properties.origin_hub_idx,
                      'hub ' + f.properties.origin_hub_idx);
        }
      }
      if (ORIGINS.size > 0 && !ISO_LOADED) {
        const sel = document.getElementById('iso-origin');
        for (const [k, name] of [...ORIGINS.entries()].sort((a, b) => a[0] - b[0])) {
          const o = document.createElement('option');
          o.value = String(k); o.textContent = name;
          sel.appendChild(o);
        }
        ISO_LOADED = true;
        sel.addEventListener('change', updateIsoFilter);
      }
    }
    function updateIsoFilter() {
      const m = window.map_austria_graph_isochrones;
      const val = document.getElementById('iso-origin').value;
      const filt = val === ''
        ? ['==', ['get', 'theme'], 'chrono']
        : ['all', ['==', ['get', 'theme'], 'chrono'],
                    ['==', ['get', 'origin_hub_idx'], parseInt(val, 10)]];
      m.setFilter('ch-band-fill', filt);
      m.setFilter('ch-band-outline', filt);
      const omfilt = val === ''
        ? ['==', ['get', 'theme'], 'chrono-origin']
        : ['all', ['==', ['get', 'theme'], 'chrono-origin'],
                    ['==', ['get', 'origin_hub_idx'], parseInt(val, 10)]];
      m.setFilter('ch-origin-marker', omfilt);
    }
    const cont = document.getElementById('map-austria-graph-isochrones');
    cont.style.position = 'relative';
    const wrap = document.createElement('div');
    wrap.innerHTML = """ + repr(iso_panel_html) + """;
    while (wrap.firstChild) cont.appendChild(wrap.firstChild);
    window.map_austria_graph_isochrones.on('sourcedata', (e) => {
      if (e.sourceId === 'src' && e.isSourceLoaded) harvestIsoOrigins();
    });
    window.map_austria_graph_isochrones.on('idle', harvestIsoOrigins);
    """

    isochrone_html = build_pipeline_maplibre_html(
        martin,
        "austria-graph-isochrones",
        layer_name="austria-graph-isochrones",
        center=[14.3, 47.6],
        zoom=6,
        style_layers=CHRONO_STYLE,
        source_maxzoom=10,
        satellite_background=False,
        glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
        extra_js=iso_extra_js,
    )
    isochrone_map_view = mo.iframe(isochrone_html, height="600px")
    isochrone_map_view
    return


@app.cell
def _fastest_connections_map(FASTLINK_STYLE, dag_run_states, martin, mo, versatiles_assets):
    # Fastest-connections map (FLMA). Visualises austria-graph-hubpairs
    # as colored polylines (background = all hub-hub routes faded;
    # foreground = selected origin's outbound routes highlighted).
    # Hour slider (24×1h) + weekday picker filter the highlight.
    mo.stop(
        dag_run_states.get("notebook_austria_graph_pipeline") != "success",
        mo.md("⏳ Waiting for DAG"),
    )

    fl_panel_html = """
    <style>
      .fl-panel { position: absolute; top: 12px; left: 12px;
                  background: rgba(255,255,255,0.96);
                  border: 1px solid #999; border-radius: 6px;
                  padding: 10px 12px; width: 340px;
                  font: 13px/1.45 system-ui, sans-serif;
                  box-shadow: 0 2px 8px rgba(0,0,0,0.18); z-index: 10; }
      .fl-panel h3 { margin: 0 0 8px 0; font-size: 14px;
                     border-bottom: 1px solid #ddd; padding-bottom: 4px; }
      .fl-panel label { display: block; margin-top: 8px;
                        font-weight: 600; font-size: 12px; }
      .fl-panel select, .fl-panel input[type=range] { width: 100%; }
      .fl-panel .hour-display { font-variant-numeric: tabular-nums;
                                font-weight: 600; color: #1d3557; }
      .fl-panel .wd-btn { padding: 2px 6px; font-size: 11px; cursor: pointer;
                          margin-right: 2px; border: 1px solid #999;
                          background: #f7f7f7; }
      .fl-panel .wd-btn.active { background: #1d3557; color: white;
                                  border-color: #1d3557; }
      .fl-panel .summary { margin-top: 10px; padding-top: 8px;
                           border-top: 1px solid #eee; font-size: 12px; }
    </style>
    <div class="fl-panel">
      <h3>Fastest connections</h3>
      <label>Origin hub</label>
      <select id="fl-origin"><option value="">— pick a hub —</option></select>
      <label>Depart window <span class="hour-display" id="fl-hour-label">13:00 - 14:00</span></label>
      <input type="range" id="fl-hour" min="0" max="23" value="13" step="1"/>
      <label>Weekday
        <button class="wd-btn active" data-w="1">Mon</button>
        <button class="wd-btn" data-w="2">Tue</button>
        <button class="wd-btn" data-w="4">Wed</button>
        <button class="wd-btn" data-w="8">Thu</button>
        <button class="wd-btn" data-w="16">Fri</button>
        <button class="wd-btn" data-w="32">Sat</button>
        <button class="wd-btn" data-w="64">Sun</button>
      </label>
      <div class="summary" id="fl-summary">Pick a hub to highlight its outbound routes.</div>
    </div>
    """

    fl_extra_js = """
    const HUBS = new Map();
    let ACTIVE_WD_FL = 1;
    let FL_READY = false;
    function harvestHubs() {
      const m = window.map_austria_graph_hubpairs;
      const feats = m.querySourceFeatures('src', {sourceLayer: 'austria-graph-hubpairs'});
      for (const f of feats) {
        const p = f.properties;
        if (p.origin_hub_sfid && !HUBS.has(p.origin_hub_sfid)) {
          HUBS.set(p.origin_hub_sfid, {idx: p.origin_hub_idx,
                                        name: p.origin_name || p.origin_hub_sfid});
        }
        if (p.dest_hub_sfid && !HUBS.has(p.dest_hub_sfid)) {
          HUBS.set(p.dest_hub_sfid, {idx: p.dest_hub_idx,
                                      name: p.dest_name || p.dest_hub_sfid});
        }
      }
      if (HUBS.size > 0 && !FL_READY) {
        FL_READY = true;
        const sel = document.getElementById('fl-origin');
        for (const [sfid, info] of [...HUBS.entries()].sort(
            (a, b) => a[1].name.localeCompare(b[1].name))) {
          const o = document.createElement('option');
          o.value = sfid; o.textContent = info.name;
          sel.appendChild(o);
        }
        sel.addEventListener('change', flUpdate);
        document.getElementById('fl-hour').addEventListener('input', () => {
          const v = parseInt(document.getElementById('fl-hour').value, 10);
          document.getElementById('fl-hour-label').textContent =
            String(v).padStart(2,'0') + ':00 - '
            + String((v + 1) % 24).padStart(2,'0') + ':00';
          flUpdate();
        });
        document.querySelectorAll('.fl-panel .wd-btn').forEach(b => {
          b.addEventListener('click', () => {
            ACTIVE_WD_FL = parseInt(b.dataset.w, 10);
            document.querySelectorAll('.fl-panel .wd-btn').forEach(x => x.classList.remove('active'));
            b.classList.add('active');
            flUpdate();
          });
        });
      }
    }
    function flUpdate() {
      const m = window.map_austria_graph_hubpairs;
      const origin = document.getElementById('fl-origin').value;
      const window_idx = parseInt(document.getElementById('fl-hour').value, 10);
      const summary = document.getElementById('fl-summary');
      if (!origin) {
        m.setFilter('fl-hubpair-fg', ['==', ['get', 'osm_id'], '__never__']);
        summary.innerHTML = 'Pick a hub to highlight its outbound routes.';
        return;
      }
      const fg_filter = ['all',
        ['==', ['get', 'theme'], 'hubpair'],
        ['==', ['get', 'origin_hub_sfid'], origin],
        ['==', ['get', 'window_idx'], window_idx],
        ['==', ['get', 'weekday_mask'], ACTIVE_WD_FL]];
      m.setFilter('fl-hubpair-fg', fg_filter);
      const feats = m.querySourceFeatures('src', {
        sourceLayer: 'austria-graph-hubpairs',
        filter: ['all',
                 ['==', ['get', 'origin_hub_sfid'], origin],
                 ['==', ['get', 'window_idx'], window_idx],
                 ['==', ['get', 'weekday_mask'], ACTIVE_WD_FL]]});
      summary.innerHTML = '<b>' + feats.length + '</b> outbound from <b>'
        + (HUBS.get(origin)?.name || origin) + '</b> at ' + window_idx + ':00';
    }
    const fl_cont = document.getElementById('map-austria-graph-hubpairs');
    fl_cont.style.position = 'relative';
    const fl_wrap = document.createElement('div');
    fl_wrap.innerHTML = """ + repr(fl_panel_html) + """;
    while (fl_wrap.firstChild) fl_cont.appendChild(fl_wrap.firstChild);
    window.map_austria_graph_hubpairs.on('sourcedata', (e) => {
      if (e.sourceId === 'src' && e.isSourceLoaded) harvestHubs();
    });
    window.map_austria_graph_hubpairs.on('idle', harvestHubs);
    """

    flma_html = build_pipeline_maplibre_html(
        martin,
        "austria-graph-hubpairs",
        layer_name="austria-graph-hubpairs",
        center=[14.3, 47.6],
        zoom=6,
        style_layers=FASTLINK_STYLE,
        source_maxzoom=0,
        satellite_background=False,
        glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
        extra_js=fl_extra_js,
    )
    flma_view = mo.iframe(flma_html, height="600px")
    flma_view
    return


@app.cell
def _route_builder_map(ROUTEBUILD_STYLE, dag_run_states, martin, mo, versatiles_assets):
    # Route builder map (RBUI). PURE JS + PMTiles — no kernel callbacks.
    # Full first-mile + last-mile + hub-pair composer running in the
    # browser.
    #
    # Data sources:
    #   src           = austria-graph-routes  (theme='trip' + theme='station')
    #   hubpairs-src  = austria-graph-hubpairs (theme='hubpair' precomputed table)
    #   route-src     = client-injected GeoJSON FeatureCollection (selected route LineStrings)
    #   route-pick-src= client-injected GeoJSON (origin/dest pick pins with numbered labels)
    #
    # In-memory dicts built at page load:
    #   STATION_INFO[sfid] = {idx, is_hub, name, lon, lat, near}
    #   TRIP_BY_SFID[sfid] = [{trip_id, stops: [[sfid, arr_s, dep_s, is_hub], ...], runs_dow}, ...]
    #   HUBPAIRS[ohub_idx|dhub_idx|win_idx|weekday_bit] = {travel_min, n_transfers, first_dep_s, arr_s, trip_chain}
    #
    # findRoute(o_sfid, d_sfid, window_idx, weekday_bit) cases:
    #   - hub→hub direct lookup
    #   - hub→non-hub: hub-pair + last-mile
    #   - non-hub→hub: first-mile + hub-pair
    #   - non-hub→non-hub: cross-product (first-mile × hub-pair × last-mile)
    # Min by total travel time; tie-break by fewer transfers.
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

    # Empty GeoJSON sources injected client-side for the selected
    # route's polyline + the origin/dest pick pins.
    extra_sources = {
        "hubpairs-src": {
            "type": "vector",
            "url": f"{martin}/austria-graph-hubpairs",
            "maxzoom": 0,
        },
        "route-src": {
            "type": "geojson",
            "data": {"type": "FeatureCollection", "features": []},
        },
        "route-pick-src": {
            "type": "geojson",
            "data": {"type": "FeatureCollection", "features": []},
        },
    }

    panel_html = """
    <style>
      .rb-panel { position: absolute; top: 12px; left: 12px;
                  background: rgba(255,255,255,0.96);
                  border: 1px solid #999; border-radius: 6px;
                  padding: 10px 12px; width: 360px;
                  font: 13px/1.45 system-ui, sans-serif;
                  box-shadow: 0 2px 8px rgba(0,0,0,0.18); z-index: 10;
                  max-height: 90vh; overflow-y: auto; }
      .rb-panel h3 { margin: 0 0 8px 0; font-size: 14px;
                     border-bottom: 1px solid #ddd; padding-bottom: 4px; }
      .rb-panel label { display: block; margin-top: 8px;
                        font-weight: 600; font-size: 12px; }
      .rb-panel select, .rb-panel input[type=range] { width: 100%; margin-top: 2px; }
      .rb-panel .hour-display { font-variant-numeric: tabular-nums;
                                font-weight: 600; color: #1d3557; }
      .rb-panel .summary { margin-top: 10px; padding-top: 8px;
                           border-top: 1px solid #eee; font-size: 12px; }
      .rb-panel .legs { margin: 6px 0 0 0; padding-left: 18px; font-size: 12px; }
      .rb-panel .legs li { margin: 4px 0; }
      .rb-panel .badge { display: inline-block; padding: 1px 6px;
                         border-radius: 4px; background: #1d3557;
                         color: white; font-size: 11px; }
      .rb-panel .status { color: #888; font-style: italic; font-size: 11px; }
      .rb-panel .wd-btn { padding: 2px 6px; font-size: 11px; cursor: pointer;
                          margin-right: 2px; border: 1px solid #999;
                          background: #f7f7f7; }
      .rb-panel .wd-btn.active { background: #1d3557; color: white;
                                  border-color: #1d3557; }
      .rb-panel .help { font-size: 11px; color: #555; margin-top: 6px; }
      .rb-panel .trip-class { display: inline-block; width: 18px; height: 18px;
                              vertical-align: middle; font-size: 15px; }
    </style>
    <div class="rb-panel">
      <h3>Route Builder <span class="status" id="rb-loading">loading…</span></h3>
      <label>Origin station</label>
      <select id="rb-origin"></select>
      <label>Destination station</label>
      <select id="rb-dest"></select>
      <label>Depart window <span class="hour-display" id="rb-hour-label">13:00 - 14:00</span></label>
      <input type="range" id="rb-hour" min="0" max="23" value="13" step="1"/>
      <label>Weekday
        <button class="wd-btn active" data-w="1">Mon</button>
        <button class="wd-btn" data-w="2">Tue</button>
        <button class="wd-btn" data-w="4">Wed</button>
        <button class="wd-btn" data-w="8">Thu</button>
        <button class="wd-btn" data-w="16">Fri</button>
        <button class="wd-btn" data-w="32">Sat</button>
        <button class="wd-btn" data-w="64">Sun</button>
      </label>
      <div class="help">Click any station marker to set origin; shift-click to set destination.</div>
      <div class="summary" id="rb-summary">Pick origin + destination above or click stations on the map.</div>
    </div>
    """

    extra_js = """
    // =================================================================
    // STATE
    // =================================================================
    const STATION_INFO = {};       // sfid → {idx, is_hub, name, lon, lat, near[]}
    const TRIP_BY_SFID = {};       // sfid → [{trip_id, stops, runs_dow}, ...]
    const HUBPAIRS = {};           // 'oidx|didx|window|weekday' → hp props
    const HUB_BY_IDX = {};         // hub_idx → sfid
    let LOAD_READY = false;
    let LOADED_STATIONS = false;
    let LOADED_HUBPAIRS = false;
    let ACTIVE_WD = 1;             // Mon=1
    let CUR_WIN = 13;
    let CUR_ORIGIN = null;
    let CUR_DEST = null;
    const MIN_TRANSFER_S = 60;

    function fmtTime(secs) {
      const s = Math.round(secs);
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      return String(h % 24).padStart(2, '0') + ':' + String(m).padStart(2, '0')
             + (h >= 24 ? ' (+' + Math.floor(h / 24) + 'd)' : '');
    }

    // Trip-class emoji (matches gtfs-austria.py semantics)
    function emojiOf(rsn, kmh) {
      if (kmh && kmh > 150) return '🚄';
      if (!rsn) return '🚆';
      const head = String(rsn).trim().split(/\\s+/)[0].toUpperCase();
      if (/^S\\d/.test(head) || head === 'SB' || head === 'S') return '🚈';
      const T = {RJ:'🚄', RJX:'🚄', ICE:'🚄', TGV:'🚄', AVE:'🚄',
                 EC:'🚆', IC:'🚆', EN:'🚆', NJ:'🚆', D:'🚆',
                 EX:'🚆', REX:'🚆', R:'🚂', RB:'🚂'};
      return T[head] || '🚆';
    }

    // =================================================================
    // LOADERS
    // =================================================================
    function harvestStations() {
      const m = window.map_austria_graph_routes;
      const feats = m.querySourceFeatures('src', {sourceLayer: 'austria-graph-routes'});
      let nstations = 0, ntrips = 0;
      for (const f of feats) {
        const p = f.properties;
        if (p.theme === 'station') {
          if (!STATION_INFO[p.osm_id]) {
            const coord = f.geometry?.coordinates;
            const near = (p.nearest_hubs || '').split(',').filter(Boolean).map(x => parseInt(x, 10));
            STATION_INFO[p.osm_id] = {
              idx: p.station_idx, is_hub: !!p.is_hub,
              name: p.station_name || p.osm_id,
              lon: coord ? coord[0] : 0, lat: coord ? coord[1] : 0,
              near: near,
            };
            if (p.is_hub) HUB_BY_IDX[p.station_idx] = p.osm_id;
            nstations++;
          }
        } else if (p.theme === 'trip') {
          let stops;
          try { stops = JSON.parse(p.stops); } catch (e) { continue; }
          if (!Array.isArray(stops) || stops.length < 2) continue;
          for (const stop of stops) {
            const sfid = stop[0];
            if (!TRIP_BY_SFID[sfid]) TRIP_BY_SFID[sfid] = [];
            // Avoid duplicate trip entries
            if (!TRIP_BY_SFID[sfid].some(t => t.trip_id === p.osm_id)) {
              TRIP_BY_SFID[sfid].push({
                trip_id: p.osm_id,
                stops: stops,
                runs_dow: p.runs_dow,
                rsn: p.rsn || '',
                avg_kmh: p.avg_kmh || 0,
              });
              ntrips++;
            }
          }
        }
      }
      if (Object.keys(STATION_INFO).length > 100) {
        LOADED_STATIONS = true;
        maybeReady();
      }
    }

    function harvestHubpairs() {
      const m = window.map_austria_graph_routes;
      const feats = m.querySourceFeatures('hubpairs-src',
                                          {sourceLayer: 'austria-graph-hubpairs'});
      for (const f of feats) {
        const p = f.properties;
        const k = p.origin_hub_idx + '|' + p.dest_hub_idx + '|'
                  + p.window_idx + '|' + p.weekday_mask;
        if (!HUBPAIRS[k]) {
          HUBPAIRS[k] = {
            travel_min: p.travel_min, n_transfers: p.n_transfers,
            first_dep_s: p.first_dep_s, arr_s: p.arr_s,
            origin_hub_sfid: p.origin_hub_sfid, dest_hub_sfid: p.dest_hub_sfid,
            origin_hub_idx: p.origin_hub_idx, dest_hub_idx: p.dest_hub_idx,
          };
        }
      }
      if (Object.keys(HUBPAIRS).length > 100) {
        LOADED_HUBPAIRS = true;
        maybeReady();
      }
    }

    function maybeReady() {
      if (LOAD_READY) return;
      if (LOADED_STATIONS && LOADED_HUBPAIRS) {
        LOAD_READY = true;
        document.getElementById('rb-loading').textContent = '';
        publishPickerOptions();
        runQuery();
      } else {
        document.getElementById('rb-loading').textContent =
          'stations:' + Object.keys(STATION_INFO).length
          + ' hubpairs:' + Object.keys(HUBPAIRS).length;
      }
    }

    function publishPickerOptions() {
      const orig = document.getElementById('rb-origin');
      const dest = document.getElementById('rb-dest');
      orig.innerHTML = ''; dest.innerHTML = '';
      const entries = Object.entries(STATION_INFO)
        .sort((a, b) => a[1].name.localeCompare(b[1].name));
      for (const [sfid, info] of entries) {
        const tag = info.is_hub ? '★ ' : '';
        const o1 = document.createElement('option');
        o1.value = sfid; o1.textContent = tag + info.name;
        orig.appendChild(o1);
        const o2 = document.createElement('option');
        o2.value = sfid; o2.textContent = tag + info.name;
        dest.appendChild(o2);
      }
      // Default Wien Hbf → Salzburg Hbf
      const WIEN = 'way/423692233', SBG = 'node/619805688';
      if (STATION_INFO[WIEN]) orig.value = WIEN;
      if (STATION_INFO[SBG])  dest.value = SBG;
      CUR_ORIGIN = orig.value; CUR_DEST = dest.value;
    }

    // =================================================================
    // findRoute composer
    // =================================================================
    function findRoute(o_sfid, d_sfid, win_idx, wd_bit) {
      if (o_sfid === d_sfid) return null;
      const o = STATION_INFO[o_sfid], d = STATION_INFO[d_sfid];
      if (!o || !d) return null;
      const win_lo = win_idx * 3600, win_hi = win_lo + 3600;

      // --- Helper: compute trip leg from a trip's stops at a board+alight sequence
      function legFromTrip(trip, boardSfid, alightSfid) {
        const stops = trip.stops;
        let bi = -1, ai = -1;
        for (let i = 0; i < stops.length; i++) {
          if (stops[i][0] === boardSfid && bi < 0) bi = i;
          if (stops[i][0] === alightSfid && i > bi && bi >= 0) { ai = i; break; }
        }
        if (bi < 0 || ai < 0) return null;
        const leg_dep_s = stops[bi][2];
        const leg_arr_s = stops[ai][1];
        if (leg_arr_s <= leg_dep_s) return null;
        const segStops = stops.slice(bi, ai + 1).map(s => s[0]);
        return {trip_id: trip.trip_id, rsn: trip.rsn, kmh: trip.avg_kmh,
                board_sfid: boardSfid, alight_sfid: alightSfid,
                dep_s: leg_dep_s, arr_s: leg_arr_s, seg_stops: segStops,
                board_seq: bi, alight_seq: ai};
      }

      // Hub-pair scan helpers (window-locked lookups miss queries
      // whose 1h window contains no train but a later window does;
      // both the Python validator and the JS planner now scan ALL
      // 24 windows for the earliest at-or-after / best-before
      // hub-pair entry, matching MOTIS "earliest at or after the
      // query window" semantics).
      function findFirstHpAfter(oIdx, dIdx, wdBit, minFirstDep) {
        let best = null;
        for (let w = 0; w < 24; w++) {
          const hp = HUBPAIRS[oIdx + '|' + dIdx + '|' + w + '|' + wdBit];
          if (!hp) continue;
          if (hp.first_dep_s < minFirstDep) continue;
          if (best === null || hp.first_dep_s < best.first_dep_s) best = hp;
        }
        return best;
      }
      function findBestHpArrivingBefore(oIdx, dIdx, wdBit, latestArr, minFirstDep) {
        let best = null;
        for (let w = 0; w < 24; w++) {
          const hp = HUBPAIRS[oIdx + '|' + dIdx + '|' + w + '|' + wdBit];
          if (!hp) continue;
          if (hp.arr_s > latestArr) continue;
          if (hp.first_dep_s < minFirstDep) continue;
          if (best === null || hp.first_dep_s > best.first_dep_s) best = hp;
        }
        return best;
      }

      // --- 1. Hub-Hub direct (scan-forward variant)
      if (o.is_hub && d.is_hub) {
        const hp = findFirstHpAfter(o.idx, d.idx, wd_bit, win_lo);
        if (hp) {
          return {
            travel_min: hp.travel_min, n_transfers: hp.n_transfers,
            first_dep_s: hp.first_dep_s, arr_s: hp.arr_s,
            legs: [{kind: 'hub_pair', from: o_sfid, to: d_sfid,
                    dep_s: hp.first_dep_s, arr_s: hp.arr_s, n_transfers: hp.n_transfers}],
            kind: 'hub_hub',
          };
        }
        // Fall through to compose path
      }

      // --- 2. Build first-mile-by-alight-station: every onward stop
      //        (hub AND non-hub) is a candidate alight; non-hub
      //        transfers (Path B below) recover routes the hub-pair
      //        graph alone misses.
      const fmByAlight = {};
      const tripsAtO = TRIP_BY_SFID[o_sfid] || [];
      for (const t of tripsAtO) {
        if ((t.runs_dow & wd_bit) === 0) continue;
        const boardStop = t.stops.find(s => s[0] === o_sfid);
        if (!boardStop) continue;
        if (boardStop[2] < win_lo) continue;
        for (let j = t.stops.indexOf(boardStop) + 1; j < t.stops.length; j++) {
          const onwardStop = t.stops[j];
          if (onwardStop[1] <= boardStop[2]) continue;
          if (onwardStop[0] === o_sfid) continue;
          const leg = legFromTrip(t, o_sfid, onwardStop[0]);
          if (!leg) continue;
          const info = STATION_INFO[onwardStop[0]] || {};
          (fmByAlight[onwardStop[0]] = fmByAlight[onwardStop[0]] || []).push({
            hub_idx: onwardStop[3] === 1 ? info.idx : -1,
            alight_sfid: onwardStop[0],
            is_hub: onwardStop[3] === 1,
            leg,
          });
        }
      }

      // --- 3. Build last-mile-by-board-station: every prior stop.
      const lmByBoard = {};
      const tripsAtD = TRIP_BY_SFID[d_sfid] || [];
      for (const t of tripsAtD) {
        if ((t.runs_dow & wd_bit) === 0) continue;
        const alightStop = t.stops.find(s => s[0] === d_sfid);
        if (!alightStop) continue;
        for (let i = 0; i < t.stops.indexOf(alightStop); i++) {
          const prior = t.stops[i];
          if (prior[2] >= alightStop[1]) continue;
          if (prior[0] === d_sfid) continue;
          const leg = legFromTrip(t, prior[0], d_sfid);
          if (!leg) continue;
          const info = STATION_INFO[prior[0]] || {};
          (lmByBoard[prior[0]] = lmByBoard[prior[0]] || []).push({
            hub_idx: prior[3] === 1 ? info.idx : -1,
            board_sfid: prior[0],
            is_hub: prior[3] === 1,
            leg,
          });
        }
      }

      // Convenience hub-only lists for the hub-pair compose paths
      // (Path C / D below). Flattened from the by-station maps.
      const firstMile = [];
      for (const sfid in fmByAlight)
        for (const e of fmByAlight[sfid])
          if (e.is_hub)
            firstMile.push({hub_idx: e.hub_idx, hub_sfid: sfid, leg: e.leg});
      const lastMile = [];
      for (const sfid in lmByBoard)
        for (const e of lmByBoard[sfid])
          if (e.is_hub)
            lastMile.push({hub_idx: e.hub_idx, hub_sfid: sfid, leg: e.leg});

      let best = null;
      function tryBest(total, ntr, first_dep, arr, legs, kind) {
        if (!best || total < best._tot
            || (total === best._tot && ntr < best.n_transfers)) {
          best = {travel_min: Math.round(total / 60), n_transfers: ntr,
                  first_dep_s: first_dep, arr_s: arr, legs: legs,
                  _tot: total, kind: kind};
        }
      }

      // --- 4a. Path A: direct single trip o → d
      for (const fm of (fmByAlight[d_sfid] || [])) {
        const total = fm.leg.arr_s - fm.leg.dep_s;
        tryBest(total, 0, fm.leg.dep_s, fm.leg.arr_s, [fm.leg], 'direct');
      }

      // --- 4a'. Path B: single transfer at ANY shared station (hub
      //          or not). Set-intersection of fmByAlight ∩ lmByBoard
      //          minus the endpoints, paired by trip cross-product.
      for (const X in fmByAlight) {
        if (X === o_sfid || X === d_sfid) continue;
        if (!lmByBoard[X]) continue;
        for (const fm of fmByAlight[X]) {
          for (const lm of lmByBoard[X]) {
            if (lm.leg.dep_s < fm.leg.arr_s + MIN_TRANSFER_S) continue;
            const total = lm.leg.arr_s - fm.leg.dep_s;
            tryBest(total, 1, fm.leg.dep_s, lm.leg.arr_s,
                    [fm.leg, lm.leg], 'transfer');
          }
        }
      }

      // --- 4b. Path C1: Origin is hub → hub-pair → last-mile (hubs)
      if (o.is_hub) {
        for (const lm of lastMile) {
          if (lm.hub_idx === o.idx) continue;
          const hp = findBestHpArrivingBefore(
            o.idx, lm.hub_idx, wd_bit,
            lm.leg.dep_s - MIN_TRANSFER_S, win_lo,
          );
          if (!hp) continue;
          const total = lm.leg.arr_s - hp.first_dep_s;
          const ntr = (hp.n_transfers || 0) + 1;
          tryBest(total, ntr, hp.first_dep_s, lm.leg.arr_s, [
            {kind: 'hub_pair', from: o_sfid, to: HUB_BY_IDX[lm.hub_idx] || lm.hub_sfid,
             dep_s: hp.first_dep_s, arr_s: hp.arr_s, n_transfers: hp.n_transfers || 0},
            lm.leg,
          ], 'origin_hub');
        }
      }

      // --- 4c. Path C2: First-mile (hubs) → hub-pair → Dest is hub
      if (d.is_hub) {
        for (const fm of firstMile) {
          if (fm.hub_idx === d.idx) continue;
          const hp = findFirstHpAfter(
            fm.hub_idx, d.idx, wd_bit, fm.leg.arr_s + MIN_TRANSFER_S,
          );
          if (!hp) continue;
          const total = hp.arr_s - fm.leg.dep_s;
          const ntr = 1 + (hp.n_transfers || 0);
          tryBest(total, ntr, fm.leg.dep_s, hp.arr_s, [
            fm.leg,
            {kind: 'hub_pair', from: HUB_BY_IDX[fm.hub_idx] || fm.hub_sfid, to: d_sfid,
             dep_s: hp.first_dep_s, arr_s: hp.arr_s, n_transfers: hp.n_transfers || 0},
          ], 'dest_hub');
        }
      }

      // --- 4d. Path D: Both non-hub, fm-hub → hub-pair → lm-hub
      // (same-hub case already handled by Path B above.)
      if (!o.is_hub && !d.is_hub) {
        for (const fm of firstMile) {
          for (const lm of lastMile) {
            if (fm.hub_idx === lm.hub_idx) continue;
            const hp = findFirstHpAfter(
              fm.hub_idx, lm.hub_idx, wd_bit, fm.leg.arr_s + MIN_TRANSFER_S,
            );
            if (!hp) continue;
            if (lm.leg.dep_s < hp.arr_s + MIN_TRANSFER_S) continue;
            const total = lm.leg.arr_s - fm.leg.dep_s;
            const ntr = 2 + (hp.n_transfers || 0);
            tryBest(total, ntr, fm.leg.dep_s, lm.leg.arr_s, [
              fm.leg,
              {kind: 'hub_pair', from: HUB_BY_IDX[fm.hub_idx] || fm.hub_sfid,
               to: HUB_BY_IDX[lm.hub_idx] || lm.hub_sfid,
               dep_s: hp.first_dep_s, arr_s: hp.arr_s,
               n_transfers: hp.n_transfers || 0},
              lm.leg,
            ], 'compose');
          }
        }
      }

      // --- 4e. Path G: 2-hop transfer at non-hub intermediates.
      // o -> X -> Y -> d, where X and Y are different (possibly
      // non-hub) stations. Mirrors the Python Path G — picks the
      // LATEST fm with fm.arr_s + 60 <= trip2.x_dep (= no wait at
      // X = max fm.dep_s = min total travel).
      if (!o.is_hub && !d.is_hub) {
        const lm_keys = new Set(Object.keys(lmByBoard));
        const G_IT_CAP = 50000;
        let g_iters = 0;
        for (const X in fmByAlight) {
          if (X === d_sfid || X === o_sfid) continue;
          if (g_iters >= G_IT_CAP) break;
          const fms_at_X = fmByAlight[X];
          if (!fms_at_X || !fms_at_X.length) continue;
          const fms_sorted = [...fms_at_X].sort((a, b) => a.leg.arr_s - b.leg.arr_s);
          const trips_at_X = TRIP_BY_SFID[X] || [];
          for (const t of trips_at_X) {
            if (g_iters >= G_IT_CAP) break;
            if ((t.runs_dow & wd_bit) === 0) continue;
            const stops = t.stops;
            const xi = stops.findIndex(s => s[0] === X);
            if (xi < 0) continue;
            const x_dep = stops[xi][2];
            let latest_fm = null;
            for (const fm_at_X of fms_sorted) {
              if (fm_at_X.leg.arr_s + MIN_TRANSFER_S > x_dep) break;
              if (latest_fm === null || fm_at_X.leg.dep_s > latest_fm.leg.dep_s) {
                latest_fm = fm_at_X;
              }
            }
            if (latest_fm === null) continue;
            for (let j = xi + 1; j < stops.length; j++) {
              const Y = stops[j][0];
              if (!lm_keys.has(Y)) continue;
              if (Y === o_sfid || Y === d_sfid || Y === X) continue;
              const y_arr = stops[j][1];
              if (y_arr <= x_dep) continue;
              for (const lm_at_Y of lmByBoard[Y]) {
                g_iters++;
                if (lm_at_Y.leg.dep_s < y_arr + MIN_TRANSFER_S) continue;
                const total = lm_at_Y.leg.arr_s - latest_fm.leg.dep_s;
                tryBest(total, 2, latest_fm.leg.dep_s, lm_at_Y.leg.arr_s, [
                  latest_fm.leg,
                  {trip_id: t.trip_id, board_sfid: X, alight_sfid: Y,
                   dep_s: x_dep, arr_s: y_arr,
                   seg_stops: stops.slice(xi, j + 1).map(s => s[0])},
                  lm_at_Y.leg,
                ], 'transfer_2hop');
              }
            }
          }
        }
      }

      return best;
    }

    // =================================================================
    // RENDER
    // =================================================================
    function buildRouteGeoJSON(route) {
      if (!route) return {type: 'FeatureCollection', features: []};
      const features = [];
      for (const leg of route.legs) {
        let coords = [];
        if (leg.kind === 'hub_pair') {
          const from = STATION_INFO[leg.from], to = STATION_INFO[leg.to];
          if (!from || !to) continue;
          coords = [[from.lon, from.lat], [to.lon, to.lat]];
        } else {
          for (const sfid of (leg.seg_stops || [])) {
            const info = STATION_INFO[sfid];
            if (info) coords.push([info.lon, info.lat]);
          }
        }
        if (coords.length < 2) continue;
        features.push({type: 'Feature',
                       geometry: {type: 'LineString', coordinates: coords},
                       properties: {kind: leg.kind, trip_id: leg.trip_id || ''}});
      }
      return {type: 'FeatureCollection', features};
    }

    function buildPickGeoJSON() {
      const features = [];
      let i = 1;
      for (const sfid of [CUR_ORIGIN, CUR_DEST]) {
        if (!sfid) { i++; continue; }
        const info = STATION_INFO[sfid];
        if (!info) { i++; continue; }
        features.push({type: 'Feature',
                       geometry: {type: 'Point', coordinates: [info.lon, info.lat]},
                       properties: {label: String(i)}});
        i++;
      }
      return {type: 'FeatureCollection', features};
    }

    function renderSummary(route) {
      const summary = document.getElementById('rb-summary');
      if (!route) {
        summary.innerHTML = '<em>No route found in this window/weekday. '
          + 'Try a different time or weekday.</em>';
        return;
      }
      let html = '<div><span class="badge">' + route.travel_min + ' min</span>'
        + ' · ' + route.n_transfers + ' transfer'
        + (route.n_transfers === 1 ? '' : 's')
        + ' · depart ' + fmtTime(route.first_dep_s)
        + ' arrive ' + fmtTime(route.arr_s)
        + '</div>';
      html += '<ol class="legs">';
      for (const leg of route.legs) {
        if (leg.kind === 'hub_pair') {
          const from = STATION_INFO[leg.from], to = STATION_INFO[leg.to];
          html += '<li><span class="trip-class">🔁</span> '
            + (from?.name || leg.from)
            + ' <small>' + fmtTime(leg.dep_s) + '</small>'
            + ' → ' + (to?.name || leg.to)
            + ' <small>' + fmtTime(leg.arr_s) + '</small>'
            + ' <em>(hub-pair · ' + leg.n_transfers + ' transfer'
            + (leg.n_transfers === 1 ? '' : 's') + ')</em></li>';
        } else {
          const from = STATION_INFO[leg.board_sfid];
          const to = STATION_INFO[leg.alight_sfid];
          html += '<li><span class="trip-class">' + emojiOf(leg.rsn, leg.kmh) + '</span> '
            + (leg.rsn ? '<b>' + leg.rsn + '</b> ' : '')
            + (from?.name || leg.board_sfid)
            + ' <small>' + fmtTime(leg.dep_s) + '</small>'
            + ' → ' + (to?.name || leg.alight_sfid)
            + ' <small>' + fmtTime(leg.arr_s) + '</small></li>';
        }
      }
      html += '</ol>';
      summary.innerHTML = html;
    }

    function runQuery() {
      const m = window.map_austria_graph_routes;
      if (!LOAD_READY) return;
      const o = document.getElementById('rb-origin').value;
      const d = document.getElementById('rb-dest').value;
      const winIdx = parseInt(document.getElementById('rb-hour').value, 10);
      CUR_ORIGIN = o; CUR_DEST = d; CUR_WIN = winIdx;
      const hourLabel = document.getElementById('rb-hour-label');
      hourLabel.textContent = String(winIdx).padStart(2,'0') + ':00 - '
        + String((winIdx + 1) % 24).padStart(2,'0') + ':00';
      m.getSource('route-pick-src').setData(buildPickGeoJSON());
      if (o === d) {
        document.getElementById('rb-summary').innerHTML =
          '<em>Origin and destination are the same.</em>';
        m.getSource('route-src').setData({type: 'FeatureCollection', features: []});
        return;
      }
      const route = findRoute(o, d, winIdx, ACTIVE_WD);
      renderSummary(route);
      m.getSource('route-src').setData(buildRouteGeoJSON(route));
    }

    // =================================================================
    // WIRING
    // =================================================================
    const mapContainer = document.getElementById('map-austria-graph-routes');
    mapContainer.style.position = 'relative';
    const panelWrap = document.createElement('div');
    panelWrap.innerHTML = """ + repr(panel_html) + """;
    // Append ALL children (the panel HTML contains both <style> and
    // <div class="rb-panel">, not just one root element).
    while (panelWrap.firstChild) mapContainer.appendChild(panelWrap.firstChild);

    const m = window.map_austria_graph_routes;
    m.on('sourcedata', (e) => {
      if (e.sourceId === 'src' && e.isSourceLoaded) harvestStations();
      if (e.sourceId === 'hubpairs-src' && e.isSourceLoaded) harvestHubpairs();
    });
    m.on('idle', () => {
      if (!LOADED_STATIONS) harvestStations();
      if (!LOADED_HUBPAIRS) harvestHubpairs();
    });

    document.getElementById('rb-origin').addEventListener('change', runQuery);
    document.getElementById('rb-dest').addEventListener('change', runQuery);
    document.getElementById('rb-hour').addEventListener('input', runQuery);
    document.querySelectorAll('.rb-panel .wd-btn').forEach(b => {
      b.addEventListener('click', () => {
        ACTIVE_WD = parseInt(b.dataset.w, 10);
        document.querySelectorAll('.rb-panel .wd-btn').forEach(x => x.classList.remove('active'));
        b.classList.add('active');
        runQuery();
      });
    });

    // Click on station marker → set origin (shift-click = dest)
    m.on('click', ['rb-station-dot', 'rb-hub-marker'], (e) => {
      const f = e.features?.[0]; if (!f) return;
      const sfid = f.properties.osm_id;
      const isShift = !!(e.originalEvent && e.originalEvent.shiftKey);
      if (isShift) {
        CUR_DEST = sfid;
        document.getElementById('rb-dest').value = sfid;
      } else {
        CUR_ORIGIN = sfid;
        document.getElementById('rb-origin').value = sfid;
      }
      runQuery();
    });
    m.on('mouseenter', ['rb-station-dot', 'rb-hub-marker'], () => {
      m.getCanvas().style.cursor = 'pointer';
    });
    m.on('mouseleave', ['rb-station-dot', 'rb-hub-marker'], () => {
      m.getCanvas().style.cursor = '';
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
        source_maxzoom=0,
        satellite_background=False,
        glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
        extra_js=extra_js,
    )
    route_builder_view = mo.iframe(rb_html, height="720px")
    route_builder_view
    return


@app.cell
def _validate_routes_against_transitous(
    HARDFAIL_MIN_AHEAD_MIN, MOTIS_BASE_PROD, MOTIS_BASE_STAGING,
    R10_CACHE_DIR, R10_CACHE_ONLY, R10_CACHE_SCHEMA_VERSION,
    R10_CORPUS_FILE, R10_FRESH_PAIRS,
    SOFTFLAG_PCT, SOFTFLAG_TR_DELTA, VAL_MAX_TRANSFERS,
    VAL_MOTIS_OFFSETS_MIN, VAL_N, VAL_WINDOWS_DEFAULT_HOURS,
    dag_run_states, mo, os, pl,
):
    # R10 Transitous gate. Compares our JS-mirror Python composer
    # against MOTIS /api/v5/plan for 60 fresh OD pairs (date-seeded)
    # + every entry in the persistent hardfail-corpus-graph.json.
    # 3 representative 1h windows per pair (07-08, 13-14, 19-20).
    # Cache + corpus + evidence files all `-graph` suffixed.
    mo.stop(
        dag_run_states.get("notebook_austria_graph_pipeline") != "success",
        mo.md("⏳ R10 gate waits for DAG green."),
    )
    import hashlib
    import json as _j
    import math
    import random
    import subprocess
    import urllib.parse
    import urllib.request
    # Private-prefix to avoid marimo's "Variable defined in multiple
    # cells" lint trip (these clash with the imports cell's Path /
    # the trigger cell's datetime/timezone).
    from datetime import datetime as _datetime
    from datetime import timedelta as _timedelta
    from datetime import timezone as _timezone
    from pathlib import Path as _Path

    import pyarrow.parquet as papq

    _CACHE_DIR = _Path(R10_CACHE_DIR)
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _CORPUS = _Path(R10_CORPUS_FILE)
    _CORPUS.parent.mkdir(parents=True, exist_ok=True)

    _MOTIS_BASE = os.environ.get("TRANSITOUS_ENV") == "staging" and MOTIS_BASE_STAGING or MOTIS_BASE_PROD
    _RUN_ALL_WINDOWS = os.environ.get("R10_FULL_WINDOWS") == "1"

    # User-Agent per Transitous AUP (contact info mandatory)
    try:
        _email = subprocess.check_output(
            ["git", "config", "user.email"], stderr=subprocess.DEVNULL,
        ).decode().strip() or "unknown"
    except Exception:
        _email = "unknown"
    _UA = f"ecovoyage-r10-gate/2026.05 ({_email})"

    # ---- Load post-DAG artifacts ----
    _CACHE = _Path("/workspace/cache/austria-teg")
    stations_df = pl.from_arrow(papq.read_table(_CACHE / "transit" / "stations.parquet"))
    hubs_df = pl.from_arrow(papq.read_table(_CACHE / "transit" / "optimal_hubs.parquet"))
    hp_df = pl.from_arrow(papq.read_table(_CACHE / "transit" / "hub_pair_routes.parquet"))
    rst_df = pl.from_arrow(papq.read_table(_CACHE / "teg" / "rail_stop_times.parquet"))

    hub_sfids = set(hubs_df["station_feature_id"].to_list())
    hub_idx_by_sfid = dict(zip(hubs_df["station_feature_id"].to_list(),
                                hubs_df["station_idx"].cast(pl.Int32).to_list()))
    station_info = {
        r["station_feature_id"]: {
            "idx": int(r["station_idx"]),
            "is_hub": r["station_feature_id"] in hub_sfids,
            "name": r["station_name"],
            "lon": float(r["station_lon"]),
            "lat": float(r["station_lat"]),
        }
        for r in stations_df.iter_rows(named=True)
        if r["is_rail_served"] == "true"
    }
    # Build TRIP_BY_SFID once for the Python composer (instance-level)
    rst_pd = rst_df.to_pandas()
    rst_pd = rst_pd.sort_values(["trip_id", "stop_sequence"])
    trips_at = {}    # sfid → list of {trip_id, stops, runs_dow}
    for trip_id, grp in rst_pd.groupby("trip_id", sort=False):
        stops = [(r.station_feature_id, int(r.arr_s), int(r.dep_s),
                  1 if r.station_feature_id in hub_sfids else 0)
                 for r in grp.itertuples()]
        if len(stops) < 2:
            continue
        rd = int(grp.iloc[0]["runs_dow"])
        for sfid, _, _, _ in stops:
            trips_at.setdefault(sfid, []).append(
                {"trip_id": trip_id, "stops": stops, "runs_dow": rd}
            )
    # Build HUBPAIRS lookup
    HUBPAIRS = {}
    for r in hp_df.iter_rows(named=True):
        k = (int(r["origin_hub_idx"]), int(r["dest_hub_idx"]),
             int(r["window_idx"]), int(r["weekday_mask"]))
        HUBPAIRS[k] = {
            "travel_min": int(r["travel_min"]),
            "n_transfers": int(r["n_transfers"]) if r["n_transfers"] is not None else -1,
            "first_dep_s": int(r["first_dep_s"]),
            "arr_s": int(r["arr_s"]),
        }

    # Build HUB_LABELS lookup — precomputed shortest path (travel_s,
    # n_transfers) FROM each hub TO each station per (window, weekday).
    # The label table is "from_hub" direction only; we use it both
    # ways: directly for last-mile (hub → d), approximately-inverted
    # for first-mile (o → hub, by time symmetry of the rail graph).
    hl_df = pl.from_arrow(papq.read_table(_CACHE / "transit" / "hub_labels.parquet"))
    sfid_by_idx = dict(zip(stations_df["station_idx"].cast(pl.Int64).to_list(),
                            stations_df["station_feature_id"].to_list()))
    # HUB_LABELS[sfid][wd_bit][win_idx] = list of (hub_idx, travel_s, n_transfers)
    HUB_LABELS = {}
    for r in hl_df.iter_rows(named=True):
        sfid = sfid_by_idx.get(int(r["station_idx"]))
        if sfid is None:
            continue
        wd = int(r["weekday_mask"])
        w = int(r["window_idx"])
        HUB_LABELS.setdefault(sfid, {}).setdefault(wd, {}).setdefault(w, []).append(
            (int(r["hub_idx"]), int(r["travel_s"]), int(r["n_transfers"]))
        )

    MIN_TRANSFER_S = 60

    def _find_first_hp_after(o_idx, d_idx, wd_bit, min_first_dep):
        """Earliest hub-pair (o→d, wd_bit) with first_dep_s ≥
        min_first_dep. Scans all 24 windows. Returns None if no
        entry meets the constraint."""
        best = None
        for w in range(24):
            hp = HUBPAIRS.get((o_idx, d_idx, w, wd_bit))
            if hp is None:
                continue
            if hp["first_dep_s"] < min_first_dep:
                continue
            if best is None or hp["first_dep_s"] < best["first_dep_s"]:
                best = hp
        return best

    def _find_best_hp_arriving_before(o_idx, d_idx, wd_bit, latest_arr, min_first_dep):
        """Hub-pair (o→d, wd_bit) with arr_s ≤ latest_arr AND
        first_dep_s ≥ min_first_dep, maximising first_dep_s (= least
        wait at the hub before catching the connection). Returns None
        if no entry meets both constraints."""
        best = None
        for w in range(24):
            hp = HUBPAIRS.get((o_idx, d_idx, w, wd_bit))
            if hp is None:
                continue
            if hp["arr_s"] > latest_arr:
                continue
            if hp["first_dep_s"] < min_first_dep:
                continue
            if best is None or hp["first_dep_s"] > best["first_dep_s"]:
                best = hp
        return best

    def _find_route(o_sfid, d_sfid, win_idx, wd_bit):
        """Python mirror of the JS findRoute composer. Matches MOTIS
        "earliest departure at or after the query window" semantics —
        first-mile and hub-pair lookups scan all windows from win_lo
        onward, so a 04:00 query can return a 06:30 train. Path
        coverage:
          A) direct single trip o → d (any vehicle stopping at both),
          B) single transfer at ANY shared station (hub or non-hub),
          C) origin/dest is hub: hub-pair + last/first-mile,
          D) both non-hub: fm → hub-pair → lm (two-transfer compose).
        Path B is the key fix for the "we slower" verdicts — non-hub
        transfer stations (regional junctions like St. Veit/Glan, Bruck
        an der Mur) often give materially shorter routes than the
        nearest-hub-only composer would find."""
        if o_sfid == d_sfid:
            return None
        o, d = station_info.get(o_sfid), station_info.get(d_sfid)
        if not o or not d:
            return None
        win_lo = win_idx * 3600
        best = None

        def update_best(total, ntr, first_dep, arr, legs, kind):
            nonlocal best
            if best is None or total < best["_tot"] or (
                total == best["_tot"] and ntr < best["n_transfers"]
            ):
                best = {"travel_min": round(total / 60), "n_transfers": ntr,
                        "first_dep_s": first_dep, "arr_s": arr,
                        "legs": legs, "_tot": total, "kind": kind}

        # Hub-hub direct (scan ALL windows for the earliest at-or-after
        # entry; the original window_idx-keyed lookup missed cases
        # where no train departs in the requested 1h slot).
        if o["is_hub"] and d["is_hub"]:
            hp = _find_first_hp_after(o["idx"], d["idx"], wd_bit, win_lo)
            if hp:
                update_best(hp["arr_s"] - hp["first_dep_s"], hp["n_transfers"],
                            hp["first_dep_s"], hp["arr_s"],
                            [{"kind": "hub_pair", "from": o_sfid, "to": d_sfid,
                              "dep_s": hp["first_dep_s"], "arr_s": hp["arr_s"],
                              "n_transfers": hp["n_transfers"]}],
                            "hub_hub")
                return best

        # Build first-mile-by-alight-station: for every trip calling
        # at o_sfid (boarding at-or-after win_lo), index each onward
        # stop as a candidate alight point. Hubs and non-hubs both
        # get indexed — non-hub transfers (Path B) are how regional-
        # rail composition picks up the short routes the hub-pair
        # graph alone misses.
        fm_by_alight = {}
        for t in trips_at.get(o_sfid, []):
            if (t["runs_dow"] & wd_bit) == 0:
                continue
            stops = t["stops"]
            bi = next((i for i, s in enumerate(stops) if s[0] == o_sfid), -1)
            if bi < 0:
                continue
            board_dep = stops[bi][2]
            if board_dep < win_lo:
                continue
            for j in range(bi + 1, len(stops)):
                sfid_X = stops[j][0]
                arr_X = stops[j][1]
                if arr_X <= board_dep:
                    continue
                if sfid_X == o_sfid:
                    continue
                fm_by_alight.setdefault(sfid_X, []).append({
                    "trip_id": t["trip_id"],
                    "board_sfid": o_sfid, "alight_sfid": sfid_X,
                    "dep_s": board_dep, "arr_s": arr_X,
                    "is_hub": stops[j][3] == 1,
                    "hub_idx": station_info.get(sfid_X, {}).get("idx"),
                })

        # Build last-mile-by-board-station: every trip calling at
        # d_sfid; index each prior stop as a candidate board point.
        lm_by_board = {}
        for t in trips_at.get(d_sfid, []):
            if (t["runs_dow"] & wd_bit) == 0:
                continue
            stops = t["stops"]
            ai = next((i for i, s in enumerate(stops) if s[0] == d_sfid), -1)
            if ai < 0:
                continue
            alight_arr = stops[ai][1]
            for i in range(0, ai):
                sfid_X = stops[i][0]
                dep_X = stops[i][2]
                if dep_X >= alight_arr:
                    continue
                if sfid_X == d_sfid:
                    continue
                lm_by_board.setdefault(sfid_X, []).append({
                    "trip_id": t["trip_id"],
                    "board_sfid": sfid_X, "alight_sfid": d_sfid,
                    "dep_s": dep_X, "arr_s": alight_arr,
                    "is_hub": stops[i][3] == 1,
                    "hub_idx": station_info.get(sfid_X, {}).get("idx"),
                })

        # --- Path A: direct single trip o → d ---
        for fm in fm_by_alight.get(d_sfid, []):
            total = fm["arr_s"] - fm["dep_s"]
            update_best(total, 0, fm["dep_s"], fm["arr_s"],
                        [{"trip_id": fm["trip_id"],
                          "board_sfid": o_sfid, "alight_sfid": d_sfid,
                          "dep_s": fm["dep_s"], "arr_s": fm["arr_s"]}],
                        "direct")

        # --- Path B: single transfer at ANY shared station ---
        shared = (set(fm_by_alight.keys()) & set(lm_by_board.keys())) - {o_sfid, d_sfid}
        for X in shared:
            for fm in fm_by_alight[X]:
                for lm in lm_by_board[X]:
                    if lm["dep_s"] < fm["arr_s"] + MIN_TRANSFER_S:
                        continue
                    total = lm["arr_s"] - fm["dep_s"]
                    update_best(total, 1, fm["dep_s"], lm["arr_s"],
                                [{"trip_id": fm["trip_id"],
                                  "board_sfid": fm["board_sfid"],
                                  "alight_sfid": fm["alight_sfid"],
                                  "dep_s": fm["dep_s"], "arr_s": fm["arr_s"]},
                                 {"trip_id": lm["trip_id"],
                                  "board_sfid": lm["board_sfid"],
                                  "alight_sfid": lm["alight_sfid"],
                                  "dep_s": lm["dep_s"], "arr_s": lm["arr_s"]}],
                                "transfer")

        # Convenience: hub-only lists for the hub-pair compose paths.
        first_mile = [fm for fms in fm_by_alight.values() for fm in fms if fm["is_hub"]]
        last_mile = [lm for lms in lm_by_board.values() for lm in lms if lm["is_hub"]]

        # --- Path C1: Origin is hub → hub-pair → last-mile ---
        if o["is_hub"]:
            for lm in last_mile:
                if lm["hub_idx"] == o["idx"]:
                    continue
                hp = _find_best_hp_arriving_before(
                    o["idx"], lm["hub_idx"], wd_bit,
                    latest_arr=lm["dep_s"] - MIN_TRANSFER_S,
                    min_first_dep=win_lo,
                )
                if not hp:
                    continue
                ntr = max(0, hp["n_transfers"]) + 1
                total = lm["arr_s"] - hp["first_dep_s"]
                update_best(total, ntr, hp["first_dep_s"], lm["arr_s"],
                            [{"kind": "hub_pair", "from": o_sfid, "to": lm["board_sfid"],
                              "dep_s": hp["first_dep_s"], "arr_s": hp["arr_s"],
                              "n_transfers": max(0, hp["n_transfers"])},
                             {"trip_id": lm["trip_id"], "board_sfid": lm["board_sfid"],
                              "alight_sfid": lm["alight_sfid"],
                              "dep_s": lm["dep_s"], "arr_s": lm["arr_s"]}],
                            "origin_hub")

        # --- Path C2: First-mile → hub-pair → Dest is hub ---
        if d["is_hub"]:
            for fm in first_mile:
                if fm["hub_idx"] == d["idx"]:
                    total = fm["arr_s"] - fm["dep_s"]
                    update_best(total, 0, fm["dep_s"], fm["arr_s"],
                                [{"trip_id": fm["trip_id"],
                                  "board_sfid": fm["board_sfid"],
                                  "alight_sfid": fm["alight_sfid"],
                                  "dep_s": fm["dep_s"], "arr_s": fm["arr_s"]}],
                                "direct_to_hub")
                    continue
                hp = _find_first_hp_after(
                    fm["hub_idx"], d["idx"], wd_bit,
                    min_first_dep=fm["arr_s"] + MIN_TRANSFER_S,
                )
                if not hp:
                    continue
                ntr = 1 + max(0, hp["n_transfers"])
                total = hp["arr_s"] - fm["dep_s"]
                update_best(total, ntr, fm["dep_s"], hp["arr_s"],
                            [{"trip_id": fm["trip_id"],
                              "board_sfid": fm["board_sfid"],
                              "alight_sfid": fm["alight_sfid"],
                              "dep_s": fm["dep_s"], "arr_s": fm["arr_s"]},
                             {"kind": "hub_pair", "from": fm["alight_sfid"], "to": d_sfid,
                              "dep_s": hp["first_dep_s"], "arr_s": hp["arr_s"],
                              "n_transfers": max(0, hp["n_transfers"])}],
                            "dest_hub")

        # --- Path D: Both non-hub, first-mile → hub-pair → last-mile ---
        # (Path B already handles the "same hub" / "same non-hub" case
        # since the hub appears as a shared station; here we cover the
        # different-hub cross-product that requires the hub-pair table.)
        if not o["is_hub"] and not d["is_hub"]:
            for fm in first_mile:
                for lm in last_mile:
                    if fm["hub_idx"] == lm["hub_idx"]:
                        continue  # covered by Path B
                    hp = _find_first_hp_after(
                        fm["hub_idx"], lm["hub_idx"], wd_bit,
                        min_first_dep=fm["arr_s"] + MIN_TRANSFER_S,
                    )
                    if not hp:
                        continue
                    if lm["dep_s"] < hp["arr_s"] + MIN_TRANSFER_S:
                        continue
                    ntr = 2 + max(0, hp["n_transfers"])
                    total = lm["arr_s"] - fm["dep_s"]
                    update_best(total, ntr, fm["dep_s"], lm["arr_s"],
                                [{"trip_id": fm["trip_id"], "board_sfid": fm["board_sfid"],
                                  "alight_sfid": fm["alight_sfid"], "dep_s": fm["dep_s"], "arr_s": fm["arr_s"]},
                                 {"kind": "hub_pair", "from": fm["alight_sfid"], "to": lm["board_sfid"],
                                  "dep_s": hp["first_dep_s"], "arr_s": hp["arr_s"],
                                  "n_transfers": max(0, hp["n_transfers"])},
                                 {"trip_id": lm["trip_id"], "board_sfid": lm["board_sfid"],
                                  "alight_sfid": lm["alight_sfid"], "dep_s": lm["dep_s"], "arr_s": lm["arr_s"]}],
                                "compose")

        # --- Path G: 2-hop transfer at non-hub intermediates ---
        # For each X reachable from o via fm, find trips at X going
        # to Y where Y is reachable to d via lm. This covers the
        # "rural junction" pattern (e.g., St. Veit/Glan, Bruck/Mur,
        # Selzthal) that's NOT in the K=24 hub set but is the
        # natural transfer point. For each trip2 (X → Y), pick the
        # LATEST fm with fm.arr_s + 60 ≤ trip2.x_dep (= no wait at
        # X = max fm.dep_s = min total travel from o to d).
        if not o["is_hub"] and not d["is_hub"]:
            lm_keys = set(lm_by_board.keys())
            G_IT_CAP = 50000
            g_iters = 0
            for X, fms_at_X in fm_by_alight.items():
                if X == d_sfid or X == o_sfid:
                    continue
                if g_iters >= G_IT_CAP:
                    break
                # Sort fms by arr_s ASCENDING so we can pick the
                # LATEST satisfying fm.arr_s + 60 ≤ x_dep via
                # bisect-or-scan-backwards. We use scan-and-keep-max
                # here (small lists; <50 typical).
                if not fms_at_X:
                    continue
                fms_sorted = sorted(fms_at_X, key=lambda x: x["arr_s"])
                for t in trips_at.get(X, []):
                    if g_iters >= G_IT_CAP:
                        break
                    if (t["runs_dow"] & wd_bit) == 0:
                        continue
                    stops = t["stops"]
                    xi = next((i for i, s in enumerate(stops) if s[0] == X), -1)
                    if xi < 0:
                        continue
                    x_dep = stops[xi][2]
                    # Find latest valid fm (max fm.dep_s subject to
                    # fm.arr_s + 60 ≤ x_dep). Iterate sorted asc and
                    # keep the last one that still satisfies.
                    latest_fm = None
                    for fm_at_X in fms_sorted:
                        if fm_at_X["arr_s"] + MIN_TRANSFER_S > x_dep:
                            break
                        # Keep the latest VALID fm (sorted asc, so
                        # successive valid ones overwrite latest_fm).
                        if latest_fm is None or fm_at_X["dep_s"] > latest_fm["dep_s"]:
                            latest_fm = fm_at_X
                    if latest_fm is None:
                        continue
                    # Onward stops from X on this trip
                    for j in range(xi + 1, len(stops)):
                        Y = stops[j][0]
                        if Y not in lm_keys:
                            continue
                        if Y == o_sfid or Y == d_sfid or Y == X:
                            continue
                        y_arr = stops[j][1]
                        if y_arr <= x_dep:
                            continue
                        for lm_at_Y in lm_by_board[Y]:
                            g_iters += 1
                            if lm_at_Y["dep_s"] < y_arr + MIN_TRANSFER_S:
                                continue
                            total = lm_at_Y["arr_s"] - latest_fm["dep_s"]
                            update_best(total, 2, latest_fm["dep_s"], lm_at_Y["arr_s"],
                                        [{"trip_id": latest_fm["trip_id"],
                                          "board_sfid": latest_fm["board_sfid"],
                                          "alight_sfid": latest_fm["alight_sfid"],
                                          "dep_s": latest_fm["dep_s"], "arr_s": latest_fm["arr_s"]},
                                         {"trip_id": t["trip_id"],
                                          "board_sfid": X, "alight_sfid": Y,
                                          "dep_s": x_dep, "arr_s": y_arr},
                                         {"trip_id": lm_at_Y["trip_id"],
                                          "board_sfid": lm_at_Y["board_sfid"],
                                          "alight_sfid": lm_at_Y["alight_sfid"],
                                          "dep_s": lm_at_Y["dep_s"], "arr_s": lm_at_Y["arr_s"]}],
                                        "transfer_2hop")

        # --- Path F: label-based fallback (covers pairs where
        # direct trips from origin don't reach any hub AND pairs
        # whose optimal route requires a transfer at a hub the
        # composer doesn't see directly). Uses the precomputed
        # GPU-SSSP hub_labels.parquet — for the window of interest,
        # it gives travel-time from each hub to the dest station
        # (and, by approximate symmetry of the rail TEG, from the
        # origin station to each hub). The total time is the sum of
        # (o → hub_a) + hub_pair(hub_a → hub_b) + (hub_b → d), all
        # for the same depart-window.
        if best is None or best["_tot"] > 6 * 3600:
            o_labels_by_wd = HUB_LABELS.get(o_sfid, {})
            d_labels_by_wd = HUB_LABELS.get(d_sfid, {})
            o_labels_for_wd = o_labels_by_wd.get(wd_bit, {})
            d_labels_for_wd = d_labels_by_wd.get(wd_bit, {})
            # Scan the request window first, then the next 12 hours
            # — covers MOTIS-aligned "earliest at or after" semantics
            # while keeping the scan bounded.
            for w_off in range(13):
                w = (win_idx + w_off) % 24
                ol = o_labels_for_wd.get(w)
                dl = d_labels_for_wd.get(w)
                if not ol or not dl:
                    continue
                # Build hub_a → (best_travel_s, n_tr) and hub_b → ...
                o_best = {}
                for h, t, n in ol:
                    if h not in o_best or t < o_best[h][0]:
                        o_best[h] = (t, n)
                d_best = {}
                for h, t, n in dl:
                    if h not in d_best or t < d_best[h][0]:
                        d_best[h] = (t, n)
                # Same hub on both sides → direct o → hub → d
                for h, (t_o, n_o) in o_best.items():
                    if h in d_best:
                        t_d, n_d = d_best[h]
                        total = t_o + MIN_TRANSFER_S + t_d
                        ntr = n_o + n_d + 1
                        first_dep = win_lo + w_off * 3600
                        update_best(total, ntr, first_dep, first_dep + total,
                                    [{"kind": "label_via_hub",
                                      "from": o_sfid, "via_hub_idx": h, "to": d_sfid,
                                      "travel_s": total, "n_transfers": ntr}],
                                    "label_same_hub")
                # Different hubs → o → hub_a → HP → hub_b → d
                for h_a, (t_o, n_o) in o_best.items():
                    for h_b, (t_d, n_d) in d_best.items():
                        if h_a == h_b:
                            continue
                        # Find earliest hub-pair h_a → h_b after we
                        # arrive at h_a (= win_lo + w_off + t_o + 60).
                        ha_arr_s = win_lo + w_off * 3600 + t_o
                        hp = _find_first_hp_after(
                            h_a, h_b, wd_bit,
                            min_first_dep=ha_arr_s + MIN_TRANSFER_S,
                        )
                        if not hp:
                            continue
                        hp_travel = hp["arr_s"] - hp["first_dep_s"]
                        total = (hp["first_dep_s"] - (win_lo + w_off * 3600)) \
                            + hp_travel + MIN_TRANSFER_S + t_d
                        ntr = n_o + max(0, hp["n_transfers"]) + n_d + 2
                        first_dep = win_lo + w_off * 3600
                        update_best(total, ntr, first_dep, first_dep + total,
                                    [{"kind": "label_via_2hubs",
                                      "from": o_sfid, "via_hub_a": h_a,
                                      "via_hub_b": h_b, "to": d_sfid,
                                      "travel_s": total, "n_transfers": ntr}],
                                    "label_two_hubs")
                # If we found a route from this offset, stop scanning
                if best is not None and best["_tot"] <= 4 * 3600:
                    break

        return best

    # ---- Sampling ----
    today = _datetime.now(_timezone.utc).date()
    # Per weekday_bit, find the next calendar date that falls on
    # that weekday. Python weekday(): Mon=0..Sun=6; our bit mapping
    # is Mon=1<<0=1, ..., Sun=1<<6=64.
    def _next_date_for_weekday(bit):
        # bit_position = 0 for Mon, ..., 6 for Sun
        wd_pos = bit.bit_length() - 1
        delta = (wd_pos - today.weekday()) % 7
        if delta == 0:
            delta = 7  # always future
        return today + _timedelta(days=delta)
    seed_int = int(today.strftime("%Y%m%d"))
    rng = random.Random(seed_int)

    rail_sfids = [s for s, i in station_info.items() if i["is_hub"] or True]
    hub_list = list(hub_sfids & set(rail_sfids))
    nonhub_list = sorted(set(rail_sfids) - set(hub_list))
    foreign_list = [s for s in nonhub_list if s.startswith("gtfs/N:")
                    or s.startswith("hu:") or s.startswith("de:")
                    or s.startswith("cz:") or s.startswith("si:")
                    or s.startswith("sk:")]

    # Sample R10_FRESH_PAIRS (21) random rail-served (origin, dest)
    # pairs ONCE — same pairs across every weekday and every window.
    # Weekdays cover the typical weekday / weekend timetable variance:
    #   Tue=bit 2  → mid-week weekday schedule
    #   Sat=bit 32 → Saturday schedule
    #   Sun=bit 64 → Sunday schedule
    # Cartesian product: 21 pairs × 3 weekdays × 24 windows = 1512
    # fresh test cells. Smart window-skip (below) deduplicates MOTIS
    # lookups when a single response covers multiple consecutive
    # windows.
    VAL_WEEKDAYS = [(2, "Tue"), (32, "Sat"), (64, "Sun")]
    rng_local = random.Random(seed_int)
    pool = sorted(rail_sfids)
    rng_local.shuffle(pool)

    def _classify_pair(a, b):
        a_is_hub = a in hub_sfids
        b_is_hub = b in hub_sfids
        a_foreign = a.startswith("gtfs/N:") or any(
            a.startswith(p) for p in ("hu:", "de:", "cz:", "si:", "sk:", "it:", "ch:")
        )
        b_foreign = b.startswith("gtfs/N:") or any(
            b.startswith(p) for p in ("hu:", "de:", "cz:", "si:", "sk:", "it:", "ch:")
        )
        if a_foreign or b_foreign:
            return "cb"
        if a_is_hub and b_is_hub:
            return "hh"
        if a_is_hub or b_is_hub:
            return "hn"
        return "nn"

    fresh_pairs_random = []  # list of (origin, dest, stratum)
    seen_pairs = set()
    attempts = 0
    while len(fresh_pairs_random) < R10_FRESH_PAIRS and attempts < R10_FRESH_PAIRS * 100:
        attempts += 1
        a = rng_local.choice(pool)
        b = rng_local.choice(pool)
        if a == b or (a, b) in seen_pairs:
            continue
        seen_pairs.add((a, b))
        fresh_pairs_random.append((a, b, _classify_pair(a, b)))

    fresh_tests = [
        (a, b, w, wb, wl, s)
        for (a, b, s) in fresh_pairs_random
        for wb, wl in VAL_WEEKDAYS
        for w in range(24)
    ]
    # Reporting-compatibility view
    fresh_pairs = list(fresh_pairs_random)

    # Corpus retest pairs
    corpus = {"version": 1, "pairs": []}
    if _CORPUS.exists():
        try:
            corpus = _j.loads(_CORPUS.read_text())
        except Exception:
            pass
    # Corpus retest set: all previous non-PASS entries (HARD-FAIL and
    # SOFT-FLAG, since both are regression-relevant) tested across
    # ALL 24 windows × all 3 weekdays.
    corpus_pairs = [
        (p["origin_sfid"], p["dest_sfid"], "corpus")
        for p in corpus.get("pairs", [])
        if p["origin_sfid"] in station_info and p["dest_sfid"] in station_info
    ]

    # Build the full test list: each entry is (origin, dest,
    # window_idx, weekday_bit, weekday_label, stratum).
    # - fresh    = R10_FRESH_PAIRS pairs × 3 weekdays × 24 windows = 1512
    # - corpus   = every corpus pair × 3 weekdays × 24 windows
    all_tests = list(fresh_tests)
    fresh_test_set = {(a, b, w, wb) for a, b, w, wb, _, _ in fresh_tests}
    for o, d, _ in corpus_pairs:
        for wb, wl in VAL_WEEKDAYS:
            for w in range(24):
                if (o, d, w, wb) not in fresh_test_set:
                    all_tests.append((o, d, w, wb, wl, "corpus"))

    # ---- MOTIS ----
    # Use a mutable dict for counters so nested functions can mutate
    # without `nonlocal` (marimo's static analysis flags nonlocal as
    # invalid even when Python's runtime accepts it).
    _counters = {"network_calls": 0, "cache_hits": 0, "cache_misses": 0,
                 "motis_errors": 0, "smart_skips": 0}

    def _cache_key(o_sfid, d_sfid, win_idx, when_iso):
        o_info = station_info[o_sfid]; d_info = station_info[d_sfid]
        blob = _j.dumps({
            "o": [o_info["lat"], o_info["lon"]],
            "d": [d_info["lat"], d_info["lon"]],
            "t": when_iso, "w": win_idx, "ep": _MOTIS_BASE,
            "max_tr": VAL_MAX_TRANSFERS, "max_match": 200,
            "modes": "TRANSIT", "v": R10_CACHE_SCHEMA_VERSION,
        }, sort_keys=True)
        return hashlib.sha1(blob.encode()).hexdigest()

    def _motis_plan(o_sfid, d_sfid, win_idx, depart_date):
        """Cache-only lookup (R10_CACHE_ONLY=True until further
        notice). Returns the cached JSON if present, otherwise None
        (treated as cache-miss / MOTIS-skipped — not an error).
        Network calls are issued only when R10_CACHE_ONLY=False AND
        the cache misses; in serial-single-request mode (one MOTIS
        call at a time) we drop the polite stagger since the loop
        itself never overlaps requests."""
        win_lo, win_hi = win_idx * 3600, (win_idx + 1) * 3600
        local_iso = f"{depart_date.isoformat()}T{win_idx:02d}:00:00+02:00"
        cache_k = _cache_key(o_sfid, d_sfid, win_idx, local_iso)
        cache_file = _CACHE_DIR / f"{cache_k}.json"
        if cache_file.exists():
            _counters["cache_hits"] += 1
            try:
                return _j.loads(cache_file.read_text())
            except Exception:
                pass
        if R10_CACHE_ONLY:
            _counters["cache_misses"] += 1
            return None
        o_info = station_info[o_sfid]; d_info = station_info[d_sfid]
        when_utc = _datetime.fromisoformat(local_iso).astimezone(_timezone.utc).isoformat()
        params = [
            ("fromPlace", f"{o_info['lat']},{o_info['lon']}"),
            ("toPlace", f"{d_info['lat']},{d_info['lon']}"),
            ("time", when_utc), ("arriveBy", "false"),
            ("numItineraries", "3"),
            ("maxTransfers", str(VAL_MAX_TRANSFERS)),
            ("searchWindow", str(win_hi - win_lo)),
            ("pedestrianProfile", "FOOT"),
            ("maxMatchingDistance", "200"),
            ("transitModes", "TRANSIT"),
        ]
        url = f"{_MOTIS_BASE}/plan?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=25) as resp:
                data = _j.loads(resp.read().decode())
            _counters["network_calls"] += 1
            cache_file.write_text(_j.dumps(data))
            return data
        except Exception as e:
            _counters["motis_errors"] += 1
            err_resp = {"_error": str(e)}
            cache_file.write_text(_j.dumps(err_resp))
            return err_resp

    def _motis_first_departure_hour_local(data, depart_date):
        """Return the local-time hour of the first transit leg's
        departure, or None. Used by the smart window-skip: if window
        W's MOTIS response actually departs at hour T (where T ≥ W),
        all windows in (W, T] would yield the same earliest-train
        answer, so reuse this response."""
        if not data or "_error" in data:
            return None
        itins = data.get("itineraries", []) or []
        if not itins:
            return None
        for leg in itins[0].get("legs", []):
            if leg.get("mode") == "WALK":
                continue
            dep = (leg.get("startTime") or leg.get("from", {}).get("departure"))
            if not dep:
                continue
            try:
                dt_utc = _datetime.fromisoformat(dep.replace("Z", "+00:00"))
                dt_local = dt_utc.astimezone(_timezone(_timedelta(hours=2)))
                # If departure crosses midnight forward, cap at 23
                # (smart-skip is a same-day optimisation; cross-day
                # would require re-targeting the next day).
                if dt_local.date() != depart_date:
                    return 23
                return dt_local.hour
            except Exception:
                return None
        return None

    def _summarise_motis(data):
        if not data or "_error" in data:
            return None
        itins = data.get("itineraries", []) or []
        best = None
        for it in itins:
            legs = it.get("legs", [])
            trip_ids = []
            modes = set()
            for leg in legs:
                if leg.get("mode") == "WALK":
                    continue
                trip_ids.append((leg.get("trip", {}) or {}).get("tripId", ""))
                modes.add(leg.get("mode", ""))
            dur = it.get("duration", 10 ** 9)
            ntr = it.get("transfers", len(legs) - 1)
            if best is None or dur < best["duration"]:
                best = {"duration": dur, "n_transfers": ntr,
                         "trip_ids": trip_ids, "modes": modes}
        if best is None:
            return None
        return {"travel_min": round(best["duration"] / 60),
                "n_transfers": best["n_transfers"],
                "trip_ids": best["trip_ids"], "modes": best["modes"]}

    def _motis_debug_offsets(data):
        if not data:
            return (0, 0)
        debug = data.get("debugOutput", {})
        return (debug.get("n_start_offsets", 0), debug.get("n_dest_offsets", 0))

    # ---- Verdict triage ----
    def _is_domestic(sfid):
        return (sfid.startswith("way/") or sfid.startswith("node/")
                or sfid.startswith("relation/"))

    def _classify(pair_origin, pair_dest, ours, motis, motis_data, stratum):
        reasons = []
        domestic = _is_domestic(pair_origin) and _is_domestic(pair_dest)
        n_so, n_do = _motis_debug_offsets(motis_data)
        coverage_ok = n_so >= VAL_MOTIS_OFFSETS_MIN and n_do >= VAL_MOTIS_OFFSETS_MIN

        if ours is None and motis is None:
            return "both-fail", []
        if ours is not None and motis is None:
            if domestic and coverage_ok:
                return "hard-fail", ["phantom (domestic): we routed, motis did not"]
            elif domestic:
                return "soft-flag", [f"phantom (domestic, MOTIS offsets {n_so}/{n_do} <20)"]
            else:
                return "soft-flag", ["phantom (cross-border / foreign)"]
        if ours is None and motis is not None:
            modes = motis.get("modes", set())
            rail_only = all(m in ("RAIL", "REGIONAL_RAIL", "SUBURBAN", "HIGHSPEED_RAIL",
                                   "LONG_DISTANCE", "NIGHT_RAIL") for m in modes) if modes else True
            if domestic and rail_only:
                return "hard-fail", ["motis-only (domestic, MOTIS rail-only)"]
            elif domestic:
                return "soft-flag", [f"motis-only via non-rail ({','.join(sorted(modes))})"]
            else:
                return "soft-flag", ["motis-only (cross-border)"]
        # Both succeed
        dur_d_min = motis["travel_min"] - ours["travel_min"]
        modes = motis.get("modes", set())
        rail_only = all(m in ("RAIL", "REGIONAL_RAIL", "SUBURBAN", "HIGHSPEED_RAIL",
                                "LONG_DISTANCE", "NIGHT_RAIL") for m in modes) if modes else True
        if domestic and rail_only and -dur_d_min >= HARDFAIL_MIN_AHEAD_MIN:
            # MOTIS faster by >=60 min
            return "hard-fail", [f"we slower by {-dur_d_min} min (domestic, MOTIS rail-only)"]
        pct = abs(ours["travel_min"] - motis["travel_min"]) / max(1, motis["travel_min"]) * 100
        tr_d = abs(ours["n_transfers"] - motis["n_transfers"])
        if domestic and rail_only and pct <= SOFTFLAG_PCT and tr_d <= SOFTFLAG_TR_DELTA:
            return "pass", []
        if not rail_only:
            reasons.append(f"motis via {','.join(sorted(modes))}")
        if pct > SOFTFLAG_PCT:
            reasons.append(f"travel-time delta {pct:.0f}%")
        if tr_d > SOFTFLAG_TR_DELTA:
            reasons.append(f"transfer delta {tr_d}")
        return ("soft-flag", reasons) if reasons else ("pass", [])

    # ---- Iterate fresh + corpus tests ----
    # Smart window-skip: tests are grouped by (origin, dest, weekday)
    # and iterated in window order. When a MOTIS lookup for window W
    # returns an itinerary whose first transit leg departs at local
    # hour T (T ≥ W), reuse that same response for every intermediate
    # window in (W, T]: requesting from those earlier hours would
    # have yielded the same earliest-train answer. The smart-skip
    # serves two purposes — (a) when R10_CACHE_ONLY=True, it covers
    # cache-miss windows whose answer is already known from an
    # earlier successful window; (b) when R10_CACHE_ONLY=False, it
    # avoids issuing redundant network calls.
    rows = []
    date_for_wd_bit = {wb: _next_date_for_weekday(wb) for wb, _ in VAL_WEEKDAYS}

    # Group tests by (origin, dest, weekday_bit) so we can window-skip
    # within each group. Preserve insertion order so the row order in
    # the evidence file mirrors the test definition order.
    from collections import OrderedDict as _OD
    grouped = _OD()
    for (o_sfid, d_sfid, win, wd_bit, wd_label, stratum) in all_tests:
        key = (o_sfid, d_sfid, wd_bit, wd_label, stratum)
        grouped.setdefault(key, []).append(win)

    for (o_sfid, d_sfid, wd_bit, wd_label, stratum), wins in grouped.items():
        depart_date = date_for_wd_bit[wd_bit]
        wins_sorted = sorted(set(wins))
        # Per-group cache of the active MOTIS response and the
        # window-up-to-which it is still applicable.
        active_motis_data = None
        active_motis_covers_through = -1
        active_motis_origin_win = -1
        for win in wins_sorted:
            ours = _find_route(o_sfid, d_sfid, win, wd_bit)
            # Smart-skip: reuse the active MOTIS response if this
            # window is still covered by it.
            if active_motis_data is not None and win <= active_motis_covers_through:
                motis_data = active_motis_data
                _counters["smart_skips"] += 1
                skip_note = (f"smart-skip: reused MOTIS response from "
                             f"window {active_motis_origin_win:02d}")
            else:
                motis_data = _motis_plan(o_sfid, d_sfid, win, depart_date)
                skip_note = ""
                if motis_data is not None and "_error" not in motis_data:
                    dep_hour = _motis_first_departure_hour_local(motis_data, depart_date)
                    if dep_hour is not None and dep_hour >= win:
                        active_motis_data = motis_data
                        active_motis_covers_through = dep_hour
                        active_motis_origin_win = win
                    else:
                        active_motis_data = None
                        active_motis_covers_through = -1
                        active_motis_origin_win = -1
                else:
                    # Cache-miss / error / no-data → reset the active
                    # carry-over so later windows don't keep skipping.
                    active_motis_data = None
                    active_motis_covers_through = -1
                    active_motis_origin_win = -1
            motis = _summarise_motis(motis_data)
            verdict, reasons = _classify(o_sfid, d_sfid, ours, motis, motis_data, stratum)
            if motis_data is None:
                # Cache-miss in CACHE_ONLY mode: surface explicitly
                # so the row isn't silently labelled "both-fail".
                verdict = "cache-miss"
                reasons = ["no cached MOTIS response (R10_CACHE_ONLY=True)"]
            rows.append({
                "stratum": stratum,
                "weekday": wd_label,
                "window": win,
                "depart_date": depart_date.isoformat(),
                "origin_sfid": o_sfid, "origin_name": station_info[o_sfid]["name"],
                "dest_sfid": d_sfid, "dest_name": station_info[d_sfid]["name"],
                "ours_min": ours["travel_min"] if ours else None,
                "ours_tr": ours["n_transfers"] if ours else None,
                "motis_min": motis["travel_min"] if motis else None,
                "motis_tr": motis["n_transfers"] if motis else None,
                "verdict": verdict,
                "reasons": "; ".join([r for r in [skip_note] + list(reasons) if r]),
            })

    val_df = pl.DataFrame(rows, schema_overrides={
        "ours_min": pl.Int64, "ours_tr": pl.Int64,
        "motis_min": pl.Int64, "motis_tr": pl.Int64,
        "window": pl.Int64,
    }, infer_schema_length=None)

    # ---- Corpus update ----
    # cache-miss verdicts are not real verdicts → not corpus-candidate.
    new_corpus_pairs = []
    existing_corpus_keys = {(p["origin_sfid"], p["dest_sfid"]) for p in corpus.get("pairs", [])}
    nonpass_pairs = set()
    for r in rows:
        if r["verdict"] in ("hard-fail", "soft-flag"):
            nonpass_pairs.add((r["origin_sfid"], r["dest_sfid"], r["verdict"], r["reasons"]))
    for (o, d, v, rs) in nonpass_pairs:
        if (o, d) not in existing_corpus_keys:
            new_corpus_pairs.append({
                "origin_sfid": o, "dest_sfid": d,
                "first_seen_utc": _datetime.now(_timezone.utc).isoformat(),
                "first_seen_verdict": v, "first_seen_reasons": rs,
                "first_seen_seed_date": today.isoformat(),
            })
    corpus["pairs"] = corpus.get("pairs", []) + new_corpus_pairs
    corpus["last_updated_utc"] = _datetime.now(_timezone.utc).isoformat()
    _CORPUS.write_text(_j.dumps(corpus, indent=2))

    # ---- Evidence JSON ----
    ev_path = _Path(f"/workspace/.r10/transitous-validation-graph-"
                    f"{_datetime.now(_timezone.utc).strftime('%Y%m%dT%H%M%S')}.json")
    ev_path.write_text(_j.dumps({
        "schema_version": 3,
        "ran_utc": _datetime.now(_timezone.utc).isoformat(),
        "cache_only": bool(R10_CACHE_ONLY),
        "depart_dates_by_weekday": {wl: date_for_wd_bit[wb].isoformat()
                                       for wb, wl in VAL_WEEKDAYS},
        "seed_date": today.isoformat(),
        "weekdays_used": [wl for _, wl in VAL_WEEKDAYS],
        "depart_dates": {wl: date_for_wd_bit[wb].isoformat()
                          for wb, wl in VAL_WEEKDAYS},
        "windows_used": sorted({w for _, _, w, _, _, _ in all_tests}),
        "motis_endpoint": _MOTIS_BASE,
        "n_fresh_pairs": len(fresh_pairs_random),
        "n_fresh_tests": len(fresh_tests),
        "n_corpus_pairs": len(corpus_pairs),
        "n_total_tests": len(all_tests),
        "network_calls": _counters["network_calls"],
        "cache_hits": _counters["cache_hits"],
        "cache_misses": _counters["cache_misses"],
        "smart_skips": _counters["smart_skips"],
        "motis_errors": _counters["motis_errors"],
        "rows": rows,
    }, indent=2))

    counts = val_df.group_by("verdict").agg(pl.len().alias("n")).sort("verdict")
    stratum_counts = val_df.group_by("stratum").agg(pl.len().alias("n")).sort("stratum")
    weekday_counts = val_df.group_by("weekday").agg(pl.len().alias("n")).sort("weekday")
    summary_md = mo.md(f"""
    ### R10 Transitous validation gate

    - **Endpoint** `{_MOTIS_BASE}` · **Mode** `{"cache-only" if R10_CACHE_ONLY else "live"}` (serial single-request)
    - **Weekdays** {dict(zip([wl for _, wl in VAL_WEEKDAYS], [date_for_wd_bit[wb].isoformat() for wb, _ in VAL_WEEKDAYS]))} (Europe/Vienna)
    - **Seed** `{today.isoformat()}` (rotates daily for fresh tests)
    - **Tests** {len(fresh_tests)} fresh ({len(fresh_pairs_random)} pairs × 3 weekdays × 24 windows)
       + {len(corpus_pairs) * len(VAL_WEEKDAYS) * 24} corpus retest ({len(corpus_pairs)} pairs × 3 wd × 24 win)
       = **{len(all_tests)} total**
    - **MOTIS lookups** {_counters["cache_hits"]} cache-hit + {_counters["cache_misses"]} cache-miss + {_counters["smart_skips"]} smart-skip + {_counters["network_calls"]} network + {_counters["motis_errors"]} errors · UA `{_UA}`
    - **Stratum** {dict(zip(stratum_counts['stratum'].to_list(), stratum_counts['n'].to_list()))}
    - **Per-weekday** {dict(zip(weekday_counts['weekday'].to_list(), weekday_counts['n'].to_list()))}
    - **Evidence** `{ev_path}`
    - **Corpus update** added {len(new_corpus_pairs)} new non-PASS entries (total in corpus: {len(corpus.get("pairs", []))})

    **Acceptance**: HARD-FAIL count must be **0** for commit at
    `fully tested and validated`. SOFT-FLAGs must be enumerated in
    the commit body with one-line per-pair rationale (R2).
    """)
    mo.vstack([summary_md, counts, val_df])
    return


@app.cell
def _tail(dag_run_states, martin, mo):
    # Operator-facing status + static-web deployment notes.
    _ok = all(s == "success" for s in dag_run_states.values())
    _badge = "✅ DAG green" if _ok else "🔴 DAG failed"
    mo.md(f"""
    ## Status

    {_badge} — `notebook_austria_graph_pipeline` →
    `{list(dag_run_states.values())[0]}`

    ### Architecture summary

    - **Standalone pipeline** (zero dependency on `gtfs-austria.py` or
      `osm-austria.py`) — downloads its own Austria PBF + Transitous
      Austria GTFS feed.
    - **Instance-level RAPTOR TEG** with nTr ∈ [0..4] layering —
      transfer cap structural, no algorithmic guard needed.
    - **cuGraph PageRank** hub selection + BFS connectivity-guarantee
      pass — replaces the O(n²) D-matrix greedy from the CPU baseline.
    - **Batched cuGraph.sssp** per (weekday, window) for the hub-pair
      contraction-hierarchy table.
    - **Partial Hub-Labeling** (K_LOCAL=8 nearest hubs per station) —
      enables O(1) JS findRoute queries via cross-product composition.
    - **JS route-builder** (first-mile + hub-pair + last-mile composer)
      runs entirely client-side over PMTiles — no marimo kernel
      callbacks, no Python at runtime. Map is static-web deployable.

    ### Tiles served by martin ({martin})

    | Source | Schema |
    |---|---|
    | `austria-graph-routes` | theme='trip' (one row per instance) + theme='station' (with is_hub + nearest_hubs) |
    | `austria-graph-hubpairs` | theme='hubpair' (K×K × 24 × 7 contraction-hierarchy table) |
    | `austria-graph-isochrones` | theme='chrono' (polygon ring fills) + theme='chrono-origin' (hub markers) |
    | `austria-graph-hublabels` | theme='hublabel' (per-station nearest-hub labels) |

    ### Static-web deployment

    The route-builder HTML iframe is fully self-contained: copy the
    `<head>`/`<body>` to any static CDN, replace
    `{martin}/austria-graph-*` URLs with PMTiles archive HTTP URLs
    (served as static files; pmtiles.js reads via HTTP range), and
    the JS planner works without any backend. The data contract is
    the PMTiles archives — no Python needed at runtime.

    ### Scale

    Architecture chosen for **Europe-scale** (50k stations, 500k trips):
    cuGraph PageRank + partial Hub-Labeling + 1h windows scale
    linearly with station count. Austria deploy is the first
    verification target; multi-country full Europe is a follow-on
    cutover.
    """)
    return


if __name__ == "__main__":
    app.run()
