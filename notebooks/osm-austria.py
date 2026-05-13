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
        # Austria — OSM data + freestiler tile pipelines

        Sibling to `osm-monaco-viz.py`, scoped to Austria's OSM data
        plane. This notebook **self-authors the OSM Airflow DAG**
        (`notebook_austria_pipeline`) into `${{AIRFLOW_DAGS_DIR}}`,
        **triggers** it via the Airflow REST API at <{airflow_public}>,
        **polls until success**, then renders the 5 vector-tile maps
        martin serves from the freestiler PMTiles archives.

        **Two-notebook contract** — the GTFS pipeline + unified
        GTFS↔OSM analysis + 5-theme ecovoyage visualisation moved to
        the sibling notebook `gtfs-austria.py`. The two notebooks share
        the workspace bind-mount (DAGs in `/workspace/dags/`, tile
        outputs in `/workspace/tiles/`, the unified DuckDB in
        `/workspace/duckdb/austria.duckdb`) but never cross-reference in
        Airflow's TaskFlow API. The GTFS DAG's `materialize_duckdb` task
        waits for THIS notebook's `austria.parquet` output via task
        retries — filesystem-only coordination.

        Maps below:

        - **Austria vector-tile map** — `austria-duckdb-freestiler`
          source (PBF → quackosm GeoParquet → freestiler PMTiles),
          served by martin at <{martin}>.
        - **Per-theme maps** — `austria-railway`, `austria-cycle`,
          `austria-topo`, `austria-hiking` PMTiles, each styled from
          its MapLibre theme constant (`RAILWAY_STYLE` / `CYCLE_STYLE`
          / `TOPO_STYLE` / `HIKING_STYLE`).

        The 5-theme consolidated `austria-ecovoyage` map (4 OSM themes
        + GTFS transit overlay) is rendered by `gtfs-austria.py` —
        that's the unified-everything visualisation.

        ## Download policy — monthly-cached, idempotent

        The OSM DAG runs on `schedule="@monthly"` (Airflow's cron alias
        for `0 0 1 * *`) so the scheduler auto-fires on the 1st of each
        month at 00:00 UTC. Each derivation task additionally
        short-circuits when its output exists AND its mtime falls in
        the current calendar month — so ad-hoc / notebook-triggered
        re-runs within a month skip every step whose output is already
        fresh. Together: each artifact materializes at most once per
        month, exactly when stale.

        ## Data source

        | Source | URL |
        |---|---|
        | OSM PBF | `https://download.geofabrik.de/europe/austria-latest.osm.pbf` (~750 MB) |

        ## URL strategy — server-side vs browser-side

        Same two-space split as the Monaco + GTFS sibling notebooks:
        kernel-side calls (notebook → Airflow REST) use
        `AIRFLOW_API_INTERNAL_URL`; the MapLibre map cells embed
        `MARTIN_PUBLIC_URL` into their iframes so the browser can reach
        martin via the published host port. The diagnostic table above
        resolves both at runtime — the values rotate when
        `port: [auto]` rotates host ports on rebuild.
        """
    )
    return


@app.cell
def _(Path, os, textwrap):
    # Self-author the Austria OSM + freestiler pipeline DAG. Idempotent
    # — overwriting on every notebook run keeps the DAG body in sync
    # with this notebook (single source of truth: this cell IS the DAG
    # spec). The sibling notebook gtfs-austria.py self-authors the GTFS
    # pipeline DAG (notebook_austria_gtfs_pipeline); see CLAUDE.md
    # "Two-notebook contract" for the cross-notebook coordination model.
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

        Monthly-cache policy: every derivation task (download, parquet
        build, all freestiler tile builds) short-circuits when its
        output already exists and was produced in the current calendar
        month (UTC). See _needs_regen() below. Paired with
        schedule="@monthly" so the scheduler auto-fires on the 1st of
        each month — outputs from last month are then stale and get
        re-derived. Within a month, manual/notebook-triggered runs are
        free (every task is O(1) stat).
        """
        import os
        import subprocess
        from datetime import datetime, timezone
        from pathlib import Path

        from airflow.sdk import dag, task

        WORK = Path(os.path.expanduser("/workspace/tiles/work"))
        TILES = Path(os.path.expanduser("/workspace/tiles/pmtiles"))

        # Shared column projection used by every themed freestiler task
        # (cycle / topo / hiking). One source of truth for the base
        # columns every MapLibre style cell consumes (osm_id, geometry,
        # name/ref/highway/surface/bridge/tunnel/etc., plus the
        # geometry-type discriminator). Theme tasks concatenate their
        # theme-specific fields after this fragment. R3.
        _COMMON_SELECT_FIELDS = """
                      feature_id                                       AS osm_id,
                      geometry,
                      tags['name']                                     AS name,
                      tags['ref']                                      AS ref,
                      tags['highway']                                  AS highway,
                      tags['surface']                                  AS surface,
                      tags['tracktype']                                AS tracktype,
                      tags['access']                                   AS access,
                      tags['bridge']                                   AS bridge,
                      tags['tunnel']                                   AS tunnel,
                      TRY_CAST(tags['layer'] AS INTEGER)               AS layer,
                      tags['oneway']                                   AS oneway,
                      tags['lit']                                      AS lit,
                      tags['natural']                                  AS natural,
                      TRY_CAST(tags['ele'] AS DOUBLE)                  AS ele,
                      tags['tourism']                                  AS tourism,
                      ST_GeometryType(geometry)                        AS geometry_type,
                      CASE WHEN ST_GeometryType(geometry) IN ('POLYGON','MULTIPOLYGON')
                           THEN ST_Area(geometry) ELSE NULL END        AS way_area"""

        # Per-theme SELECT field fragments — theme-specific projections appended
        # after _COMMON_SELECT_FIELDS in each freestiler task's SELECT body. The
        # tag inventory for each theme is cited from upstream rendering style
        # configs (see the per-task comments). R3: one definition per theme,
        # used by both the theme's standalone task AND the consolidated
        # austria-ecovoyage UNION ALL BY NAME query.

        _CYCLE_FIELDS = """
                      tags['cycleway']                                 AS cycleway,
                      tags['cycleway:left']                            AS cycleway_left,
                      tags['cycleway:right']                           AS cycleway_right,
                      tags['cycleway:both']                            AS cycleway_both,
                      tags['bicycle']                                  AS bicycle,
                      tags['bicycle_road']                             AS bicycle_road,
                      tags['oneway:bicycle']                           AS oneway_bicycle,
                      tags['mtb:scale']                                AS mtb_scale,
                      tags['mtb:scale:uphill']                         AS mtb_scale_uphill,
                      tags['smoothness']                               AS smoothness,
                      tags['segregated']                               AS segregated,
                      tags['route']                                    AS route,
                      tags['network']                                  AS network,
                      tags['amenity']                                  AS amenity"""

        _CYCLE_WHERE = """tags['highway'] IS NOT NULL
                      OR tags['cycleway'] IS NOT NULL
                      OR tags['route'] = 'bicycle'
                      OR tags['amenity'] IN ('bicycle_parking','bicycle_rental','bicycle_repair_station')"""

        _TOPO_FIELDS = """
                      tags['place']                                    AS place,
                      tags['boundary']                                 AS boundary,
                      TRY_CAST(tags['admin_level'] AS INTEGER)         AS admin_level,
                      tags['landuse']                                  AS landuse,
                      tags['landcover']                                AS landcover,
                      tags['waterway']                                 AS waterway,
                      tags['intermittent']                             AS intermittent,
                      tags['aerialway']                                AS aerialway,
                      tags['power']                                    AS power,
                      tags['building']                                 AS building,
                      tags['amenity']                                  AS amenity,
                      tags['historic']                                 AS historic,
                      tags['man_made']                                 AS man_made,
                      tags['mountain_pass']                            AS mountain_pass,
                      tags['wikipedia']                                AS wikipedia,
                      tags['railway']                                  AS railway"""

        _TOPO_WHERE = """tags['highway'] IN ('motorway','trunk','primary','secondary','tertiary','unclassified',
                                               'residential','service','track','path','footway','bridleway','cycleway','steps')
                      OR tags['railway'] IS NOT NULL
                      OR tags['waterway'] IS NOT NULL
                      OR tags['natural'] IS NOT NULL
                      OR tags['landuse'] IN ('forest','meadow','farmland','farmyard','grass','orchard','vineyard',
                                               'cemetery','residential','industrial','commercial','quarry')
                      OR tags['boundary'] IS NOT NULL
                      OR tags['place'] IS NOT NULL
                      OR tags['amenity'] IS NOT NULL
                      OR tags['tourism'] IS NOT NULL
                      OR tags['aerialway'] IS NOT NULL
                      OR tags['building'] IS NOT NULL
                      OR tags['power'] IN ('line','minor_line','tower','pole','substation','generator')
                      OR tags['man_made'] IN ('tower','mast','lighthouse','windmill','chimney','communications_tower')"""

        # Railway-specific fields plus the z_order CASE expression that mirrors
        # OpenRailwayMap CartoCSS layer ordering (tunnels below, bridges above,
        # mainline rail above transit + abandoned/preserved tracks).
        _RAILWAY_FIELDS = """
                      tags['railway']                                  AS railway,
                      tags['public_transport']                         AS public_transport,
                      tags['usage']                                    AS usage,
                      tags['service']                                  AS service,
                      tags['construction']                             AS construction,
                      tags['cutting']                                  AS cutting,
                      tags['embankment']                               AS embankment,
                      tags['abandoned']                                AS abandoned,
                      tags['disused']                                  AS disused,
                      tags['razed']                                    AS razed,
                      tags['proposed']                                 AS proposed,
                      tags['man_made']                                 AS man_made,
                      tags['power']                                    AS power,
                      tags['area']                                     AS area,
                      tags['electrified']                              AS electrified,
                      TRY_CAST(tags['frequency'] AS DOUBLE)            AS frequency,
                      TRY_CAST(tags['voltage'] AS INTEGER)             AS voltage,
                      tags['deelectrified']                            AS deelectrified,
                      tags['construction:electrified']                 AS construction_electrified,
                      TRY_CAST(tags['construction:frequency'] AS DOUBLE)  AS construction_frequency,
                      TRY_CAST(tags['construction:voltage'] AS INTEGER)   AS construction_voltage,
                      tags['proposed:electrified']                     AS proposed_electrified,
                      TRY_CAST(tags['proposed:frequency'] AS DOUBLE)      AS proposed_frequency,
                      TRY_CAST(tags['proposed:voltage'] AS INTEGER)       AS proposed_voltage,
                      tags['abandoned:electrified']                    AS abandoned_electrified,
                      tags['maxspeed']                                 AS maxspeed,
                      tags['maxspeed:forward']                         AS maxspeed_forward,
                      tags['maxspeed:backward']                        AS maxspeed_backward,
                      tags['railway:preferred_direction']              AS preferred_direction,
                      tags['railway:position']                         AS railway_position,
                      tags['railway:position:detail']                  AS railway_position_detail,
                      tags['railway:local_operated']                   AS railway_local_operated,
                      tags['railway:signal:direction']                 AS signal_direction,
                      tags['railway:signal:speed_limit']               AS signal_speed_limit,
                      tags['railway:signal:speed_limit:form']          AS signal_speed_limit_form,
                      tags['railway:signal:speed_limit:speed']         AS signal_speed_limit_speed,
                      tags['railway:signal:speed_limit_distant']       AS signal_speed_limit_distant,
                      tags['railway:signal:speed_limit_distant:form']  AS signal_speed_limit_distant_form,
                      tags['railway:signal:speed_limit_distant:speed'] AS signal_speed_limit_distant_speed,
                      COALESCE(TRY_CAST(tags['layer'] AS INTEGER), 0) * 10
                        + CASE WHEN tags['tunnel'] IS NOT NULL THEN -10
                               WHEN tags['bridge'] IS NOT NULL THEN  10
                               ELSE 0 END
                        + CASE WHEN tags['railway'] = 'rail' THEN 5
                               WHEN tags['railway'] IN ('light_rail','subway','tram','narrow_gauge','monorail','funicular') THEN 3
                               WHEN tags['railway'] IN ('preserved','miniature') THEN 1
                               ELSE 0 END                              AS z_order"""

        _RAILWAY_WHERE = """tags['railway'] IS NOT NULL
                      OR tags['public_transport'] IN ('station','stop_position','platform','halt')
                      OR (tags['power'] = 'line' AND tags['line'] = 'busbar')
                      OR (tags['man_made'] IN ('mast','tower')
                          AND tags['tower:type'] = 'communication'
                          AND tags['railway'] IS NOT NULL)"""

        _HIKING_FIELDS = """
                      tags['sac_scale']                                AS sac_scale,
                      tags['trail_visibility']                         AS trail_visibility,
                      tags['mtb:scale']                                AS mtb_scale,
                      tags['foot']                                     AS foot,
                      tags['hiking']                                   AS hiking,
                      tags['informal']                                 AS informal,
                      tags['route']                                    AS route,
                      tags['network']                                  AS network,
                      tags['osmc:symbol']                              AS osmc_symbol,
                      tags['wheelchair']                               AS wheelchair,
                      tags['mountain_pass']                            AS mountain_pass,
                      tags['railway']                                  AS railway"""

        _HIKING_WHERE = """tags['highway'] IN ('path','footway','track','bridleway','steps','pedestrian')
                      OR tags['route'] = 'hiking'
                      OR tags['sac_scale'] IS NOT NULL
                      OR tags['natural'] IN ('peak','saddle','cliff','ridge','arete','volcano','spring','cave_entrance','glacier')
                      OR tags['tourism'] IN ('alpine_hut','wilderness_hut','viewpoint','camp_site','picnic_site','information')
                      OR tags['mountain_pass'] IS NOT NULL"""


        def _needs_regen(path: Path) -> bool:
            """Return True if `path` is missing OR was produced in a
            prior calendar month (UTC). Used by every data-derivation
            task to short-circuit re-runs within the same month.

            Combined with schedule='@monthly' on the DAG, this gives two
            independent guards:
              * Airflow auto-fires the DAG on the 1st of each month →
                every output's mtime is from last month → all tasks
                regenerate fresh data.
              * Ad-hoc / notebook-triggered runs within the same month
                skip every task whose output is already fresh — manual
                retriggers cost O(1) stat per task, NOT the full pipeline.
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
                if not _needs_regen(out):
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
                if not _needs_regen(out):
                    return str(out)
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
                if not _needs_regen(out):
                    return str(out)
                query = f"SELECT * FROM read_parquet('{parquet_path}')"
                if hasattr(freestiler, "freestile_query"):
                    freestiler.freestile_query(
                        query=query,
                        output=str(out),
                        layer_name="austria",
                        min_zoom=0,
                        max_zoom=12,
                        base_zoom=12,
                        drop_rate=2.0,
                        coalesce=True,
                    )
                elif hasattr(freestiler, "freestile"):
                    freestiler.freestile(
                        input=query,
                        output=str(out),
                        layer_name="austria",
                        min_zoom=0,
                        max_zoom=12,
                        base_zoom=12,
                        drop_rate=2.0,
                        coalesce=True,
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
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT{_COMMON_SELECT_FIELDS},{_RAILWAY_FIELDS}
                    FROM read_parquet('{parquet_path}')
                    WHERE {_RAILWAY_WHERE}
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
                    base_zoom=14,
                    drop_rate=2.0,
                    coalesce=True,
                )
                return str(out)

            @task
            def freestiler_cycle_convert(parquet_path: str) -> str:
                # Cycling-themed projection. Tag inventory derived from
                # cyclemap/openmaptiles-cycle's transportation layer +
                # cycleway.sql + the cycle style overlay. Output is a
                # narrow PMTiles archive martin auto-discovers as
                # `austria-cycle` — feeds a MapLibre style cell that
                # paints cycle networks blue, segregated cycleways green,
                # on-road dashed, and mtb:scale>=3 in red-orange.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-cycle.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT{_COMMON_SELECT_FIELDS},{_CYCLE_FIELDS}
                    FROM read_parquet('{parquet_path}')
                    WHERE {_CYCLE_WHERE}
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-cycle",
                    min_zoom=0,
                    max_zoom=14,
                    base_zoom=14,
                    drop_rate=2.0,
                    coalesce=True,
                )
                return str(out)

            @task
            def freestiler_topo_convert(parquet_path: str) -> str:
                # Topographic projection. Tag inventory derived from
                # OpenTopoMap's vector/tilemaker/process-otm.lua acceptance
                # sets + tilemaker-config-otm.json layer schema. OSM-derived
                # topo features only — DEM contours (SRTM/ASTER) are a
                # separate raster pipeline out of scope. max_zoom=12 keeps
                # the archive at single-GB scale; 10 M-feature filter is
                # ~3x the railway scope.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-topo.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT{_COMMON_SELECT_FIELDS},{_TOPO_FIELDS}
                    FROM read_parquet('{parquet_path}')
                    WHERE {_TOPO_WHERE}
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-topo",
                    min_zoom=0,
                    max_zoom=12,
                    base_zoom=12,
                    drop_rate=2.0,
                    coalesce=True,
                )
                return str(out)

            @task
            def freestiler_hiking_convert(parquet_path: str) -> str:
                # Hiking-themed projection. Tag inventory derived from
                # sletuffe/OpenHikingMap mapnik XML styles (path-in-mountain,
                # tracks, symbols-peaks, symbols-1/2). Style cell paints
                # sac_scale>=T3 in red dashes, T1-T2 green, peak/saddle/cliff
                # natural-feature symbols, alpine_hut house-icons.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-hiking.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT{_COMMON_SELECT_FIELDS},{_HIKING_FIELDS}
                    FROM read_parquet('{parquet_path}')
                    WHERE {_HIKING_WHERE}
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-hiking",
                    min_zoom=0,
                    max_zoom=14,
                    base_zoom=14,
                    drop_rate=2.0,
                    coalesce=True,
                )
                return str(out)

            @task
            def freestiler_ecovoyage_convert(parquet_path: str) -> str:
                # Consolidated single-PMTiles output carrying the union of all
                # four themes (cycle / topo / railway / hiking) in ONE vector
                # layer (`austria-ecovoyage`) discriminated by a `theme` column.
                # Built FROM SCRATCH via a single optimized DuckDB query — no
                # tile-join, no pmtiles merge of the existing theme archives.
                #
                # Optimization shape:
                #   1. MATERIALIZED CTE pre-filters the parquet ONCE with the
                #      UNION of all four themes' WHERE predicates.
                #   2. Four UNION ALL BY NAME subqueries read from the same
                #      materialized base. Each emits its theme's column set
                #      (theme-specific fields + the shared _COMMON_SELECT_FIELDS);
                #      UNION ALL BY NAME pads missing columns with NULL.
                #   3. freestiler streams DuckDB rows directly into the Rust
                #      tiling engine — no Python materialization.
                # A row that matches multiple themes is emitted once per matching
                # theme so MapLibre can style each appearance independently via a
                # ["==", ["get", "theme"], "<name>"] filter clause prepended to
                # every style layer.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-ecovoyage.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    WITH base AS MATERIALIZED (
                        SELECT feature_id, geometry, tags
                        FROM read_parquet('{parquet_path}')
                        WHERE ({_CYCLE_WHERE})
                           OR ({_TOPO_WHERE})
                           OR ({_RAILWAY_WHERE})
                           OR ({_HIKING_WHERE})
                    )
                    SELECT 'cycle' AS theme,{_COMMON_SELECT_FIELDS},{_CYCLE_FIELDS}
                    FROM base WHERE {_CYCLE_WHERE}
                    UNION ALL BY NAME
                    SELECT 'topo' AS theme,{_COMMON_SELECT_FIELDS},{_TOPO_FIELDS}
                    FROM base WHERE {_TOPO_WHERE}
                    UNION ALL BY NAME
                    SELECT 'railway' AS theme,{_COMMON_SELECT_FIELDS},{_RAILWAY_FIELDS}
                    FROM base WHERE {_RAILWAY_WHERE}
                    UNION ALL BY NAME
                    SELECT 'hiking' AS theme,{_COMMON_SELECT_FIELDS},{_HIKING_FIELDS}
                    FROM base WHERE {_HIKING_WHERE}
                """
                # base_zoom + drop_rate + coalesce thin features at low
                # zooms so single-tile bytes stay browser-friendly. Without
                # these freestiler emits z=7 tiles >300 MB which crash
                # browser tabs on the consolidated view. Attempts to run
                # without coalesce destabilized freestiler — multiple tasks
                # segfault at runtime; coalesce is required for stable
                # builds at this data scale. Cost: polygons (forest /
                # landuse / water fills) get merged aggressively at low
                # zooms; country-zoom views are line-heavy. Users zoom
                # into theme-specific cells for full-density detail at z14.
                # For ecovoyage specifically max_zoom=12 (vs the standalone
                # theme tiles' z14) caps detail at city zoom — the 4
                # standalone cells still go to z14 for fine detail when
                # users click into a specific theme.
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-ecovoyage",
                    min_zoom=0,
                    max_zoom=12,
                    base_zoom=12,
                    drop_rate=2.0,
                    coalesce=True,
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
                freestiler_cycle_convert(parquet),
                freestiler_topo_convert(parquet),
                freestiler_hiking_convert(parquet),
                freestiler_ecovoyage_convert(parquet),
            ])


        notebook_austria_pipeline()
    ''').lstrip())

    # Single-DAG list/dict so the trigger cell's loop semantics
    # (`for _did in dag_ids: ...`) work unchanged. After the GTFS
    # split, this notebook owns only notebook_austria_pipeline; the
    # GTFS DAG is self-authored by the sibling gtfs-austria.py.
    dag_ids = [austria_dag_id]
    dag_files = {austria_dag_id: austria_dag_file}
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
    extra_sources: dict | None = None,
    extra_layers: list | None = None,
    mlt: bool = False,
    terrain: bool = False,
    satellite_background: bool = False,
    pitch: int = 0,
    max_pitch: int = 60,
    hillshade: bool = True,
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

    `extra_sources` / `extra_layers` (both optional) compose ADDITIONAL
    MapLibre sources + layers on top of the primary `src` vector
    source. No caller in this OSM-only notebook uses them; the
    GTFS-side sibling gtfs-austria.py uses them to overlay the
    austria-transit PMTiles on the railway/ecovoyage maps. Kept here
    for R3 parity between the two notebooks' duplicate copies of this
    helper. Each extra layer is appended verbatim — callers set
    `source` / `source-layer` themselves; the helper does NOT inject
    src into them (they're meant to reference the OTHER source).

    `mlt=True` records the inner-encoding hint in the source
    declaration. Martin currently emits MVT bytes regardless of
    inner format; the flag is here so that when MapLibre GL JS adds
    MLT decode support upstream we can flip a single bit. The
    MapLibre version is captured in a DOM `data-maplibre-version`
    attribute so a CDP probe can assert the decoder side too.

    `terrain=True` adds mapterhorn raster-DEM `terrainSource` +
    `hillshadeSource`, a `hillshade` layer rendered between the
    background and the data layers, top-level `terrain` + `sky` style
    keys, `pitch` / `maxPitch` on the Map constructor, and a
    `TerrainControl` button at top-right. `satellite_background=True`
    replaces the solid bg layer with a versatiles satellite raster
    layer fed from `https://tiles.versatiles.org/tiles/satellite/{z}/{x}/{y}`
    (webp, maxzoom 17 — verified against the upstream style.json on
    2026-05-13). `pitch` / `max_pitch` are honoured only when
    `terrain=True`; with both flags False the helper emits
    byte-identical output to the pre-2026-05 template, leaving the
    other six callers in this notebook untouched.

    `hillshade=False` together with `terrain=True` keeps the 3D
    elevation effect + sky + camera pitch + TerrainControl but drops
    the relief-shading `hills-*` layer + the `hillshadeSource`
    raster-DEM source. Useful on a satellite background where the
    imagery already renders shadows naturally.
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
    # Defensive copy + force-set source / source-layer to this cell's
    # layer_name. The previous setdefault-based approach was an R1 bug:
    # when style_layers was a shared module-level constant (the four
    # *_STYLE lists in _theme_styles), a prior cell's invocation
    # MUTATED the dicts to set source-layer="austria-<that-cell>".
    # Subsequent cells (e.g. the consolidated ecovoyage cell consuming
    # the same constants via with_theme) inherited the stale value,
    # and MapLibre tried to fetch features from a non-existent
    # source-layer inside the ecovoyage tile → blank map.
    # Always copy + always overwrite. R3 — one definition, many
    # call sites — only works when shared definitions are immutable.
    data_layers = []
    for _layer in raw_layers:
        _layer = dict(_layer)  # shallow copy — paint/filter dicts shared but not mutated
        if _layer.get("type") != "background":
            _layer["source"] = "src"
            _layer["source-layer"] = layer_name
        data_layers.append(_layer)

    # Base layers below the data layers. The default solid background is
    # swapped for a versatiles satellite raster when satellite_background
    # is on; the mapterhorn hillshade is inserted between the background
    # and the data layers when terrain is on (so vector data still draws
    # over the relief). With both flags off this collapses to the
    # original single bg layer — byte-identical output for the six
    # untouched callers.
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
        # Extra layers (e.g. transit-stops circle overlay) compose on top
        # of the primary data layers. They reference whichever source
        # the caller declared in `extra_sources` — no injection.
        *(extra_layers or []),
    ]
    layers_js = _json.dumps(all_layers, indent=2)
    source_dict = {"type": "vector", "url": f"{martin}/{source_name}"}
    if mlt:
        # Informational marker — the actual decode path depends on the
        # MapLibre GL JS version's MLT support; martin's content-type
        # response is what drives the decoder. Captured in the DOM via
        # data-mlt for the CDP probe.
        source_dict["mlt"] = True
    all_sources = {"src": source_dict, **(extra_sources or {})}
    if satellite_background:
        # Versatiles public satellite raster — webp, maxzoom 17. URL +
        # attribution copied verbatim from the upstream style.json at
        # tiles.versatiles.org/assets/styles/satellite/style.json.
        all_sources["satellite-src"] = {
            "type": "raster",
            "tiles": ["https://tiles.versatiles.org/tiles/satellite/{z}/{x}/{y}"],
            "tileSize": 256,
            "minzoom": 0,
            "maxzoom": 17,
            "attribution": "<a href='https://versatiles.org/sources/'>VersaTiles sources</a>",
        }
    if terrain:
        # Mapterhorn raster-DEM, same source the Monaco streets cell uses
        # (pre-validated CORS + MapLibre integration). One URL declared
        # twice so MapLibre can use it both for elevation (terrain) and
        # for relief shading (hillshade) without re-fetching tiles.
        _dem = {
            "type": "raster-dem",
            "url": "https://tiles.mapterhorn.com/tilejson.json",
        }
        all_sources["terrainSource"] = _dem
        if hillshade:
            all_sources["hillshadeSource"] = _dem
    sources_js = _json.dumps(all_sources)
    mlt_attr = ' data-mlt="true"' if mlt else ''

    # Conditional snippets — empty strings when terrain=False so the
    # emitted JS is character-identical to the pre-terrain template.
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
    layers: {layers_js}{terrain_extras_js}
  }},
  center: [{center[0]}, {center[1]}],
  zoom: {zoom},
{pitch_js}  attributionControl: false
}});
map_{js_var}.addControl(new maplibregl.NavigationControl({{ showZoom: true, showCompass: true }}), 'top-right');{terrain_control_js}
</script>
</body></html>"""


@app.function
def with_theme(theme: str, layers: list) -> list:
    """For the consolidated `austria-ecovoyage` cell: prepend a
    theme-equality clause to each style-layer's filter, so a paint
    rule originally targeting all railway features only paints rows
    whose `theme` discriminator column equals `theme`. Also rewrites
    each layer's `id` with an `evo-` prefix so the merged style has
    no id collisions across the four themes."""
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
def _theme_styles():
    # MapLibre style-layer lists per theme, factored out of the four
    # individual theme map cells so the consolidated austria-ecovoyage
    # cell can reuse them via with_theme(...). R3: one source of truth
    # for each theme's paint rules; both the standalone theme cell and
    # the ecovoyage cell pull from the same constants.

    RAILWAY_STYLE = [
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
    ]

    CYCLE_STYLE = [
        # Road/path context (muted)
        {"id": "ctx-road", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["motorway","trunk","primary","secondary","tertiary"]]]],
         "paint": {"line-color": "#cccccc", "line-width": 0.6}},
        {"id": "ctx-minor", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["unclassified","residential","service","track"]]]],
         "paint": {"line-color": "#dddddd", "line-width": 0.4}},
        # Cycleways
        {"id": "cycle-lane", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "cycleway"],
                     ["literal", ["lane","shared_lane","share_busway"]]]],
         "paint": {"line-color": "#3388ff",
                   "line-width": 1.2,
                   "line-dasharray": [3, 2]}},
        {"id": "cycle-track", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["==", ["get", "cycleway"], "track"],
                     ["==", ["get", "segregated"], "yes"]]],
         "paint": {"line-color": "#2e8b3a", "line-width": 1.4}},
        # Dedicated cycleway/bicycle_road highways
        {"id": "cycle-dedicated", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["==", ["get", "highway"], "cycleway"],
                     ["==", ["get", "bicycle_road"], "yes"]]],
         "paint": {"line-color": "#1e6f2c", "line-width": 1.8}},
        # Cycle routes (relation members tagged route=bicycle)
        {"id": "cycle-route", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "bicycle"]],
         "paint": {"line-color": "#3050d0",
                   "line-width": 2.0,
                   "line-opacity": 0.65}},
        # MTB difficult trails
        {"id": "cycle-mtb-hard", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "mtb_scale"],
                     ["literal", ["3","4","5","6"]]]],
         "paint": {"line-color": "#d24a1f", "line-width": 1.4,
                   "line-dasharray": [4, 2]}},
        # Bicycle amenities (parking, rental, repair)
        {"id": "cycle-amenity", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "amenity"],
                     ["literal", ["bicycle_parking","bicycle_rental","bicycle_repair_station"]]]],
         "paint": {"circle-color": "#1e6f2c",
                   "circle-radius": 3.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
    ]

    TOPO_STYLE = [
        # Landuse polygons (drawn first, below everything)
        {"id": "topo-forest", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["in", ["get", "landuse"],
                     ["literal", ["forest","wood"]]]],
         "paint": {"fill-color": "#9bbf8a", "fill-opacity": 0.55}},
        {"id": "topo-meadow", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["in", ["get", "landuse"],
                     ["literal", ["meadow","grass","orchard","vineyard","farmland","farmyard"]]]],
         "paint": {"fill-color": "#dfe9c8", "fill-opacity": 0.5}},
        {"id": "topo-urban", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["in", ["get", "landuse"],
                     ["literal", ["residential","industrial","commercial"]]]],
         "paint": {"fill-color": "#e8d8c8", "fill-opacity": 0.45}},
        # Natural polygons (water bodies, glaciers, scree)
        {"id": "topo-water", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["==", ["get", "natural"], "water"]],
         "paint": {"fill-color": "#a8c8e8", "fill-opacity": 0.85}},
        {"id": "topo-glacier", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["==", ["get", "natural"], "glacier"]],
         "paint": {"fill-color": "#eaf2ff", "fill-opacity": 0.9,
                   "fill-outline-color": "#88a8c8"}},
        # Waterways (lines)
        {"id": "topo-waterway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "waterway"], None]],
         "paint": {"line-color": "#4a90c8", "line-width": 0.8}},
        # Roads thinned for topo context
        {"id": "topo-road-major", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["motorway","trunk","primary"]]]],
         "paint": {"line-color": "#b08858", "line-width": 1.2}},
        {"id": "topo-road-minor", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["secondary","tertiary","unclassified","residential"]]]],
         "paint": {"line-color": "#b0b0b0", "line-width": 0.6}},
        # Railways (gray dashed)
        {"id": "topo-rail", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "railway"], None]],
         "paint": {"line-color": "#606060", "line-width": 0.8,
                   "line-dasharray": [3, 2]}},
        # Buildings
        {"id": "topo-building", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["!=", ["get", "building"], None]],
         "paint": {"fill-color": "#9c8c7c", "fill-opacity": 0.6}},
        # Peaks (orange triangles)
        {"id": "topo-peak", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "natural"],
                     ["literal", ["peak","volcano"]]]],
         "paint": {"circle-color": "#c8642a",
                   "circle-radius": 3,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
        # Saddles + mountain passes (yellow dots)
        {"id": "topo-saddle", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["any",
                     ["==", ["get", "natural"], "saddle"],
                     ["!=", ["get", "mountain_pass"], None]]],
         "paint": {"circle-color": "#dccb44", "circle-radius": 2.5,
                   "circle-stroke-color": "#a08e2a",
                   "circle-stroke-width": 1}},
        # Alpine huts (red squares done with small circles)
        {"id": "topo-hut", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "tourism"],
                     ["literal", ["alpine_hut","wilderness_hut"]]]],
         "paint": {"circle-color": "#cc3333",
                   "circle-radius": 3.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
    ]

    HIKING_STYLE = [
        # Base trails (anything path-like, gray underlay)
        {"id": "hike-trail-base", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["path","footway","track","bridleway","steps","pedestrian"]]]],
         "paint": {"line-color": "#888888", "line-width": 0.8}},
        # SAC scale T1-T2 (easy/mountain hiking) — green solid
        {"id": "hike-sac-easy", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["hiking","mountain_hiking"]]]],
         "paint": {"line-color": "#2e8b3a", "line-width": 1.4}},
        # SAC scale T3-T4 — red dashed
        {"id": "hike-sac-demanding", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["demanding_mountain_hiking","alpine_hiking"]]]],
         "paint": {"line-color": "#d24a1f", "line-width": 1.4,
                   "line-dasharray": [4, 2]}},
        # SAC scale T5-T6 — black dashed
        {"id": "hike-sac-extreme", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["demanding_alpine_hiking","difficult_alpine_hiking"]]]],
         "paint": {"line-color": "#202020", "line-width": 1.6,
                   "line-dasharray": [2, 3]}},
        # Hiking-route relations colored by network
        {"id": "hike-route-iwn", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"],
                    ["==", ["get", "network"], "iwn"]],
         "paint": {"line-color": "#7e22ce", "line-width": 2.2,
                   "line-opacity": 0.6}},
        {"id": "hike-route-nwn", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"],
                    ["==", ["get", "network"], "nwn"]],
         "paint": {"line-color": "#cc2233", "line-width": 1.8,
                   "line-opacity": 0.6}},
        {"id": "hike-route-rwn", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"],
                    ["==", ["get", "network"], "rwn"]],
         "paint": {"line-color": "#2244cc", "line-width": 1.4,
                   "line-opacity": 0.6}},
        {"id": "hike-route-lwn", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"],
                    ["==", ["get", "network"], "lwn"]],
         "paint": {"line-color": "#bba422", "line-width": 1.2,
                   "line-opacity": 0.6}},
        # Cliffs (gray)
        {"id": "hike-cliff", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "natural"],
                     ["literal", ["cliff","ridge","arete"]]]],
         "paint": {"line-color": "#666666", "line-width": 0.8}},
        # Mountain features
        {"id": "hike-peak", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "natural"],
                     ["literal", ["peak","volcano"]]]],
         "paint": {"circle-color": "#c8642a",
                   "circle-radius": 3,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
        {"id": "hike-saddle", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["any",
                     ["==", ["get", "natural"], "saddle"],
                     ["!=", ["get", "mountain_pass"], None]]],
         "paint": {"circle-color": "#dccb44", "circle-radius": 2.5,
                   "circle-stroke-color": "#a08e2a",
                   "circle-stroke-width": 1}},
        # Springs (blue dots)
        {"id": "hike-spring", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "spring"]],
         "paint": {"circle-color": "#4a90c8", "circle-radius": 2.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
        # Alpine huts (red)
        {"id": "hike-hut", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "tourism"],
                     ["literal", ["alpine_hut","wilderness_hut"]]]],
         "paint": {"circle-color": "#cc3333", "circle-radius": 4,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1.5}},
        # Viewpoints (turquoise eye)
        {"id": "hike-viewpoint", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "tourism"], "viewpoint"]],
         "paint": {"circle-color": "#22aaaa", "circle-radius": 3,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
    ]
    return CYCLE_STYLE, HIKING_STYLE, RAILWAY_STYLE, TOPO_STYLE


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
def _(RAILWAY_STYLE, dag_run_states, martin, mo):
    # OpenRailwayMap-aligned render of the austria-railway PMTiles
    # archive. Style is RAILWAY_STYLE (defined in the _theme_styles
    # cell) — the same list also feeds the consolidated
    # austria-ecovoyage cell via with_theme("railway", RAILWAY_STYLE).
    # Mirrors categorical color choices in OpenRailwayMap-
    # CartoCSS/standard.mss: mainline rail in blue, branch lines in
    # orange, urban transit (tram/light_rail/subway) in purple,
    # freight-yard service tracks in gray, stations as filled circles.
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
            style_layers=RAILWAY_STYLE,
        ),
        height="500px",
    )
    return


@app.cell
def _(CYCLE_STYLE, dag_run_states, martin, mo):
    # Cycle-themed render of the austria-cycle PMTiles archive. Style is
    # CYCLE_STYLE (defined in the _theme_styles cell) — also fed to the
    # consolidated austria-ecovoyage cell via
    # with_theme("cycle", CYCLE_STYLE).
    mo.stop(
        dag_run_states.get("notebook_austria_pipeline") != "success",
        f"Waiting for notebook_austria_pipeline (state="
        f"{dag_run_states.get('notebook_austria_pipeline')!r})",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-cycle",
            layer_name="austria-cycle",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=CYCLE_STYLE,
        ),
        height="500px",
    )
    return


@app.cell
def _(TOPO_STYLE, dag_run_states, martin, mo):
    # Topo-themed render of the austria-topo PMTiles archive. Style is
    # TOPO_STYLE (defined in the _theme_styles cell) — also fed to the
    # consolidated austria-ecovoyage cell via
    # with_theme("topo", TOPO_STYLE).
    mo.stop(
        dag_run_states.get("notebook_austria_pipeline") != "success",
        f"Waiting for notebook_austria_pipeline (state="
        f"{dag_run_states.get('notebook_austria_pipeline')!r})",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-topo",
            layer_name="austria-topo",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=TOPO_STYLE,
        ),
        height="500px",
    )
    return


@app.cell
def _(HIKING_STYLE, dag_run_states, martin, mo):
    # Hiking-themed render of the austria-hiking PMTiles archive. Style
    # is HIKING_STYLE (defined in the _theme_styles cell) — also fed to
    # the consolidated austria-ecovoyage cell via
    # with_theme("hiking", HIKING_STYLE).
    mo.stop(
        dag_run_states.get("notebook_austria_pipeline") != "success",
        f"Waiting for notebook_austria_pipeline (state="
        f"{dag_run_states.get('notebook_austria_pipeline')!r})",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-hiking",
            layer_name="austria-hiking",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=HIKING_STYLE,
        ),
        height="500px",
    )
    return


if __name__ == "__main__":
    app.run()
