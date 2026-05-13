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
    # this notebook touches. Same shape as osm-austria.py / osm-monaco-viz.py
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
    urls
    return airflow_public, martin


@app.cell
def _(airflow_public, mo):
    mo.md(f"""
    # Austria GTFS pipeline + unified GTFS↔OSM analysis

    Sibling to `osm-austria.py`. This notebook **writes the GTFS
    Airflow DAG** (`notebook_austria_gtfs_pipeline`) to
    `${{AIRFLOW_DAGS_DIR}}`, triggers it via the Airflow REST API at
    <{airflow_public}>, polls until success, then runs the unified
    GTFS↔OSM analysis against the persistent DuckDB at
    `/workspace/duckdb/austria.duckdb`.

    **Two-notebook contract** — for a cold deploy, open
    `osm-austria.py` **first**: it authors the OSM DAG
    (`notebook_austria_pipeline`) which produces `austria.parquet`
    and `austria-ecovoyage.pmtiles`. This GTFS DAG's
    `materialize_duckdb` task waits for `austria.parquet` via task
    retries (20 × 60s); the ecovoyage 5-theme map cell at the
    bottom of this notebook waits for `austria-ecovoyage.pmtiles`.
    Both notebooks can be open concurrently; their DAGs share the
    workspace bind-mount but never cross-reference in Airflow's
    TaskFlow API.

    **The shared persistent DuckDB**
    (`/workspace/duckdb/austria.duckdb`) is built by this notebook's
    GTFS DAG `materialize_duckdb` task. Schema layout:

    | Schema | Tables / Views | Source |
    |---|---|---|
    | `osm.*` | `features` (VIEW over austria.parquet — ~13 M features) | osm-austria.py DAG |
    | `gtfs.*` | EVERY *.parquet the feed shipped — `stops`, `routes`, `trips`, **`stop_times` (the full timetable)**, `shapes`, `calendar`, `calendar_dates`, `agency`, plus any optionals (`transfers`, `fare_*`, `frequencies`, `pathways`, …) | THIS notebook's DAG |
    | `transit.*` | `osm_stops`, `osm_route_masters`, `osm_routes`, `matched_stops`, `matched_routes`, `matched_trips` | THIS notebook's wiki-compliant joins |

    Maps below — three viewpoints on the same unified dataset:

    - **Unified transit map** — `austria-railway` PMTiles (tracks)
      + `austria-transit` PMTiles (GTFS stops as points) overlaid on
      3D mapterhorn terrain + versatiles satellite imagery. Stops
      colour-coded by wiki-tier (gtfs:stop_id / ref:IFOPT /
      spatial-last-resort) matched to OSM. The "3D explore" view.
    - **Ecovoyage 5-theme map** — `austria-ecovoyage` PMTiles
      (cycle/topo/railway/hiking) + GTFS transit overlay on a flat
      solid background. The "vector overview" view — every OSM
      polygon fill (water / forest / landuse) drawn from OSM tags.
    - **Satellite-overlay map** — `austria-ecovoyage` lines + points
      ONLY (polygon fills stripped) + GTFS transit overlay, drawn on
      top of a versatiles satellite background draped over 3D
      mapterhorn terrain (sky + camera tilt + TerrainControl on; the
      hillshade overlay is OFF because the satellite imagery already
      renders shadows). The "satellite annotation" view — relies on
      the satellite imagery to show water / forest / fields, overlays
      just the railways, roads, hiking trails, peaks, alpine huts,
      and GTFS stops as thin annotations.

    ## GTFS↔OSM unification model — see [OSM wiki: GTFS](https://wiki.openstreetmap.org/wiki/GTFS)

    Stops match by a three-tier chain — primary, fallback, last resort:

    1. **`tags['gtfs:stop_id:<feed>']`** = `stops.stop_id` (high confidence)
    2. **`tags['ref:IFOPT']`** = `stops.stop_id` (Austria uses IFOPT widely)
    3. **Spatial proximity ≤ 50 m** — last resort ONLY, when both
       tag-based tiers failed. Labelled `spatial_last_resort`.

    Routes match `gtfs:route_id:<feed>` on `type=route_master` relations
    (primary) or `ref`+`operator`/`network` heuristic (fallback). Trips
    match `gtfs:trip_id:<feed>` on `type=route` relations (rare in
    practice — most feeds produce 0 matches; the empty result is
    diagnostic).

    ## Download policy — monthly-cached, idempotent

    The GTFS DAG runs on `schedule="@monthly"` (Airflow's cron alias
    for `0 0 1 * *`). Each download task short-circuits when the
    cached file's mtime falls in the current calendar month — so
    ad-hoc / notebook-triggered re-runs within a month skip the
    network fetch entirely.

    ## Data sources

    | Source | URL |
    |---|---|
    | GTFS | `https://api.transitous.org/gtfs/at_Railway-Current-Reference-Data-2026.gtfs.zip` |
    | OSM (consumed) | `/workspace/tiles/work/austria.parquet` produced by osm-austria.py |

    ## URL strategy

    Same two-space split as the sibling notebooks: kernel-side calls
    use `AIRFLOW_API_INTERNAL_URL`; MapLibre map cells embed
    `MARTIN_PUBLIC_URL` so the browser can reach martin via the
    published host port. The diagnostic table above resolves both
    at runtime — values rotate when `port: [auto]` rotates host
    ports on rebuild.
    """)
    return


@app.cell
def _(Path, os, textwrap):
    # Self-author the GTFS pipeline DAG. Idempotent — overwriting on
    # every notebook run keeps the DAG body in sync with this notebook
    # (single source of truth: this cell IS the DAG spec). The sibling
    # osm-austria.py notebook authors notebook_austria_pipeline.py; this
    # one authors notebook_austria_gtfs_pipeline.py. Both files land in
    # /workspace/dags/ and Airflow's dag-processor picks them up via
    # AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=10.
    dags_dir = Path(os.environ.get(
        "AIRFLOW_DAGS_DIR",
        os.path.expanduser("/workspace/dags"),
    ))
    dags_dir.mkdir(parents=True, exist_ok=True)

    gtfs_dag_id = "notebook_austria_gtfs_pipeline"
    gtfs_dag_file = dags_dir / f"{gtfs_dag_id}.py"
    gtfs_dag_file.write_text(textwrap.dedent('''
        """Austria railway GTFS pipeline self-authored by gtfs-austria.py.

        Downloads the at_Railway-Current-Reference-Data-2026 GTFS feed
        from transitous.org and parses it into Parquet via gtfs-parquet
        (one .parquet per GTFS table — stops, routes, trips, stop_times,
        etc.).

        Download policy: skip-if-cached-this-month + schedule="@monthly".
        """
        import os
        from datetime import datetime, timedelta, timezone
        from pathlib import Path

        from airflow.sdk import dag, task

        # Per-feed subdir under /workspace/gtfs/ so Austria's parquet
        # output never overwrites Monaco's (or any other feed's). Same
        # pattern for `raw/` and `parquet/`.
        RAW = Path(os.path.expanduser("/workspace/gtfs/austria/raw"))
        PARQUET = Path(os.path.expanduser("/workspace/gtfs/austria/parquet"))

        # Shared with the OSM DAG via the workspace bind-mount. austria.parquet
        # is produced by the OSM DAG's pbf_to_geoparquet task — this GTFS DAG
        # consumes it inside materialize_duckdb. The cross-DAG dependency is
        # handled by Airflow retries: materialize_duckdb raises if austria.parquet
        # is missing or stale; the task's @task(retries=, retry_delay=) decorator
        # waits out the OSM DAG's wall-time without sleep loops (R4).
        TILES_WORK = Path(os.path.expanduser("/workspace/tiles/work"))
        TILES = Path(os.path.expanduser("/workspace/tiles/pmtiles"))
        DB_DIR = Path(os.path.expanduser("/workspace/duckdb"))

        # Feed-code suffix used in OSM-side tag keys like gtfs:stop_id:<feed>.
        # transitous.org's Austria railway feed publishes under this label;
        # match_gtfs_stops_to_osm auto-verifies by probing the OSM tag
        # inventory and logging the most-populated feed code if this guess
        # turns out wrong.
        _AT_FEED_CODE = "AT-Transitous"

        # Wiki-compliant predicate for OSM features that ARE stop-like (i.e.
        # GTFS stops.txt matching candidates). Single source of truth for
        # transit.osm_stops; see https://wiki.openstreetmap.org/wiki/GTFS.
        _TRANSIT_WHERE = """tags['railway'] IN ('station','stop','halt','tram_stop','subway_entrance')
                      OR tags['public_transport'] IN ('stop_position','platform','station')
                      OR tags['highway'] = 'bus_stop'
                      OR tags['amenity'] = 'ferry_terminal'"""

        # Wiki-compliant predicate for OSM relations that ARE route masters
        # (i.e. GTFS routes.txt matching candidates per PTv2).
        _ROUTE_MASTER_WHERE = """tags['type'] = 'route_master'
                      AND tags['route_master'] IN ('bus','train','tram','subway','ferry',
                                                     'trolleybus','light_rail','monorail')"""


        def _needs_regen(path: Path) -> bool:
            """Same policy as the OSM DAG — exists + this-month-mtime."""
            if not path.exists() or path.stat().st_size == 0:
                return True
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            now = datetime.now(timezone.utc)
            return (mtime.year, mtime.month) != (now.year, now.month)


        def _needs_input(path: Path) -> bool:
            """Inverse of _needs_regen — True if `path` is FRESH this month
            (a usable input). Used by tasks that depend on another DAG's
            output: they raise unless the upstream is this-month-fresh,
            and Airflow's per-task retries wait for it to land.
            """
            return not _needs_regen(path)


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
                if not _needs_regen(out):
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
                # GTFS table set: stops/routes/trips/stop_times are the
                # canonical four. Use stops.parquet as the freshness
                # canary — if its mtime is this-month it implies the
                # whole conversion ran successfully this month.
                if not _needs_regen(PARQUET / "stops.parquet"):
                    return str(PARQUET)
                feed = parse_gtfs(zip_path)
                write_parquet(feed, str(PARQUET))
                return str(PARQUET)

            # ---- The unification surface ----
            # materialize_duckdb + match_gtfs_{stops,routes,trips}_to_osm +
            # freestiler_transit_convert all live in this GTFS DAG (NOT the
            # OSM DAG) for two reasons:
            #   1. They are GTFS-anchored: every task either loads GTFS
            #      tables, joins GTFS records, or emits a GTFS-derived tile.
            #   2. The OSM DAG already finishes first in steady state (OSM
            #      cold-cache ~12 min vs GTFS ~30 s); putting the cross-
            #      cutting work in the SLOWER DAG would block the FASTER
            #      one. Putting it in the FASTER DAG (this one) means it
            #      waits via Airflow retries — backoff is principled
            #      synchronization (R4), not a sleep loop.
            #
            # Cross-DAG input: austria.parquet (produced by the OSM DAG).
            # If it's missing or stale when materialize_duckdb runs, the
            # task raises AirflowException and the @task(retries=...,
            # retry_delay=...) decorator waits out the OSM DAG's wall time.
            # Worst case: 20 retries × 60 s = 20 min — well past OSM cold
            # cache (~15 min).

            @task(retries=20, retry_delay=timedelta(seconds=60))
            def materialize_duckdb(gtfs_parquet_dir: str) -> str:
                import duckdb
                osm_parquet = TILES_WORK / "austria.parquet"
                if not _needs_input(osm_parquet):
                    raise RuntimeError(
                        f"austria.parquet not yet this-month-fresh at {osm_parquet} "
                        "(OSM DAG still running or hasn't fired this month) — "
                        "Airflow will retry"
                    )
                DB_DIR.mkdir(parents=True, exist_ok=True)
                db_path = DB_DIR / "austria.duckdb"
                # Same monthly-cache policy as every other data task. The
                # output IS the duckdb file — its mtime gates regen.
                if not _needs_regen(db_path):
                    return str(db_path)
                # Drop any stale build so CREATE OR REPLACE doesn't trip
                # over half-written WAL files from an aborted prior run.
                if db_path.exists():
                    db_path.unlink()
                wal = db_path.with_suffix(db_path.suffix + ".wal")
                if wal.exists():
                    wal.unlink()
                con = duckdb.connect(str(db_path))
                con.sql("INSTALL spatial; LOAD spatial;")
                con.sql("CREATE SCHEMA IF NOT EXISTS osm;")
                con.sql("CREATE SCHEMA IF NOT EXISTS gtfs;")
                con.sql("CREATE SCHEMA IF NOT EXISTS transit;")
                # OSM as a VIEW — zero-copy lazy read; the 13 M-feature
                # parquet stays on disk. read_parquet is re-evaluated on
                # every query, but DuckDB's column-pruning + predicate-
                # pushdown make narrow queries fast.
                con.sql(
                    "CREATE OR REPLACE VIEW osm.features AS "
                    f"SELECT * FROM read_parquet('{osm_parquet}')"
                )
                # GTFS as TABLES — small enough (<50 MB total) to
                # materialize for fast repeated joins. Loop over EVERY
                # *.parquet the feed shipped — no hardcoded list. Whatever
                # gtfs_parquet produced (stops, routes, trips, stop_times
                # (the full timetable), shapes, calendar, calendar_dates,
                # agency, transfers, fare_attributes, fare_rules,
                # frequencies, pathways, levels, feed_info, translations,
                # attributions, ...) lands as gtfs.<table_name>.
                loaded = []
                for p in sorted(Path(gtfs_parquet_dir).glob("*.parquet")):
                    table = p.stem
                    con.sql(
                        f'CREATE OR REPLACE TABLE gtfs."{table}" AS '
                        f"SELECT * FROM read_parquet('{p}')"
                    )
                    loaded.append(table)
                # Inventory log so the operator can confirm every GTFS
                # file landed (esp. stop_times — the actual timetable).
                print(
                    f"[materialize_duckdb] gtfs tables loaded ({len(loaded)}): "
                    f"{', '.join(loaded)}"
                )
                con.close()
                return str(db_path)

            @task
            def match_gtfs_stops_to_osm(db_path: str) -> str:
                # Wiki-compliant three-tier match
                # (https://wiki.openstreetmap.org/wiki/GTFS):
                #   1. Primary:  tags['gtfs:stop_id:<feed>'] = stops.stop_id
                #   2. Fallback: tags['ref:IFOPT']           = stops.stop_id
                #                (Austria uses IFOPT widely)
                #   3. Last resort ONLY: spatial proximity ≤ 50 m.
                # The spatial tier fires ONLY for stops both tag-based
                # tiers failed to match. Each row carries a match_kind
                # discriminator so downstream consumers (the analysis
                # cell + the transit map) can colour-code by tier.
                import duckdb
                con = duckdb.connect(db_path)
                con.sql("INSTALL spatial; LOAD spatial;")
                con.sql(f"""
                    CREATE OR REPLACE TABLE transit.osm_stops AS
                    SELECT
                        feature_id,
                        geometry,
                        ST_X(ST_Centroid(geometry)) AS lon,
                        ST_Y(ST_Centroid(geometry)) AS lat,
                        tags
                    FROM osm.features
                    WHERE {_TRANSIT_WHERE}
                """)
                # Inventory of stop-like OSM features by which keys they
                # carry — surfaces whether _AT_FEED_CODE is right + how
                # many features use ref:IFOPT. Operator-readable diagnostic.
                inventory = con.sql(f"""
                    SELECT
                        count(*) FILTER (
                            WHERE tags['gtfs:stop_id:{_AT_FEED_CODE}'] IS NOT NULL
                        ) AS feature_code_hits,
                        count(*) FILTER (
                            WHERE tags['ref:IFOPT'] IS NOT NULL
                        ) AS ifopt_hits,
                        count(*) AS total_stop_like_features
                    FROM transit.osm_stops
                """).fetchone()
                print(
                    f"[match_gtfs_stops_to_osm] osm_stops inventory: "
                    f"gtfs:stop_id:{_AT_FEED_CODE} hits={inventory[0]}, "
                    f"ref:IFOPT hits={inventory[1]}, "
                    f"total stop-like OSM features={inventory[2]}"
                )
                con.sql(f"""
                    CREATE OR REPLACE TABLE transit.matched_stops AS
                    WITH
                      tag_match AS (
                        SELECT s.stop_id,
                               o.feature_id   AS osm_feature_id,
                               'gtfs:stop_id' AS match_kind,
                               0.0            AS match_distance_m
                        FROM gtfs.stops s
                        JOIN transit.osm_stops o
                          ON o.tags['gtfs:stop_id:{_AT_FEED_CODE}'] = s.stop_id
                      ),
                      ifopt_match AS (
                        SELECT s.stop_id,
                               o.feature_id AS osm_feature_id,
                               'ref:IFOPT'  AS match_kind,
                               0.0          AS match_distance_m
                        FROM gtfs.stops s
                        JOIN transit.osm_stops o
                          ON o.tags['ref:IFOPT'] = s.stop_id
                        WHERE s.stop_id NOT IN (SELECT stop_id FROM tag_match)
                      ),
                      -- LAST RESORT: spatial proximity. Fires ONLY for
                      -- stops both tag-based tiers failed. Capped at
                      -- ~50 m (0.00045 deg at Austrian latitude) and
                      -- best-of-1 per stop_id. The 'spatial_last_resort'
                      -- label lets consumers visually flag these as
                      -- low-confidence matches.
                      spatial_last_resort AS (
                        SELECT s.stop_id,
                               o.feature_id           AS osm_feature_id,
                               'spatial_last_resort'  AS match_kind,
                               ST_Distance(
                                   ST_Point(s.stop_lon, s.stop_lat),
                                   ST_Point(o.lon, o.lat)
                               ) AS match_distance_m
                        FROM gtfs.stops s
                        JOIN transit.osm_stops o
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
                rates = con.sql("""
                    SELECT
                        count(*) FILTER (WHERE match_kind='gtfs:stop_id')        AS by_tag,
                        count(*) FILTER (WHERE match_kind='ref:IFOPT')           AS by_ifopt,
                        count(*) FILTER (WHERE match_kind='spatial_last_resort') AS by_proximity,
                        (SELECT count(*) FROM gtfs.stops)                         AS total_gtfs_stops
                    FROM transit.matched_stops
                """).fetchone()
                print(
                    f"[match_gtfs_stops_to_osm] match-rate: by_tag={rates[0]}, "
                    f"by_ifopt={rates[1]}, by_proximity_LAST_RESORT={rates[2]}, "
                    f"total_gtfs_stops={rates[3]} "
                    f"(unmatched={rates[3] - rates[0] - rates[1] - rates[2]})"
                )
                # Export the joined view as parquet for freestiler
                # ingestion (freestiler can't ATTACH a duckdb file mid-
                # query, so we round-trip through parquet — the same
                # pattern every other freestiler task already uses).
                transit_parquet = TILES_WORK / "austria-transit-stops.parquet"
                con.sql(f"""
                    COPY (
                        SELECT
                            CAST(s.stop_id AS VARCHAR)         AS osm_id,
                            ST_Point(s.stop_lon, s.stop_lat)   AS geometry,
                            'transit'                          AS theme,
                            s.stop_id                          AS gtfs_stop_id,
                            s.stop_name                        AS name,
                            CAST(s.location_type AS INTEGER)   AS location_type,
                            COALESCE(m.match_kind, 'unmatched') AS match_kind,
                            m.match_distance_m,
                            m.osm_feature_id
                        FROM gtfs.stops s
                        LEFT JOIN transit.matched_stops m USING (stop_id)
                    ) TO '{transit_parquet}' (FORMAT 'parquet')
                """)
                con.close()
                return str(transit_parquet)

            @task
            def match_gtfs_routes_to_osm(db_path: str) -> str:
                # Wiki maps GTFS routes.txt to OSM type=route_master
                # relations. Two-tier match:
                #   1. gtfs:route_id:<feed> tag (high confidence).
                #   2. ref+operator heuristic (route_short_name matches
                #      ref/route_ref AND agency_name appears in operator
                #      or network).
                # No spatial fallback for routes — route geometry is a
                # multi-way linestring, not a point; the wiki defines no
                # spatial-proximity convention for routes.
                import duckdb
                con = duckdb.connect(db_path)
                con.sql(f"""
                    CREATE OR REPLACE TABLE transit.osm_route_masters AS
                    SELECT feature_id, tags
                    FROM osm.features
                    WHERE {_ROUTE_MASTER_WHERE}
                """)
                # Does the agency table exist? Some GTFS feeds skip it.
                has_agency = con.sql("""
                    SELECT count(*) FROM information_schema.tables
                    WHERE table_schema='gtfs' AND table_name='agency'
                """).fetchone()[0] > 0
                if has_agency:
                    ref_match_sql = f"""
                      ref_match AS (
                        SELECT r.route_id,
                               m.feature_id  AS osm_relation_id,
                               'ref+operator' AS match_kind
                        FROM gtfs.routes r
                        JOIN gtfs.agency a USING (agency_id)
                        JOIN transit.osm_route_masters m ON
                          (m.tags['ref'] = r.route_short_name
                            OR m.tags['route_ref'] = r.route_short_name)
                          AND (
                            lower(m.tags['operator']) LIKE lower('%' || a.agency_name || '%')
                            OR lower(m.tags['network']) LIKE lower('%' || a.agency_name || '%')
                          )
                        WHERE r.route_id NOT IN (SELECT route_id FROM tag_match)
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY r.route_id ORDER BY m.feature_id
                        ) = 1
                      )
                    """
                else:
                    # No agency table → match by ref alone (lower
                    # confidence; same ref reused by multiple agencies
                    # produces duplicates). Still better than nothing.
                    ref_match_sql = """
                      ref_match AS (
                        SELECT r.route_id,
                               m.feature_id           AS osm_relation_id,
                               'ref+no_agency'        AS match_kind
                        FROM gtfs.routes r
                        JOIN transit.osm_route_masters m ON
                          m.tags['ref'] = r.route_short_name
                          OR m.tags['route_ref'] = r.route_short_name
                        WHERE r.route_id NOT IN (SELECT route_id FROM tag_match)
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY r.route_id ORDER BY m.feature_id
                        ) = 1
                      )
                    """
                con.sql(f"""
                    CREATE OR REPLACE TABLE transit.matched_routes AS
                    WITH
                      tag_match AS (
                        SELECT r.route_id,
                               m.feature_id   AS osm_relation_id,
                               'gtfs:route_id' AS match_kind
                        FROM gtfs.routes r
                        JOIN transit.osm_route_masters m
                          ON m.tags['gtfs:route_id:{_AT_FEED_CODE}'] = r.route_id
                      ),
                      {ref_match_sql}
                    SELECT * FROM tag_match
                    UNION ALL SELECT * FROM ref_match
                """)
                rates = con.sql("""
                    SELECT
                        count(*) FILTER (WHERE match_kind='gtfs:route_id')   AS by_tag,
                        count(*) FILTER (WHERE match_kind LIKE 'ref%')       AS by_ref_heuristic,
                        (SELECT count(*) FROM gtfs.routes)                    AS total_gtfs_routes
                    FROM transit.matched_routes
                """).fetchone()
                print(
                    f"[match_gtfs_routes_to_osm] match-rate: by_tag={rates[0]}, "
                    f"by_ref_heuristic={rates[1]}, total_gtfs_routes={rates[2]} "
                    f"(unmatched={rates[2] - rates[0] - rates[1]})"
                )
                con.close()
                return db_path

            @task
            def match_gtfs_trips_to_osm(db_path: str) -> str:
                # Wiki maps GTFS trips.txt to OSM type=route relations,
                # but trip IDs are rarely stable across feed versions —
                # most feeds produce 0 matches here. Emitted for
                # completeness; non-zero is a pleasant surprise.
                import duckdb
                con = duckdb.connect(db_path)
                con.sql("""
                    CREATE OR REPLACE TABLE transit.osm_routes AS
                    SELECT feature_id, tags
                    FROM osm.features
                    WHERE tags['type'] = 'route'
                      AND tags['route'] IN ('bus','train','tram','subway','ferry',
                                             'trolleybus','light_rail','monorail')
                """)
                con.sql(f"""
                    CREATE OR REPLACE TABLE transit.matched_trips AS
                    SELECT t.trip_id,
                           r.feature_id   AS osm_relation_id,
                           'gtfs:trip_id' AS match_kind
                    FROM gtfs.trips t
                    JOIN transit.osm_routes r
                      ON r.tags['gtfs:trip_id:{_AT_FEED_CODE}'] = t.trip_id
                """)
                n = con.sql("SELECT count(*) FROM transit.matched_trips").fetchone()[0]
                total = con.sql("SELECT count(*) FROM gtfs.trips").fetchone()[0]
                print(
                    f"[match_gtfs_trips_to_osm] matched {n}/{total} GTFS trips "
                    "to OSM type=route relations (0 is normal — trip IDs "
                    "rarely tagged on OSM relations)"
                )
                con.close()
                return db_path

            @task
            def freestiler_transit_convert(transit_parquet_path: str) -> str:
                # GTFS-stops-as-points PMTiles. Same shape as every other
                # freestiler task: read parquet via DuckDB SQL, stream
                # rows into the Rust tiling engine, archive as PMTiles.
                # max_zoom=14 because ~7,600 points fit comfortably at
                # full zoom; drop_rate=2.0 thins at low zooms for
                # browser-friendly tile sizes (same defaults the line/
                # polygon themes use).
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-transit.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT osm_id,
                           geometry,
                           theme,
                           gtfs_stop_id,
                           name,
                           location_type,
                           match_kind,
                           match_distance_m,
                           osm_feature_id
                    FROM read_parquet('{transit_parquet_path}')
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-transit",
                    min_zoom=0,
                    max_zoom=14,
                    base_zoom=14,
                    drop_rate=2.0,
                    coalesce=True,
                )
                return str(out)

            @task
            def reload_martin_transit(pmtiles_path: str) -> str:
                # Reload martin so it picks up the new austria-transit
                # PMTiles. Uses the same flock + readiness-probe primitives
                # as the OSM DAG's reload_martin task. The OSM DAG runs
                # its own reload_martin first (over the 6 OSM-side tiles);
                # this reload picks up the 1 new transit tile. Two
                # serialized restarts are fine — flock keeps them
                # ordered, /catalog membership check verifies end-state.
                import fcntl
                import json as _json
                import socket
                import subprocess
                import time as _time
                import urllib.request
                expected = pmtiles_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
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
                if expected not in available:
                    raise RuntimeError(
                        f"martin /catalog missing source {expected!r} "
                        f"after reload; available={available}",
                    )
                return pmtiles_path

            # ---- Chain ----
            # DuckDB enforces single-writer semantics: only ONE process may
            # hold a write connection on a given .duckdb file at a time.
            # Parallel match_* tasks against the SAME austria.duckdb file
            # are therefore impossible — the second-to-arrive task hits
            #   IOException: Conflicting lock is held in airflow worker
            # Serialize the three match tasks via Airflow's `>>` operator;
            # cost is ~10s sequential vs ~5s parallel, well worth the
            # correctness. R1 — the previous parallel layout was an R1
            # violation: the failure mode was "transient lock contention",
            # not actually transient.
            #
            # download_gtfs → gtfs_to_parquet → materialize_duckdb
            #     → match_stops → match_routes → match_trips
            #          ↘ freestiler_transit_convert → reload_martin_transit
            gtfs_dir = gtfs_to_parquet(download_gtfs())
            db = materialize_duckdb(gtfs_dir)
            stops_task = match_gtfs_stops_to_osm(db)
            routes_task = match_gtfs_routes_to_osm(db)
            trips_task = match_gtfs_trips_to_osm(db)
            # Force sequential: stops → routes → trips. Airflow's TaskFlow
            # `>>` operator on the .output attribute sets upstream/downstream
            # without altering the data flow (each match task still takes
            # `db` as its parameter; only ordering is constrained).
            stops_task >> routes_task >> trips_task
            # The transit tile only needs match_stops' parquet export — it
            # branches off here independent of routes/trips diagnostics.
            reload_martin_transit(freestiler_transit_convert(stops_task))


        notebook_austria_gtfs_pipeline()
    ''').lstrip())
    return gtfs_dag_file, gtfs_dag_id


@app.cell
def _(gtfs_dag_file, gtfs_dag_id, os, requests, time):
    # Adopt-or-trigger the GTFS DAG run, then poll to terminal state.
    # Same shape as osm-austria.py's trigger cell but scoped to ONE DAG
    # (this notebook authors only the GTFS pipeline). The ecovoyage map
    # cell below uses an additional file-existence check for the
    # austria-ecovoyage.pmtiles produced by the sibling osm-austria.py.
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
    # The dag-processor scans the dags folder every 10s; 90s gives ~9
    # scan opportunities.
    _reg_deadline = time.monotonic() + 90
    while time.monotonic() < _reg_deadline:
        _r = requests.get(
            f"{_api}/api/v2/dags/{gtfs_dag_id}", headers=_auth, timeout=5,
        )
        if _r.status_code == 200:
            if _r.json().get("is_paused"):
                requests.patch(
                    f"{_api}/api/v2/dags/{gtfs_dag_id}",
                    headers=_auth, json={"is_paused": False}, timeout=5,
                )
                time.sleep(1)
                continue
            break
        time.sleep(2)
    else:
        raise RuntimeError(
            f"Airflow never registered DAG {gtfs_dag_id} from {gtfs_dag_file}"
        )

    # Phase 2 — pick the target run. Decision rules:
    #   * non-terminal run exists (running/queued) → ADOPT it.
    #   * terminal-success run THIS calendar month → ADOPT.
    #   * neither → TRIGGER a new manual run.
    _runs = _http_with_retry(
        "GET",
        f"{_api}/api/v2/dags/{gtfs_dag_id}/dagRuns?limit=10&order_by=-logical_date",
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
            f"{_api}/api/v2/dags/{gtfs_dag_id}/dagRuns",
            headers=_auth,
            json={
                "conf": {},
                "logical_date": _now.isoformat(),
            },
            timeout=10,
        ).json()
        _run_id = _new["dag_run_id"]
    else:
        _run_id = _adopt["dag_run_id"]

    # Phase 3 — poll the target run until terminal state. 2400s (40 min)
    # covers a cold-cache run where the GTFS DAG's materialize_duckdb
    # task is waiting for austria.parquet to be produced by the OSM DAG
    # (via task retries 20 × 60 s). Warm-cache reuse (adopted success
    # run) short-circuits to ~0s.
    _poll_deadline = time.monotonic() + 2400
    _state = None
    while time.monotonic() < _poll_deadline:
        _state = _http_with_retry(
            "GET",
            f"{_api}/api/v2/dags/{gtfs_dag_id}/dagRuns/{_run_id}",
            headers=_auth,
            timeout=5,
        ).json()["state"]
        if _state in ("success", "failed"):
            break
        time.sleep(3)
    if _state not in ("success", "failed"):
        raise TimeoutError(f"DAG {gtfs_dag_id} did not finish in 40 min")
    if _state != "success":
        raise RuntimeError(f"DAG {gtfs_dag_id} ended {_state}")

    dag_run_states = {gtfs_dag_id: _state}
    dag_run_states
    return (dag_run_states,)


@app.function
# Same helper as osm-austria.py — keep parity. R3 trade-off: cross-
# notebook DRY would extract this to notebooks/_lib_austria.py, but
# marimo notebooks are conventionally self-contained .py files (the
# Monaco sibling also keeps its own copy). Drift risk is low — the
# four opt-in kwargs (terrain, satellite_background, pitch, max_pitch)
# are stable. KEEP THIS IN SYNC WITH notebooks/osm-austria.py.
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

    See the sibling osm-austria.py docstring for the full description
    of the opt-in terrain/satellite kwargs. With all defaults the
    output is byte-identical to the pre-terrain template.

    `hillshade=False` together with `terrain=True` keeps the 3D
    elevation effect + sky + camera pitch + TerrainControl but drops
    the relief-shading `hills-*` layer + the `hillshadeSource`
    raster-DEM source. Useful on a satellite background where the
    imagery already renders shadows naturally and an explicit
    hillshade overlay is duplicative.
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
    # Always copy + always overwrite. R3 — one definition, many call
    # sites — only works when shared definitions are immutable.
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
    # and the data layers when terrain is on. With both flags off this
    # collapses to the original single bg layer.
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
    source_dict = {"type": "vector", "url": f"{martin}/{source_name}"}
    if mlt:
        source_dict["mlt"] = True
    all_sources = {"src": source_dict, **(extra_sources or {})}
    if satellite_background:
        # Versatiles public satellite raster — webp, maxzoom 17. URL +
        # attribution verbatim from the upstream style.json at
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
    """Same helper as osm-austria.py. Prepends a theme-equality clause
    to each style-layer's filter for the consolidated ecovoyage cell.
    KEEP THIS IN SYNC WITH notebooks/osm-austria.py."""
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


@app.function
def lines_and_points_only(layers: list) -> list:
    """Filter out fill-type layers so the satellite background shows
    through. Used by the satellite-overlay map cell — polygon fills
    (water / forest / glacier / farmland / residential) would
    otherwise paint over the imagery satellite already renders.
    Run AFTER `with_theme(...)` so the evo-prefixed layer IDs survive.
    """
    return [layer for layer in layers if layer.get("type") != "fill"]


@app.cell
def _theme_styles():
    # MapLibre style-layer lists per theme. Copied verbatim from
    # osm-austria.py's _theme_styles cell — the ecovoyage 5-theme map
    # below needs all four OSM theme constants plus TRANSIT_STYLE.
    # KEEP THIS IN SYNC WITH notebooks/osm-austria.py.

    RAILWAY_STYLE = [
        {"id": "rail-tunnel", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "tunnel"], None]],
         "paint": {"line-color": "#888888", "line-width": 1.0,
                   "line-dasharray": [2, 2]}},
        {"id": "rail-construction", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "construction"], None]],
         "paint": {"line-color": "#aaaaaa", "line-width": 1.0,
                   "line-dasharray": [4, 2]}},
        {"id": "rail-disused", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["!=", ["get", "abandoned"], None],
                     ["!=", ["get", "disused"], None],
                     ["!=", ["get", "razed"], None]]],
         "paint": {"line-color": "#cccccc", "line-width": 0.8}},
        {"id": "rail-service", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "service"], None]],
         "paint": {"line-color": "#888888", "line-width": 0.8}},
        {"id": "rail-branch", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "usage"], "main"],
                    ["==", ["get", "service"], None]],
         "paint": {"line-color": "#d97a23", "line-width": 1.1}},
        {"id": "rail-mainline", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "rail"],
                    ["==", ["get", "usage"], "main"],
                    ["==", ["get", "service"], None]],
         "paint": {"line-color": "#1f6bb5", "line-width": 1.6}},
        {"id": "rail-urban", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "railway"],
                     ["literal", ["light_rail","subway","tram",
                                  "narrow_gauge","monorail","funicular"]]]],
         "paint": {"line-color": "#8b3aa8", "line-width": 1.3}},
        {"id": "rail-preserved", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "railway"],
                     ["literal", ["preserved","miniature"]]]],
         "paint": {"line-color": "#5a7c2f", "line-width": 0.9}},
        {"id": "rail-bridge", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "bridge"], None],
                    ["==", ["get", "railway"], "rail"]],
         "paint": {"line-color": "#1f6bb5", "line-width": 2.0,
                   "line-opacity": 0.9}},
        {"id": "rail-station", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["any",
                     ["==", ["get", "railway"], "station"],
                     ["==", ["get", "public_transport"], "station"]]],
         "paint": {"circle-color": "#1f6bb5", "circle-radius": 3,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
        {"id": "rail-halt", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "railway"],
                     ["literal", ["halt","stop"]]]],
         "paint": {"circle-color": "#888888", "circle-radius": 2,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 0.8}},
    ]

    CYCLE_STYLE = [
        {"id": "cycle-road", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["motorway","trunk","primary","secondary",
                                  "tertiary","unclassified","residential"]]]],
         "paint": {"line-color": "#cccccc", "line-width": 0.6}},
        {"id": "cycle-track", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "track"]],
         "paint": {"line-color": "#9b6b3f", "line-width": 0.8,
                   "line-dasharray": [3, 2]}},
        {"id": "cycle-path", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["path","footway","bridleway"]]]],
         "paint": {"line-color": "#a83232", "line-width": 0.6,
                   "line-dasharray": [2, 2]}},
        {"id": "cycle-cycleway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "cycleway"]],
         "paint": {"line-color": "#2a78b8", "line-width": 1.4}},
        {"id": "cycle-bicycle-road", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "bicycle_road"], "yes"]],
         "paint": {"line-color": "#1d5a8e", "line-width": 1.6}},
        {"id": "cycle-lane-shared", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["==", ["get", "cycleway"], "lane"],
                     ["==", ["get", "cycleway:left"], "lane"],
                     ["==", ["get", "cycleway:right"], "lane"],
                     ["==", ["get", "cycleway:both"], "lane"]]],
         "paint": {"line-color": "#5aa3d5", "line-width": 1.0,
                   "line-dasharray": [1, 1]}},
        {"id": "cycle-route", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "bicycle"]],
         "paint": {"line-color": "#22aa55", "line-width": 1.2,
                   "line-opacity": 0.7}},
        {"id": "cycle-parking", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "amenity"], "bicycle_parking"]],
         "paint": {"circle-color": "#22aa55", "circle-radius": 2.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 0.8}},
    ]

    TOPO_STYLE = [
        {"id": "topo-water", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["any",
                     ["==", ["get", "natural"], "water"],
                     ["==", ["get", "landuse"], "reservoir"]]],
         "paint": {"fill-color": "#a8c8e8", "fill-outline-color": "#5a8fb8"}},
        {"id": "topo-forest", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["any",
                     ["==", ["get", "natural"], "wood"],
                     ["==", ["get", "landuse"], "forest"]]],
         "paint": {"fill-color": "#c5dec5", "fill-opacity": 0.7}},
        {"id": "topo-glacier", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["==", ["get", "natural"], "glacier"]],
         "paint": {"fill-color": "#f0f8ff", "fill-outline-color": "#a0c0d0"}},
        {"id": "topo-farmland", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["in", ["get", "landuse"],
                     ["literal", ["farmland","farmyard","orchard","vineyard","meadow"]]]],
         "paint": {"fill-color": "#f0e8c8", "fill-opacity": 0.5}},
        {"id": "topo-residential", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["==", ["get", "landuse"], "residential"]],
         "paint": {"fill-color": "#e8d8c8", "fill-opacity": 0.6}},
        {"id": "topo-waterway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "waterway"], None]],
         "paint": {"line-color": "#5a8fb8", "line-width": 0.6}},
        {"id": "topo-road-major", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["motorway","trunk","primary"]]]],
         "paint": {"line-color": "#d0a060", "line-width": 1.4}},
        {"id": "topo-road-minor", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["secondary","tertiary","unclassified","residential"]]]],
         "paint": {"line-color": "#d0d0d0", "line-width": 0.7}},
        {"id": "topo-rail", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "railway"], None]],
         "paint": {"line-color": "#555555", "line-width": 0.6}},
        {"id": "topo-boundary", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "boundary"], "administrative"]],
         "paint": {"line-color": "#a040c0", "line-width": 0.5,
                   "line-dasharray": [3, 2], "line-opacity": 0.5}},
        {"id": "topo-peak", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "peak"]],
         "paint": {"circle-color": "#8b4513", "circle-radius": 2.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 0.8}},
        {"id": "topo-aerialway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "aerialway"], None]],
         "paint": {"line-color": "#888888", "line-width": 0.4,
                   "line-dasharray": [1, 2]}},
    ]

    HIKING_STYLE = [
        {"id": "hike-trail-easy", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["==", ["get", "highway"], "path"],
                     ["==", ["get", "highway"], "footway"]],
                    ["==", ["get", "sac_scale"], None]],
         "paint": {"line-color": "#d97a23", "line-width": 0.6,
                   "line-dasharray": [3, 2]}},
        {"id": "hike-trail-sac", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "sac_scale"], None]],
         "paint": {"line-color": "#a83232", "line-width": 1.0}},
        {"id": "hike-route", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"]],
         "paint": {"line-color": "#cc4444", "line-width": 1.3,
                   "line-opacity": 0.7}},
        {"id": "hike-bridleway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "bridleway"]],
         "paint": {"line-color": "#8b6f47", "line-width": 0.8,
                   "line-dasharray": [4, 2]}},
        {"id": "hike-steps", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "steps"]],
         "paint": {"line-color": "#444444", "line-width": 1.2,
                   "line-dasharray": [1, 1]}},
        {"id": "hike-peak", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "peak"]],
         "paint": {"circle-color": "#8b4513", "circle-radius": 3,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
        {"id": "hike-saddle", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "saddle"]],
         "paint": {"circle-color": "#b08040", "circle-radius": 2.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 0.8}},
        {"id": "hike-spring", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "spring"]],
         "paint": {"circle-color": "#4a90c8", "circle-radius": 2.5,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
        {"id": "hike-hut", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "tourism"],
                     ["literal", ["alpine_hut","wilderness_hut"]]]],
         "paint": {"circle-color": "#cc3333", "circle-radius": 4,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1.5}},
        {"id": "hike-viewpoint", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "tourism"], "viewpoint"]],
         "paint": {"circle-color": "#22aaaa", "circle-radius": 3,
                   "circle-stroke-color": "#ffffff",
                   "circle-stroke-width": 1}},
    ]

    # ---- TRANSIT_STYLE — GTFS-stops overlay layers ----
    # Used by both the standalone transit map cell AND the ecovoyage
    # 5-theme cell. Layers point at the SECOND martin source
    # (`austria-transit`, not `austria-ecovoyage`) so this style stack
    # composes via build_pipeline_maplibre_html's extra_sources /
    # extra_layers kwargs — NOT via with_theme.
    #
    # Colour coding by match_kind matches the wiki's confidence
    # hierarchy: green = high-confidence tag match (gtfs:stop_id),
    # blue = high-confidence IFOPT match, orange = low-confidence
    # spatial-last-resort, red = unmatched.
    TRANSIT_STYLE = [
        {"id": "transit-stops", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["==", ["geometry-type"], "Point"],
         "paint": {
            "circle-radius": [
                "interpolate", ["linear"], ["zoom"],
                6, 2,
                10, 3.5,
                14, 5.5,
            ],
            "circle-color": [
                "match", ["get", "match_kind"],
                "gtfs:stop_id",        "#2ca02c",
                "ref:IFOPT",           "#1f77b4",
                "spatial_last_resort", "#ff7f0e",
                "#d62728",
            ],
            "circle-stroke-width": 1,
            "circle-stroke-color": "#ffffff",
            "circle-opacity": 0.85,
         }},
    ]
    return CYCLE_STYLE, HIKING_STYLE, RAILWAY_STYLE, TOPO_STYLE, TRANSIT_STYLE


@app.cell
def _(dag_run_states, mo):
    # Unified GTFS+OSM analysis — queries the persistent DuckDB at
    # /workspace/duckdb/austria.duckdb that the GTFS DAG's
    # materialize_duckdb task builds. Both pipelines' outputs live
    # under one roof (schemas: osm.*, gtfs.*, transit.*).
    #
    # mo.stop() waits gracefully for the GTFS DAG: its terminal task
    # (reload_martin_transit) only succeeds after materialize_duckdb
    # has populated the duckdb file, so DAG-success implies queryable
    # DB. materialize_duckdb itself blocks on austria.parquet being
    # this-month-fresh (via @task retries) — so GTFS DAG success
    # implies OSM DAG has at least produced austria.parquet.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    import duckdb
    con = duckdb.connect(
        "/workspace/duckdb/austria.duckdb",
        read_only=True,
    )
    con.sql("INSTALL spatial; LOAD spatial;")

    # ---- Inventory + match-rate ----
    # One row of summary statistics that answers: how unified is this
    # database, and how well did the GTFS↔OSM joins work?
    unified_summary = con.sql("""
        SELECT
          (SELECT count(*) FROM osm.features)                                     AS osm_features,
          (SELECT count(*) FROM transit.osm_stops)                                AS osm_stop_like_features,
          (SELECT count(*) FROM gtfs.stops)                                       AS gtfs_stops,
          (SELECT count(*) FROM gtfs.routes)                                      AS gtfs_routes,
          (SELECT count(*) FROM gtfs.trips)                                       AS gtfs_trips,
          (SELECT count(*) FROM gtfs.stop_times)                                  AS gtfs_stop_times,
          (SELECT count(*) FROM transit.matched_stops
            WHERE match_kind='gtfs:stop_id')                                      AS stops_matched_by_tag,
          (SELECT count(*) FROM transit.matched_stops
            WHERE match_kind='ref:IFOPT')                                         AS stops_matched_by_ifopt,
          (SELECT count(*) FROM transit.matched_stops
            WHERE match_kind='spatial_last_resort')                               AS stops_matched_by_proximity_LAST_RESORT,
          (SELECT count(*) FROM gtfs.stops
            WHERE stop_id NOT IN (SELECT stop_id FROM transit.matched_stops))     AS stops_unmatched,
          (SELECT count(*) FROM transit.matched_routes
            WHERE match_kind='gtfs:route_id')                                     AS routes_matched_by_tag,
          (SELECT count(*) FROM transit.matched_routes
            WHERE match_kind LIKE 'ref%')                                         AS routes_matched_by_ref_heuristic,
          (SELECT count(*) FROM gtfs.routes
            WHERE route_id NOT IN (SELECT route_id FROM transit.matched_routes))  AS routes_unmatched,
          (SELECT count(*) FROM transit.matched_trips)                            AS trips_matched
    """).pl()

    # ---- Top routes by distinct stops served ----
    # Pure-GTFS query (no OSM join) — runs entirely inside duckdb
    # against gtfs.* tables. Single source of truth: no in-memory
    # polars dataframes.
    df_route_stops = con.sql("""
        SELECT
            r.route_short_name,
            r.route_long_name,
            count(DISTINCT st.stop_id) AS n_stops
        FROM gtfs.routes r
        JOIN gtfs.trips t       USING (route_id)
        JOIN gtfs.stop_times st USING (trip_id)
        GROUP BY r.route_id, r.route_short_name, r.route_long_name
        ORDER BY n_stops DESC
        LIMIT 15
    """).pl()

    # ---- Cross-dataset proof-of-life ----
    # The query that's IMPOSSIBLE without unification: every OSM
    # railway=station feature that has GTFS service, ranked by how
    # many distinct GTFS routes serve it. Joins osm.features ↔
    # transit.matched_stops ↔ gtfs.stop_times ↔ gtfs.trips ↔
    # gtfs.routes in one DuckDB query.
    top_stations = con.sql("""
        SELECT
            o.feature_id,
            o.tags['name']                       AS station_name,
            count(DISTINCT t.route_id)            AS routes_serving,
            count(DISTINCT st.trip_id)            AS trips_serving
        FROM osm.features o
        JOIN transit.matched_stops m ON m.osm_feature_id = o.feature_id
        JOIN gtfs.stop_times st USING (stop_id)
        JOIN gtfs.trips t      USING (trip_id)
        WHERE o.tags['railway'] = 'station'
        GROUP BY o.feature_id, o.tags['name']
        ORDER BY routes_serving DESC
        LIMIT 25
    """).pl()

    con.close()
    mo.vstack([
        mo.md("**Unified inventory + GTFS↔OSM match-rate** "
              "(stops via wiki-compliant tier chain: gtfs:stop_id → "
              "ref:IFOPT → spatial last-resort)"),
        unified_summary,
        mo.md("**Top 25 OSM `railway=station` features by GTFS service** "
              "(cross-dataset query — impossible without unified DuckDB)"),
        top_stations,
    ])
    return (df_route_stops,)


@app.cell
def _(df_route_stops):
    df_route_stops
    return


@app.cell
def _(dag_run_states, martin, mo):
    # Unified transit map — austria-railway PMTiles as the base (the
    # tracks themselves, from the OSM DAG) + austria-transit PMTiles
    # as a second source (the GTFS stops as points, from THIS GTFS DAG).
    # 3D mapterhorn terrain + versatiles satellite imagery on top.
    #
    # Replaces the previous folium FastMarkerCluster cell. Two wins:
    #   1. The stops are a VECTOR-TILE layer (PMTiles + martin), not
    #      a 1-MB inline GeoJSON. Browser handles tile streaming +
    #      client-side rendering at every zoom level.
    #   2. The colour-coding by match_kind is the VISIBLE result of
    #      the GTFS↔OSM unification: green/blue dots = high-confidence
    #      tag matches, orange = spatial last-resort, red = unmatched.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin, "austria-railway",
            layer_name="austria-railway",
            center=[13.3, 47.7],
            zoom=7,
            extra_sources={
                "transit-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-transit",
                },
            },
            extra_layers=[
                {"id": "transit-stops-overlay", "type": "circle",
                 "source": "transit-src",
                 "source-layer": "austria-transit",
                 "filter": ["==", ["geometry-type"], "Point"],
                 "paint": {
                    "circle-radius": [
                        "interpolate", ["linear"], ["zoom"],
                        6, 2,
                        10, 3.5,
                        14, 5.5,
                    ],
                    "circle-color": [
                        "match", ["get", "match_kind"],
                        "gtfs:stop_id",        "#2ca02c",
                        "ref:IFOPT",           "#1f77b4",
                        "spatial_last_resort", "#ff7f0e",
                        "#d62728",
                    ],
                    "circle-stroke-width": 1,
                    "circle-stroke-color": "#ffffff",
                    "circle-opacity": 0.85,
                 }},
            ],
            terrain=True,
            satellite_background=True,
            pitch=45,
            max_pitch=85,
        ),
        height="500px",
    )
    return


@app.cell
def _(
    CYCLE_STYLE,
    HIKING_STYLE,
    Path,
    RAILWAY_STYLE,
    TOPO_STYLE,
    TRANSIT_STYLE,
    dag_run_states,
    martin,
    mo,
):
    # Consolidated austria-ecovoyage render — ALL FOUR OSM themes
    # layered into a single MapLibre map served from the single
    # austria-ecovoyage.pmtiles archive (one vector layer with a
    # `theme` discriminator column) PLUS the GTFS transit stops
    # composited on top as a second MapLibre source (austria-transit).
    #
    # Style stacking order: topo as base → railway lines on top →
    # cycle routes over railway → hiking trails on top → GTFS transit
    # stops on top of everything else.
    #
    # Two cross-notebook dependencies:
    #   1. austria-ecovoyage.pmtiles is produced by osm-austria.py's
    #      freestiler_ecovoyage_convert task — verified by file
    #      existence below (gtfs-austria.py does not author the OSM DAG).
    #   2. austria-transit.pmtiles is produced by THIS notebook's
    #      freestiler_transit_convert task — gated via the GTFS DAG
    #      state check.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    _ecovoyage_pmtiles = Path("/workspace/tiles/pmtiles/austria-ecovoyage.pmtiles")
    mo.stop(
        not _ecovoyage_pmtiles.exists() or _ecovoyage_pmtiles.stat().st_size == 0,
        "`austria-ecovoyage.pmtiles` not yet present — open and run "
        "`osm-austria.py` first. Its OSM DAG produces this tile via "
        "the `freestiler_ecovoyage_convert` task.",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-ecovoyage",
            layer_name="austria-ecovoyage",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=[
                *with_theme("topo", TOPO_STYLE),
                *with_theme("railway", RAILWAY_STYLE),
                *with_theme("cycle", CYCLE_STYLE),
                *with_theme("hiking", HIKING_STYLE),
            ],
            extra_sources={
                "transit-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-transit",
                },
            },
            extra_layers=TRANSIT_STYLE,
        ),
        height="500px",
    )
    return


@app.cell
def _(
    CYCLE_STYLE,
    HIKING_STYLE,
    Path,
    RAILWAY_STYLE,
    TOPO_STYLE,
    TRANSIT_STYLE,
    dag_run_states,
    martin,
    mo,
):
    # Satellite-overlay map — versatiles satellite imagery as the
    # background; OSM-derived lines + points (railways, roads,
    # hiking trails, cycle routes, peaks, alpine huts) overlaid as
    # thin annotations; GTFS stops on top via TRANSIT_STYLE.
    #
    # Polygon-fill layers (water / forest / glacier / farmland /
    # residential — all from TOPO_STYLE) are stripped by
    # `lines_and_points_only` so the satellite imagery shows through
    # everywhere except where a deliberate line or point glyph
    # annotates the scene. The satellite already renders water as
    # blue, forest as green, fields as tan — overlaying OSM-tagged
    # polygon fills would be duplicative + obscure the imagery.
    #
    # 3D terrain + sky + camera pitch + TerrainControl are ON (same
    # mapterhorn DEM source the transit map cell uses) so the satellite
    # imagery drapes over the elevation model and the user can tilt the
    # view. The hillshade layer is OFF — relief shading is redundant
    # with the satellite's own shadow rendering and would only obscure
    # the imagery.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    _ecovoyage_pmtiles = Path("/workspace/tiles/pmtiles/austria-ecovoyage.pmtiles")
    mo.stop(
        not _ecovoyage_pmtiles.exists() or _ecovoyage_pmtiles.stat().st_size == 0,
        "`austria-ecovoyage.pmtiles` not yet present — open and run "
        "`osm-austria.py` first. Its OSM DAG produces this tile via "
        "the `freestiler_ecovoyage_convert` task.",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-ecovoyage",
            layer_name="austria-ecovoyage",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=[
                *lines_and_points_only(with_theme("topo", TOPO_STYLE)),
                *lines_and_points_only(with_theme("railway", RAILWAY_STYLE)),
                *lines_and_points_only(with_theme("cycle", CYCLE_STYLE)),
                *lines_and_points_only(with_theme("hiking", HIKING_STYLE)),
            ],
            extra_sources={
                "transit-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-transit",
                },
            },
            extra_layers=TRANSIT_STYLE,
            satellite_background=True,
            terrain=True,
            hillshade=False,
            pitch=45,
            max_pitch=85,
        ),
        height="500px",
    )
    return


if __name__ == "__main__":
    app.run()
