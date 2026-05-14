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
    versatiles_assets     = _resolved["VERSATILES_ASSETS_PUBLIC_URL"]
    urls
    return airflow_public, martin, versatiles_assets


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
    - **Satellite-overlay map** — versatiles satellite imagery
      draped over 3D mapterhorn terrain, with a dedicated zoom-banded
      transport-network overlay inspired by Artaria's 1911
      Eisenbahnkarte von Österreich-Ungarn:
      - **PRIMARY** railways: k.k. Staatsbahn red mainline (with
        white halo + a thin white center stripe at city zoom for
        the period "double-track" signature), deeper red branch
        lines, k.u. green urban transit (tram / light-rail / subway).
      - **SECONDARY** cycle network: deep teal-ink national /
        international routes (halo, visible from country zoom),
        mid-teal regional, light-teal local, navy dedicated
        cycleways.
      - **SECONDARY** hiking network: sienna long-distance routes
        (halo, visible from country zoom) for named `route=hiking`
        relations. Individual SAC-graded trails render with the
        OSM-wiki ["Hiking trails rendering proposal 1"](https://wiki.openstreetmap.org/wiki/File:Hiking_trails_rendering_proposal_1.png)
        encoding: **colour = difficulty** (T1 red `#c62828`, T2
        orange `#ef6c00`, T3 purple `#7b1fa2`, T4+ cyan `#03a9f4`,
        collapsing T4 / T5 / T6); **line pattern = visibility** —
        solid for `trail_visibility=excellent` (or untagged), dashed
        for `good`/`intermediate` ("sometimes hard to follow"),
        dotted for `bad`/`horrible`/`no` ("no clear path").
      - **TERTIARY** generic footpaths: warm ochre dashed, faint —
        the only tier that holds back until city zoom.
      - **GTFS stops**: uniform white-fill / dark-stroke dots at
        every zoom (no `match_kind` colour coding — see the
        analysis cell above for the diagnostic), with **Noto Sans
        name labels at z11+** (collision-avoided). Text rendering
        uses the versatiles-glyphs-rs SDF font protocol
        (https://github.com/versatiles-org/versatiles-glyphs-rs)
        served by the versatiles-frontend layer at
        `{{VERSATILES_ASSETS_PUBLIC_URL}}/fonts/`.

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

        # Predicate for OSM features that ARE station anchors — the
        # roll-up target for platform-granularity GTFS stops. A station
        # anchor DONATES the parent station id + name + location to all
        # its platforms. railway=station is the canonical heavy/regional
        # rail anchor; railway=halt is a small station (a halt IS a
        # station in GTFS terms — ~850 AT halt nodes, and 277 of the
        # otherwise-unmapped GTFS stations sit within 250 m of one);
        # public_transport=station is the PTv2 site node for tram/bus
        # interchanges.
        _STATION_ANCHOR_WHERE = """tags['railway'] IN ('station', 'halt')
                      OR tags['public_transport'] = 'station'"""

        # Platform-to-station snap radius for the spatial fallbacks in
        # transit.station_members (tier-3 platform→anchor, and the
        # parent_anchor GTFS-station→anchor resolution). NOT the same
        # distance as the matched_stops `spatial_last_resort` tier
        # (0.00045 deg ≈ 50 m): that snaps a GTFS stop POINT to the OSM
        # feature for the SAME physical object, so 50 m is a generous
        # "same thing" tolerance. Here we snap a PLATFORM (or a GTFS
        # station coord) to its PARENT STATION NODE — genuinely different
        # objects that sit 100-300 m apart on a large through-station
        # (Wien Hbf's platform field spans ~250 m; the GTFS station coord
        # and the OSM node centroid can each sit anywhere within it).
        # 300 m reaches the parent node from the furthest platform edge
        # without bridging to a neighbour: a census of the AT feed found
        # only ONE station whose nearest anchor falls between 250 m and
        # 600 m (Mistelbach, 253 m), so 300 m is safely below inter-
        # station spacing. 1 deg lat ≈ 111320 m at 47.5°N → 0.002695.
        _STATION_SNAP_DEG = 0.002695  # ≈ 300 m at Austrian latitude

        # Bare generic station-type names (no place qualifier) that an
        # OSM station node sometimes carries — the place is left to map
        # context. When the resolved anchor name is one of these, the
        # GTFS parent-station name ("Innsbruck Hauptbahnhof") is the
        # better display label. Used by both the tier-1 name pick AND
        # the per-station name consolidation in transit.station_members.
        _GENERIC_NAME_SET = (
            "'hauptbahnhof', 'bahnhof', 'bahnhst', 'bahnhst.', 'hbf', "
            "'bf', 'bf.', 'station', 'bahnsteig'"
        )

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

                # ---- Station roll-up: transit.station_members ----
                # Transitous publishes AT rail stops at PLATFORM
                # granularity, so transit.matched_stops points at OSM
                # platform / stop_position features — never the
                # railway=station node. Downstream consumers (the Top 25
                # ranking, the transit-map labels) want the STATION, not
                # the platform. Resolve every GTFS stop_id to a parent
                # station via a 4-tier chain:
                #   1. gtfs_parent — GTFS parent_station. The Transitous
                #      feed models the hierarchy natively; PRIMARY tier,
                #      covers Wien Hbf (44 child platforms).
                #   2. uic_ref     — the stop's matched OSM feature shares
                #      a uic_ref with a station anchor.
                #   3. spatial     — nearest station anchor within ~250 m
                #      of the stop's GTFS coords (_STATION_SNAP_DEG).
                #   4. self        — unresolvable: the stop is its own
                #      station.
                # Station IDENTITY (id / name / lon / lat) comes from the
                # OSM station anchor where correlatable, GTFS otherwise.
                # Every GTFS stop_id appears in exactly one tier.
                con.sql(f"""
                    CREATE OR REPLACE TABLE transit.station_members AS
                    WITH
                      anchors AS (
                        SELECT
                            feature_id,
                            tags['name']    AS station_name,
                            tags['uic_ref'] AS uic_ref,
                            ST_X(ST_Centroid(geometry)) AS lon,
                            ST_Y(ST_Centroid(geometry)) AS lat
                        FROM osm.features
                        WHERE {_STATION_ANCHOR_WHERE}
                      ),
                      -- Single best matched_stops row per stop_id (tag
                      -- matches beat spatial; osm_feature_id breaks ties
                      -- deterministically). matched_stops has up to 3
                      -- rows per stop_id — this collapses the grain.
                      best_match AS (
                        SELECT stop_id, osm_feature_id, match_kind
                        FROM transit.matched_stops
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY stop_id
                            ORDER BY CASE match_kind
                                       WHEN 'gtfs:stop_id' THEN 0
                                       WHEN 'ref:IFOPT'     THEN 1
                                       ELSE 2 END,
                                     osm_feature_id
                        ) = 1
                      ),
                      -- Resolve the best OSM station anchor for each
                      -- GTFS parent-station ROW. The station row itself
                      -- usually matches a PLATFORM (not the
                      -- railway=station node), so a direct anchor match
                      -- is rare for big stations — fall back to the
                      -- nearest anchor within _STATION_SNAP_DEG of the
                      -- station row's own GTFS coords. This is what makes
                      -- the merged identity come from OSM (id + name +
                      -- location) rather than a synthetic GTFS id for
                      -- Wien Hbf / Linz / Salzburg / etc.
                      parent_anchor AS (
                        SELECT
                            ps.stop_id   AS parent_stop_id,
                            ps.stop_name AS parent_name,
                            ps.stop_lon  AS parent_lon,
                            ps.stop_lat  AS parent_lat,
                            a.feature_id    AS anchor_feature_id,
                            a.station_name  AS anchor_name,
                            a.lon AS anchor_lon,
                            a.lat AS anchor_lat
                        FROM gtfs.stops ps
                        LEFT JOIN best_match pbm
                               ON pbm.stop_id = ps.stop_id
                        LEFT JOIN anchors a
                               ON a.feature_id = pbm.osm_feature_id
                               OR ST_DWithin(
                                      ST_Point(ps.stop_lon, ps.stop_lat),
                                      ST_Point(a.lon, a.lat),
                                      {_STATION_SNAP_DEG}
                                  )
                        WHERE ps.stop_id IN (
                            SELECT DISTINCT parent_station FROM gtfs.stops
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
                      -- TIER 1: GTFS parent_station. Group by the GTFS
                      -- parent_station; the identity comes from the OSM
                      -- anchor resolved above (direct match or spatial),
                      -- falling back to the GTFS station row's own
                      -- identity only when no anchor is within range.
                      tier1 AS (
                        SELECT
                            s.stop_id,
                            COALESCE(pa.anchor_feature_id,
                                     'gtfs/' || s.parent_station)
                                AS station_feature_id,
                            -- Identity (id + location) comes from the
                            -- OSM anchor. Name normally does too — but
                            -- some OSM station nodes carry only a bare
                            -- generic name ("Hauptbahnhof", "Bahnhof",
                            -- ...) with the place qualifier left to map
                            -- context. The GTFS parent-station row's name
                            -- ("Innsbruck Hauptbahnhof") is the better
                            -- display label, so prefer it whenever the
                            -- OSM name is one of those bare terms. A
                            -- final consolidation pass below then
                            -- propagates this good name to the SAME
                            -- station's tier-2/3 members.
                            CASE
                              WHEN lower(trim(pa.anchor_name))
                                   IN ({_GENERIC_NAME_SET})
                              THEN COALESCE(pa.parent_name, pa.anchor_name)
                              ELSE COALESCE(pa.anchor_name, pa.parent_name)
                            END AS station_name,
                            COALESCE(pa.anchor_lon, pa.parent_lon)
                                AS station_lon,
                            COALESCE(pa.anchor_lat, pa.parent_lat)
                                AS station_lat,
                            'gtfs_parent' AS resolution_kind
                        FROM gtfs.stops s
                        LEFT JOIN parent_anchor pa
                               ON pa.parent_stop_id = s.parent_station
                        WHERE NULLIF(s.parent_station, '') IS NOT NULL
                      ),
                      -- TIER 2: shared uic_ref. The stop's own matched
                      -- OSM feature carries a uic_ref that also
                      -- identifies a station anchor.
                      tier2 AS (
                        SELECT
                            s.stop_id,
                            a.feature_id  AS station_feature_id,
                            a.station_name,
                            a.lon AS station_lon,
                            a.lat AS station_lat,
                            'uic_ref' AS resolution_kind
                        FROM gtfs.stops s
                        JOIN best_match bm   ON bm.stop_id = s.stop_id
                        JOIN osm.features of ON of.feature_id = bm.osm_feature_id
                        JOIN anchors a
                          ON a.uic_ref = of.tags['uic_ref']
                         AND NULLIF(of.tags['uic_ref'], '') IS NOT NULL
                        WHERE s.stop_id NOT IN (
                            SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL
                        )
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY s.stop_id ORDER BY a.feature_id
                        ) = 1
                      ),
                      -- TIER 3: spatial snap to the nearest station
                      -- anchor within _STATION_SNAP_DEG of the stop's
                      -- GTFS coords.
                      tier3 AS (
                        SELECT
                            s.stop_id,
                            a.feature_id  AS station_feature_id,
                            a.station_name,
                            a.lon AS station_lon,
                            a.lat AS station_lat,
                            'spatial' AS resolution_kind
                        FROM gtfs.stops s
                        JOIN anchors a
                          ON ST_DWithin(
                                 ST_Point(s.stop_lon, s.stop_lat),
                                 ST_Point(a.lon, a.lat),
                                 {_STATION_SNAP_DEG}
                             )
                        WHERE s.stop_id NOT IN (
                                SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL
                            )
                          AND s.stop_id NOT IN (
                                SELECT stop_id FROM tier2 WHERE stop_id IS NOT NULL
                            )
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
                      -- TIER 4: self — every stop not resolved above
                      -- becomes its own station.
                      tier4 AS (
                        SELECT
                            s.stop_id,
                            s.stop_id   AS station_feature_id,
                            s.stop_name AS station_name,
                            s.stop_lon  AS station_lon,
                            s.stop_lat  AS station_lat,
                            'self' AS resolution_kind
                        FROM gtfs.stops s
                        WHERE s.stop_id NOT IN (
                                SELECT stop_id FROM tier1 WHERE stop_id IS NOT NULL
                            )
                          AND s.stop_id NOT IN (
                                SELECT stop_id FROM tier2 WHERE stop_id IS NOT NULL
                            )
                          AND s.stop_id NOT IN (
                                SELECT stop_id FROM tier3 WHERE stop_id IS NOT NULL
                            )
                      ),
                      resolved AS (
                        SELECT * FROM tier1
                        UNION ALL SELECT * FROM tier2
                        UNION ALL SELECT * FROM tier3
                        UNION ALL SELECT * FROM tier4
                      ),
                      -- Per-station name consolidation: a station_feature_id
                      -- can be reached by several tiers (a platform via
                      -- gtfs_parent, a nearby orphan stop via spatial),
                      -- and only tier 1 knows the GTFS parent name. Pick
                      -- ONE name per station_feature_id — the non-generic
                      -- one if any member contributed it — so every member
                      -- of a station shows the same, best label.
                      station_name_final AS (
                        SELECT
                            station_feature_id,
                            COALESCE(
                                max(station_name) FILTER (
                                    WHERE lower(trim(station_name))
                                          NOT IN ({_GENERIC_NAME_SET})
                                ),
                                max(station_name)
                            ) AS station_name
                        FROM resolved
                        GROUP BY station_feature_id
                      )
                    SELECT
                        r.stop_id,
                        r.station_feature_id,
                        snf.station_name,
                        r.station_lon,
                        r.station_lat,
                        r.resolution_kind
                    FROM resolved r
                    JOIN station_name_final snf USING (station_feature_id)
                """)
                # resolution_kind histogram — mirrors the match-rate log
                # above. grain MUST hold: one row per GTFS stop_id.
                res = con.sql("""
                    SELECT
                        count(*) FILTER (WHERE resolution_kind='gtfs_parent') AS by_parent,
                        count(*) FILTER (WHERE resolution_kind='uic_ref')     AS by_uic,
                        count(*) FILTER (WHERE resolution_kind='spatial')     AS by_spatial,
                        count(*) FILTER (WHERE resolution_kind='self')        AS by_self,
                        count(*)                AS total_rows,
                        count(DISTINCT stop_id) AS distinct_stop_ids,
                        (SELECT count(*) FROM gtfs.stops) AS total_gtfs_stops
                    FROM transit.station_members
                """).fetchone()
                print(
                    f"[match_gtfs_stops_to_osm] station roll-up: "
                    f"by_parent={res[0]}, by_uic={res[1]}, "
                    f"by_spatial={res[2]}, by_self={res[3]}, "
                    f"total_rows={res[4]}, distinct_stop_ids={res[5]}, "
                    f"total_gtfs_stops={res[6]} "
                    f"(grain OK = {res[4] == res[5] == res[6]})"
                )

                # Fold the station identity back onto matched_stops so
                # downstream queries never need to re-join. station_members
                # is one-row-per-stop_id; matched_stops is
                # one-row-per-(stop_id, match tier) — a LEFT JOIN USING
                # (stop_id) against a table unique on the join key cannot
                # fan out, it only widens each existing row.
                con.sql("""
                    CREATE OR REPLACE TABLE transit.matched_stops AS
                    SELECT
                        m.*,
                        sm.station_feature_id,
                        sm.station_name,
                        sm.station_lon,
                        sm.station_lat,
                        sm.resolution_kind AS station_resolution_kind
                    FROM transit.matched_stops m
                    LEFT JOIN transit.station_members sm USING (stop_id)
                """)

                # Export the joined view as parquet for freestiler
                # ingestion (freestiler can't ATTACH a duckdb file mid-
                # query, so we round-trip through parquet — the same
                # pattern every other freestiler task already uses).
                #
                # `primary_route_type` rolls the GTFS route_type column
                # up to a single integer per stop using the precedence
                # rail > subway > tram > funicular > cable_car/gondola >
                # ferry > bus. The satellite-overlay map's TRANSIT_STYLE
                # uses it to tint each stop dot by mode (Artaria-1911
                # operator-color signature). Values follow the GTFS
                # route_type enum: 0=tram, 1=subway, 2=rail, 3=bus,
                # 4=ferry, 5=cable_car, 6=gondola, 7=funicular.
                transit_parquet = TILES_WORK / "austria-transit-stops.parquet"
                con.sql(f"""
                    COPY (
                        WITH stop_route_types AS (
                            SELECT
                                st.stop_id,
                                CASE
                                    WHEN bool_or(r.route_type = 2) THEN 2  -- rail
                                    WHEN bool_or(r.route_type = 1) THEN 1  -- subway
                                    WHEN bool_or(r.route_type = 0) THEN 0  -- tram
                                    WHEN bool_or(r.route_type = 7) THEN 7  -- funicular
                                    WHEN bool_or(r.route_type IN (5, 6)) THEN 5  -- cable car / gondola
                                    WHEN bool_or(r.route_type = 4) THEN 4  -- ferry
                                    WHEN bool_or(r.route_type = 3) THEN 3  -- bus
                                    ELSE NULL
                                END AS primary_route_type
                            FROM gtfs.stop_times st
                            JOIN gtfs.trips t USING (trip_id)
                            JOIN gtfs.routes r USING (route_id)
                            GROUP BY st.stop_id
                        )
                        SELECT
                            CAST(s.stop_id AS VARCHAR)         AS osm_id,
                            ST_Point(s.stop_lon, s.stop_lat)   AS geometry,
                            'transit'                          AS theme,
                            s.stop_id                          AS gtfs_stop_id,
                            s.stop_name                        AS name,
                            CAST(s.location_type AS INTEGER)   AS location_type,
                            COALESCE(m.match_kind, 'unmatched') AS match_kind,
                            m.match_distance_m,
                            m.osm_feature_id,
                            -- Cast to VARCHAR so freestiler keeps the
                            -- column through the parquet → MVT round-
                            -- trip (numeric-nullable cols are silently
                            -- dropped). The TRANSIT_STYLE filter uses
                            -- ["==", ["get","primary_route_type"], "2"]
                            -- against the resulting string values.
                            CAST(rt.primary_route_type AS VARCHAR)
                                AS primary_route_type,
                            -- Station roll-up identity. Joined from
                            -- transit.station_members (one row per GTFS
                            -- stop_id — covers EVERY stop incl. the
                            -- unmatched, unlike matched_stops). CAST to
                            -- VARCHAR for the same reason as
                            -- primary_route_type: freestiler drops
                            -- numeric-nullable columns on the parquet →
                            -- MVT round-trip. The id is already a string
                            -- ('node/..' | 'way/..' | 'gtfs/..' | a raw
                            -- stop_id) — the CAST makes the contract
                            -- explicit and null-safe.
                            CAST(sm.station_feature_id AS VARCHAR)
                                AS station_feature_id,
                            sm.station_name,
                            -- Exactly ONE row per station_feature_id is
                            -- 'true' — the member point closest to the
                            -- station anchor coords. transit-stops-label
                            -- filters on this so a big station shows ONE
                            -- label, not one per platform. String, not
                            -- bool — bools are dropped on the MVT round-
                            -- trip just like numeric-nullables.
                            CASE WHEN ROW_NUMBER() OVER (
                                PARTITION BY sm.station_feature_id
                                ORDER BY ST_Distance(
                                    ST_Point(s.stop_lon, s.stop_lat),
                                    ST_Point(sm.station_lon, sm.station_lat)
                                ), s.stop_id
                            ) = 1 THEN 'true' ELSE 'false' END
                                AS is_station_label
                        FROM gtfs.stops s
                        LEFT JOIN transit.matched_stops m USING (stop_id)
                        LEFT JOIN stop_route_types rt USING (stop_id)
                        LEFT JOIN transit.station_members sm USING (stop_id)
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
                           osm_feature_id,
                           primary_route_type,
                           station_feature_id,
                           station_name,
                           is_station_label
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
    glyphs_url: str | None = None,
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

    `glyphs_url` is a MapLibre glyphs URL template (e.g.
    `https://example.com/fonts/{fontstack}/{range}.pbf`). When set,
    emits a top-level `glyphs:` key in the style object so symbol
    layers with `text-field` can render. The
    versatiles-glyphs-rs convention (matched by the versatiles
    frontend layer's /fonts/ re-export) is the source-of-truth
    protocol. Default `None` preserves byte-identical output for
    every existing caller — text layers fall back to MapLibre's
    empty-glyphs behaviour (text simply doesn't render).
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
    # Explicit `maxzoom: 12` matches the highest source-zoom that
    # exists in austria-ecovoyage.pmtiles (freestiler bakes z=0..12).
    # MapLibre AUTO-OVERZOOMS for display zoom > 12 — renders the
    # z=12 vector features upscaled at z=13/14/15/.... Without this
    # hint, MapLibre tries to fetch non-existent z=13+ tiles from
    # martin, gets 4xx, and returns 0 features at high zoom.
    source_dict = {
        "type": "vector",
        "url": f"{martin}/{source_name}",
        "maxzoom": 12,
    }
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

    # Optional glyphs URL emitted as a top-level style key so symbol
    # layers with text-field can resolve their SDF font tiles. The
    # versatiles-glyphs-rs URL convention (matched by the versatiles
    # frontend layer's /fonts/ re-export) is the source-of-truth
    # protocol; the caller passes the full template including the
    # {fontstack}/{range}.pbf MapLibre placeholders. Empty string
    # when None — produces byte-identical output to pre-glyphs.
    glyphs_js = f'    glyphs: "{glyphs_url}",\n' if glyphs_url else ""

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
window.map_{js_var} = map_{js_var};
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


@app.cell
def _theme_styles():
    # MapLibre style-layer lists for the 6 maps in this notebook. All
    # 5 style constants reference a shared Artaria-1911-inspired palette
    # + pattern vocabulary + width-ramp block at the TOP of this cell —
    # SINGLE SOURCE OF TRUTH (R3). To adjust the color of any tier
    # across every map in the notebook, edit ONE constant here.
    #
    # Palette is keyed to the Artaria 1911 Eisenbahnkarte legend
    # (k.k. red mainline, k.u. green urban, k.k. Böhm.Nordbahn violet
    # for funicular, etc.) and includes RESERVED slots for non-rail
    # public transport modes (bus/ferry/cable-car) that the GTFS DAG's
    # `primary_route_type` rollup tints transit-stop dots by.
    #
    # NOTE: notebooks/osm-austria.py's _theme_styles cell still uses the
    # pre-2026-05-14 heterogeneous palette and will be brought in line
    # in a separate cutover.

    # === Artaria 1911 palette =========================================
    # Railway tiers (operator hues from the legend)
    ART_KK_RED          = "#c93e3e"  # k.k. ÖStB mainline (Hauptlinie)
    ART_KK_RED_DARK     = "#a02c2c"  # k.k. branch / secondary
    ART_KU_GREEN_DARK   = "#0f3a22"  # subway / U-Bahn (near-black-green)
    ART_KU_GREEN        = "#2d6c4a"  # tram / streetcar (k.u. mid-green)
    ART_KU_GREEN_LIGHT  = "#6ba582"  # light_rail / Stadtbahn / S-Bahn
    ART_VIOLET          = "#5e3a8a"  # funicular / Zahnradbahn
    ART_VIOLET_LIGHT    = "#7a5aa0"  # aerialway / cable-car / gondola

    # Non-rail public transport (RESERVED palette slots — used by
    # TRANSIT_STYLE per-mode dot tinting)
    ART_BUS             = "#b8862b"  # bus stops (mustard ochre)
    ART_FERRY           = "#3a6a9a"  # ferry/ship piers (deep blue-grey)

    # Cycle hierarchy
    ART_TEAL            = "#1a4a6e"  # cycle long-distance routes
    ART_TEAL_DARK       = "#0a2a4a"  # dedicated cycleways

    # Cycle unpaved-surface tones (CYCLE_STYLE only)
    ART_OCHRE           = "#a06030"  # cycle-track / cycle-path
    ART_OCHRE_DARK      = "#7a4820"  # cycle gravel surface

    # Topo / context fills (period-paper tones)
    ART_WATER           = "#a8c8e8"
    ART_WATER_OUTLINE   = "#5a8fb8"
    ART_FOREST          = "#b9d6b3"
    ART_GLACIER         = "#f0f8ff"
    ART_GLACIER_OUTLINE = "#a0c0d0"
    ART_FARMLAND        = "#f0e8c8"
    ART_BUILT           = "#e8d8c8"
    ART_BOUNDARY        = "#7a4a8a"
    ART_PEAK            = "#8b4513"

    # Utility tones
    ART_GREY_LIGHT      = "#9aa0a6"  # construction
    ART_GREY_MID        = "#6c757d"  # service tracks
    ART_GREY_DARK       = "#555555"  # disused / narrow-gauge
    ART_BLACK           = "#1a1a1a"  # text labels
    ART_HALO            = "#ffffff"  # halo / paper-white center stripe

    # === Walkable-way palette (white → grey hierarchy) ================
    # Shade encodes pedestrian-friendliness: near-white = footways /
    # pedestrian zones (best to walk); dark grey = primary/secondary
    # roads (walkable but busy). motorway + trunk are NEVER drawn —
    # pedestrians are legally banned from Autobahn + Schnellstraße.
    # EVERY other highway class is walkable.
    WALK_WHITE    = "#f4f4f1"  # footway/path/pedestrian/living_street/steps/bridleway
    WALK_GREY_LT  = "#d2d2cd"  # track / service / residential
    WALK_GREY_MID = "#a6a6a0"  # tertiary / unclassified
    WALK_GREY_DK  = "#767670"  # primary / secondary
    WALK_CASING   = "#2a2a2a"  # shared dark casing (50% opacity) under the tier

    # === Hiking-path palette (green) ==================================
    # SAC difficulty encoded by shade; the dashed line pattern stays
    # reserved for ONE semantic: "this way is difficult and should be
    # avoided" (SAC T3+ only). trail_visibility is not visually encoded.
    HIKE_GREEN_LT = "#8bc34a"  # SAC T1 (hiking)             — solid
    HIKE_GREEN    = "#558b2f"  # SAC T2 (mountain_hiking)    — solid
    HIKE_GREEN_RT = "#33691e"  # route=hiking long-distance  — solid + halo
    HIKE_GREEN_DK = "#2e5016"  # SAC T3 (demanding_mountain) — DASHED
    HIKE_GREEN_DP = "#16280a"  # SAC T4+ (alpine_*)          — DASH_DOT_DASH

    # === Line-pattern vocabulary ======================================
    # MapLibre v5 line-dasharray is a LITERAL-ARRAY paint property
    # (NOT data-driven), so each pattern variant becomes its own layer
    # where filter coverage requires.
    DASH_LONG           = [4, 2]            # secondary / footway / disused
    DASH_SHORT          = [3, 2]            # construction / gravel track
    DASH_DOT            = [0.1, 2]          # cable-car / low-visibility
    DASH_DOT_DASH       = [1, 2, 4, 2]      # funicular / hard-to-find path
    DASH_FINE           = [0.1, 3]          # narrow-gauge stipple
    DASH_TIGHT          = [1, 1]            # steps

    # === Line-width zoom ramps ========================================
    # Inner array of a MapLibre `interpolate linear zoom` expression.
    # Spread with `*W_*` when used inside a layer dict.
    # Bumped at z=6-8 so the headline tier (mainline rail) reads as the
    # visual focal point at country/regional zoom against busy alpine
    # satellite imagery. Tuned 2026-05-14 iteration 2.
    W_HEADLINE          = [6, 2.6, 10, 3.4, 14, 4.6, 18, 6.0]
    W_HEADLINE_CASING   = [6, 4.6, 10, 5.6, 14, 7.5, 18, 9.5]
    W_HEADLINE_STRIPE   = [14, 1.4, 18, 2.0]
    W_BRANCH            = [8, 1.8, 12, 2.6, 16, 3.6, 18, 4.5]
    W_BRANCH_CASING     = [8, 3.0, 12, 4.2, 16, 5.5, 18, 6.5]
    W_URBAN             = [10, 1.5, 14, 2.6, 18, 3.6]
    W_URBAN_CASING      = [10, 2.9, 14, 4.2, 18, 5.4]
    W_LONGDIST          = [6, 2.0, 10, 2.6, 14, 3.6, 18, 4.5]
    W_LONGDIST_CASING   = [6, 3.4, 10, 4.4, 14, 5.8, 18, 7.0]
    W_TRAIL             = [9, 1.0, 12, 1.8, 16, 3.0, 18, 4.0]
    W_AUX               = [9, 0.8, 14, 1.6]
    W_AUX_SLIM          = [10, 1.0, 14, 2.0]

    # Walkable-street tier width ramps (white → grey hierarchy).
    W_WALK_MAJOR        = [9, 0.6, 12, 1.6, 16, 3.2, 18, 4.6]   # primary / secondary
    W_WALK_MID          = [11, 0.5, 14, 1.3, 18, 2.6]           # tertiary / unclassified
    W_WALK_LOCAL        = [11, 0.4, 14, 1.0, 18, 2.0]           # track / service / residential
    W_WALK_FOOT         = [12, 0.4, 14, 0.9, 18, 1.8]           # footway / path / pedestrian
    W_WALK_CASING       = [9, 1.4, 12, 2.6, 16, 4.6, 18, 6.2]   # shared casing (widest + ~1.5)

    # Per-mode GTFS stop circle-radius ramps
    R_STOP_BIG          = [6, 1.8, 10, 2.8, 14, 4.5, 18, 6.5]  # rail / subway
    R_STOP_MED          = [8, 1.6, 12, 2.4, 14, 3.6, 18, 5.0]  # tram / cable
    R_STOP_SMALL        = [10, 1.4, 12, 2.0, 14, 3.0, 18, 4.2]  # bus / ferry

    # RAILWAY_STYLE — for the ecovoyage 5-theme combined map. Per-mode
    # rail tier split (mainline/branch/subway/light-rail/tram/
    # narrow-gauge/funicular/aerialway each a distinct Artaria color)
    # without the white halos of the satellite-overlay variant (this
    # style draws onto a transparent base, not satellite imagery).
    RAILWAY_STYLE = [
        # === Auxiliary (drawn first; visually subordinate) ===========
        {"id": "rail-disused", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["!=", ["get", "abandoned"], None],
                     ["!=", ["get", "disused"], None],
                     ["!=", ["get", "razed"], None]]],
         "paint": {"line-color": ART_GREY_DARK, "line-width": 0.8,
                   "line-dasharray": DASH_LONG, "line-opacity": 0.6}},
        {"id": "rail-construction", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "construction"], None]],
         "paint": {"line-color": ART_GREY_LIGHT, "line-width": 1.0,
                   "line-dasharray": DASH_SHORT}},
        {"id": "rail-tunnel", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "tunnel"], None],
                    ["==", ["get", "railway"], "rail"]],
         "paint": {"line-color": ART_KK_RED, "line-width": 1.0,
                   "line-dasharray": DASH_LONG, "line-opacity": 0.55}},
        {"id": "rail-service", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "service"], None]],
         "paint": {"line-color": ART_GREY_MID, "line-width": 0.8,
                   "line-dasharray": DASH_SHORT}},
        # === Aerialway / cable-car (cable lift, gondola, chair-lift) =
        {"id": "rail-aerialway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "aerialway"], None]],
         "paint": {"line-color": ART_VIOLET_LIGHT, "line-width": 1.0,
                   "line-dasharray": DASH_DOT, "line-opacity": 0.85}},
        # === Narrow gauge / monorail (period stipple pattern) ========
        {"id": "rail-narrow-gauge", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "railway"],
                     ["literal", ["narrow_gauge", "monorail"]]]],
         "paint": {"line-color": ART_GREY_DARK, "line-width": 1.0,
                   "line-dasharray": DASH_FINE}},
        # === Funicular / cogwheel — k.k. Böhm.Nordbahn-style violet ==
        {"id": "rail-funicular", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "funicular"]],
         "paint": {"line-color": ART_VIOLET, "line-width": 1.2,
                   "line-dasharray": DASH_DOT_DASH}},
        # === Tram — k.u. green =======================================
        {"id": "rail-tram", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "tram"]],
         "paint": {"line-color": ART_KU_GREEN, "line-width": 1.3}},
        # === Light rail — k.u. green lighter (S-Bahn / Stadtbahn) ====
        {"id": "rail-light-rail", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "light_rail"]],
         "paint": {"line-color": ART_KU_GREEN_LIGHT, "line-width": 1.4}},
        # === Subway — k.u. green darker (U-Bahn) =====================
        {"id": "rail-subway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "subway"]],
         "paint": {"line-color": ART_KU_GREEN_DARK, "line-width": 1.4}},
        # === Branch rail — k.k. darker red ===========================
        {"id": "rail-branch", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "usage"], "main"],
                    ["==", ["get", "service"], None]],
         "paint": {"line-color": ART_KK_RED_DARK, "line-width": 1.2}},
        # === Mainline rail — k.k. red, headline of the map ===========
        {"id": "rail-mainline", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "railway"], "rail"],
                    ["==", ["get", "usage"], "main"],
                    ["==", ["get", "service"], None]],
         "paint": {"line-color": ART_KK_RED, "line-width": 1.8}},
        # === Bridge accent (railway only) ============================
        {"id": "rail-bridge", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "bridge"], None],
                    ["==", ["get", "railway"], "rail"]],
         "paint": {"line-color": ART_KK_RED, "line-width": 2.4,
                   "line-opacity": 0.95}},
        # === Stations / halts — black-bordered operator-color dots ===
        {"id": "rail-station", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["any",
                     ["==", ["get", "railway"], "station"],
                     ["==", ["get", "public_transport"], "station"]]],
         "paint": {"circle-color": ART_KK_RED, "circle-radius": 3,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 1.2}},
        {"id": "rail-halt", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "railway"],
                     ["literal", ["halt", "stop"]]]],
         "paint": {"circle-color": ART_KK_RED_DARK, "circle-radius": 2,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 0.8}},
    ]

    # CYCLE_STYLE — Artaria teal hierarchy + ochre track/path context.
    # Roads are pedestrian-deprioritized neutral grey (cyclists pick
    # them up but they're NOT the visual focus).
    CYCLE_STYLE = [
        # Road context (pedestrian-deprioritized; subtle grey)
        {"id": "cycle-road", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["secondary", "tertiary",
                                  "unclassified", "residential"]]]],
         "paint": {"line-color": WALK_GREY_LT, "line-width": 0.6,
                   "line-opacity": 0.6}},
        # Gravel tracks (Forststraße)
        {"id": "cycle-track", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "track"]],
         "paint": {"line-color": ART_OCHRE_DARK, "line-width": 0.8,
                   "line-dasharray": DASH_SHORT}},
        # Paths / footways (cyclable but pedestrian-coded)
        {"id": "cycle-path", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["path", "footway", "bridleway"]]]],
         "paint": {"line-color": ART_OCHRE, "line-width": 0.6,
                   "line-dasharray": DASH_LONG, "line-opacity": 0.7}},
        # Lane markings (in-roadway cycle stripes)
        {"id": "cycle-lane-shared", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["any",
                     ["==", ["get", "cycleway"], "lane"],
                     ["==", ["get", "cycleway:left"], "lane"],
                     ["==", ["get", "cycleway:right"], "lane"],
                     ["==", ["get", "cycleway:both"], "lane"]]],
         "paint": {"line-color": ART_TEAL, "line-width": 1.0,
                   "line-dasharray": DASH_TIGHT}},
        # Dedicated cycleways — Artaria teal-dark
        {"id": "cycle-cycleway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "cycleway"]],
         "paint": {"line-color": ART_TEAL_DARK, "line-width": 1.4}},
        # Bicycle roads (preferred infrastructure)
        {"id": "cycle-bicycle-road", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "bicycle_road"], "yes"]],
         "paint": {"line-color": ART_TEAL_DARK, "line-width": 1.6}},
        # Long-distance cycle routes — Artaria teal with halo casing
        {"id": "cycle-route-casing", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "bicycle"]],
         "paint": {"line-color": ART_HALO, "line-width": 2.6,
                   "line-opacity": 0.85}},
        {"id": "cycle-route", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "bicycle"]],
         "paint": {"line-color": ART_TEAL, "line-width": 1.4}},
        # Bicycle parking marker
        {"id": "cycle-parking", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "amenity"], "bicycle_parking"]],
         "paint": {"circle-color": ART_TEAL, "circle-radius": 2.5,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 0.8}},
    ]

    # TOPO_STYLE — period-paper fills + Artaria-uniform lines. Major
    # roads INTENTIONALLY OMITTED (motorways/trunks/primary cluttered
    # the visual; pedestrians + cyclists in this notebook don't use
    # them). Tertiary/secondary roads kept as faint context.
    TOPO_STYLE = [
        {"id": "topo-water", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["any",
                     ["==", ["get", "natural"], "water"],
                     ["==", ["get", "landuse"], "reservoir"]]],
         "paint": {"fill-color": ART_WATER,
                   "fill-outline-color": ART_WATER_OUTLINE}},
        {"id": "topo-forest", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["any",
                     ["==", ["get", "natural"], "wood"],
                     ["==", ["get", "landuse"], "forest"]]],
         "paint": {"fill-color": ART_FOREST, "fill-opacity": 0.7}},
        {"id": "topo-glacier", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["==", ["get", "natural"], "glacier"]],
         "paint": {"fill-color": ART_GLACIER,
                   "fill-outline-color": ART_GLACIER_OUTLINE}},
        {"id": "topo-farmland", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["in", ["get", "landuse"],
                     ["literal", ["farmland", "farmyard", "orchard",
                                  "vineyard", "meadow"]]]],
         "paint": {"fill-color": ART_FARMLAND, "fill-opacity": 0.5}},
        {"id": "topo-residential", "type": "fill",
         "filter": ["all",
                    ["==", ["geometry-type"], "Polygon"],
                    ["==", ["get", "landuse"], "residential"]],
         "paint": {"fill-color": ART_BUILT, "fill-opacity": 0.6}},
        {"id": "topo-waterway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "waterway"], None]],
         "paint": {"line-color": ART_WATER_OUTLINE, "line-width": 0.6}},
        # === WALKABLE STREET TIER (white → grey hierarchy) ==========
        # Every highway class EXCEPT motorway + trunk is walkable.
        # Shared dark casing first (continuity backstop + contrast),
        # then dark→light shades drawn major→foot so the most
        # pedestrian-friendly ways read on top.
        {"id": "topo-walk-casing", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["primary", "primary_link",
                                  "secondary", "secondary_link",
                                  "tertiary", "tertiary_link",
                                  "unclassified", "residential",
                                  "living_street", "service", "track",
                                  "road", "footway", "path",
                                  "pedestrian", "steps", "bridleway"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_CASING, "line-width": 1.4,
                   "line-opacity": 0.5}},
        # Major roads — primary / secondary (dark grey)
        {"id": "topo-walk-major", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["primary", "primary_link",
                                  "secondary", "secondary_link"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_GREY_DK, "line-width": 1.2}},
        # Mid roads — tertiary / unclassified (mid grey)
        {"id": "topo-walk-mid", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["tertiary", "tertiary_link",
                                  "unclassified"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_GREY_MID, "line-width": 0.9}},
        # Local ways — residential / service / track / road (light grey)
        {"id": "topo-walk-local", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["residential", "living_street",
                                  "service", "track", "road"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_GREY_LT, "line-width": 0.7}},
        # Footways / paths / pedestrian zones (near-white) — excludes
        # sac_scale-tagged + route=hiking ways (those go green in the
        # hiking tier).
        {"id": "topo-walk-foot", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["footway", "path", "pedestrian",
                                  "steps", "bridleway"]]],
                    ["==", ["get", "sac_scale"], None],
                    ["!=", ["get", "route"], "hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_WHITE, "line-width": 0.7}},
        # Rail context (slim, Artaria red-dark)
        {"id": "topo-rail", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "railway"], None]],
         "paint": {"line-color": ART_KK_RED_DARK, "line-width": 0.6}},
        # Administrative boundary
        {"id": "topo-boundary", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "boundary"], "administrative"]],
         "paint": {"line-color": ART_BOUNDARY, "line-width": 0.5,
                   "line-dasharray": DASH_SHORT, "line-opacity": 0.55}},
        # Peak marker
        {"id": "topo-peak", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "peak"]],
         "paint": {"circle-color": ART_PEAK, "circle-radius": 2.5,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 0.8}},
        # Aerialway context (cable-cars / gondolas — Artaria violet)
        {"id": "topo-aerialway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["!=", ["get", "aerialway"], None]],
         "paint": {"line-color": ART_VIOLET_LIGHT, "line-width": 0.5,
                   "line-dasharray": DASH_DOT, "line-opacity": 0.85}},
    ]

    # HIKING_STYLE — Artaria sienna ramp by SAC difficulty + 3
    # visibility-encoding layers (per OSM-wiki "Hiking trails rendering
    # proposal 1" classification, reskinned). Long-distance routes
    # (route=hiking) get a halo casing — the visual headline of the
    # hiking tier.
    HIKING_STYLE = [
        # Generic footways/paths (untyped — sac_scale = null) are
        # WALKABLE WAYS (white), NOT hiking trails. Green is reserved
        # for sac_scale-tagged trails + route=hiking below.
        {"id": "hike-trail-footway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "highway"],
                     ["literal", ["path", "footway"]]],
                    ["==", ["get", "sac_scale"], None]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_WHITE, "line-width": 0.8,
                   "line-opacity": 0.9}},
        # Alpine SAC tier (T4-T6) — deep green, DASH_DOT_DASH warning.
        # Drawn first so easier paths render OVER it at junctions.
        {"id": "hike-trail-alpine", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["alpine_hiking",
                                  "demanding_alpine_hiking",
                                  "difficult_alpine_hiking"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": HIKE_GREEN_DP, "line-width": 1.0,
                   "line-dasharray": DASH_DOT_DASH, "line-opacity": 0.95}},
        # Difficult SAC tier (T3 demanding_mountain_hiking) — dark
        # green, DASH_LONG.
        {"id": "hike-trail-difficult", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "sac_scale"],
                     "demanding_mountain_hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": HIKE_GREEN_DK, "line-width": 1.0,
                   "line-dasharray": DASH_LONG, "line-opacity": 0.95}},
        # Easy SAC tier (T1 + T2) — SOLID green. Shade by sac_scale.
        {"id": "hike-trail-easy", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["hiking", "mountain_hiking"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": [
                       "match", ["get", "sac_scale"],
                       "hiking",          HIKE_GREEN_LT,
                       "mountain_hiking", HIKE_GREEN,
                       HIKE_GREEN_LT,
                   ],
                   "line-width": 1.0, "line-opacity": 0.95}},
        # Long-distance hiking routes — green with white halo casing.
        {"id": "hike-route-casing", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": ART_HALO, "line-width": 2.4,
                   "line-opacity": 0.85}},
        {"id": "hike-route", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "route"], "hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": HIKE_GREEN_RT, "line-width": 1.3}},
        # Bridleway — solid white (walkable way, not a hiking trail).
        {"id": "hike-bridleway", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "bridleway"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_WHITE, "line-width": 0.8}},
        # Steps — solid white with tight dasharray (walkable way;
        # mild texture, not a difficult hiking trail).
        {"id": "hike-steps", "type": "line",
         "filter": ["all",
                    ["==", ["geometry-type"], "LineString"],
                    ["==", ["get", "highway"], "steps"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {"line-color": WALK_WHITE, "line-width": 1.2,
                   "line-dasharray": DASH_TIGHT}},
        # Peak / saddle / spring / hut / viewpoint markers
        {"id": "hike-peak", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "peak"]],
         "paint": {"circle-color": ART_PEAK, "circle-radius": 3,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 1}},
        {"id": "hike-saddle", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "saddle"]],
         "paint": {"circle-color": HIKE_GREEN, "circle-radius": 2.5,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 0.8}},
        {"id": "hike-spring", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "natural"], "spring"]],
         "paint": {"circle-color": ART_WATER_OUTLINE,
                   "circle-radius": 2.5,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 1}},
        {"id": "hike-hut", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["in", ["get", "tourism"],
                     ["literal", ["alpine_hut", "wilderness_hut"]]]],
         "paint": {"circle-color": ART_KK_RED, "circle-radius": 4,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 1.5}},
        {"id": "hike-viewpoint", "type": "circle",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "tourism"], "viewpoint"]],
         "paint": {"circle-color": ART_TEAL, "circle-radius": 3,
                   "circle-stroke-color": ART_HALO,
                   "circle-stroke-width": 1}},
    ]

    # ---- TRANSIT_STYLE — per-mode GTFS-stops overlay -----------------
    # Six per-mode circle layers tinted by `primary_route_type` (GTFS
    # route_type rolled up by the materialize_duckdb DAG task with
    # precedence rail > subway > tram > funicular > cable_car/gondola >
    # ferry > bus). Each layer's filter selects ONE mode; the filters
    # are mutually exclusive, so each stop renders exactly once.
    #
    # Stops with NO matched GTFS route (orphan OSM points) render via a
    # neutral 'unmatched' layer at the bottom of the stack.
    #
    # The text-label symbol layer at the top reads the stop name in
    # ART_BLACK with ART_HALO halo at z>=11 (versatiles-glyphs-rs SDF
    # fonts via the helper's `glyphs_url` kwarg). MapLibre's default
    # text-allow-overlap=false drops crowded labels at city zoom.

    # GTFS route_type discriminator (per spec):
    #   0 = Tram / Streetcar / Light rail
    #   1 = Subway / Metro
    #   2 = Rail
    #   3 = Bus
    #   4 = Ferry
    #   5 = Cable Car
    #   6 = Gondola / Aerial-suspended cable
    #   7 = Funicular
    # Values are STRING ("0".."7") not int, because freestiler drops
    # nullable-int columns through the parquet → MVT pipeline.
    _RT_FILTER = lambda rt: ["==",
                              ["coalesce",
                               ["get", "primary_route_type"], ""],
                              str(rt)]

    TRANSIT_STYLE = [
        # Unmatched / null primary_route_type (orphan or pre-rollup).
        # Empty-string match catches both null and DuckDB's stringified
        # null ("None" or "" depending on the DuckDB version).
        {"id": "transit-stops-unmatched", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["any",
                     ["==", ["coalesce",
                            ["get", "primary_route_type"], ""], ""],
                     ["==", ["get", "primary_route_type"], "None"]]],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_SMALL],
            "circle-color": ART_HALO,
            "circle-stroke-color": ART_GREY_DARK,
            "circle-stroke-width": 0.9,
            "circle-opacity": 0.8,
         }},
        # Bus stops (route_type=3) — Artaria mustard
        {"id": "transit-stops-bus", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(3)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_SMALL],
            "circle-color": ART_BUS,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.0,
            "circle-opacity": 0.95,
         }},
        # Ferry piers (route_type=4)
        {"id": "transit-stops-ferry", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(4)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_MED],
            "circle-color": ART_FERRY,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.0,
            "circle-opacity": 0.95,
         }},
        # Cable car / gondola (route_type=5)
        {"id": "transit-stops-cable", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(5)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_MED],
            "circle-color": ART_VIOLET_LIGHT,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.0,
            "circle-opacity": 0.95,
         }},
        # Funicular (route_type=7) — Artaria violet
        {"id": "transit-stops-funicular", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(7)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_MED],
            "circle-color": ART_VIOLET,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.2,
            "circle-opacity": 0.95,
         }},
        # Tram stops (route_type=0) — Artaria k.u. green
        {"id": "transit-stops-tram", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(0)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_MED],
            "circle-color": ART_KU_GREEN,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.2,
            "circle-opacity": 0.95,
         }},
        # Subway / U-Bahn (route_type=1) — Artaria k.u. green dark
        {"id": "transit-stops-subway", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(1)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_BIG],
            "circle-color": ART_KU_GREEN_DARK,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.2,
            "circle-opacity": 0.95,
         }},
        # Rail stations (route_type=2) — Artaria k.k. red, biggest dot
        {"id": "transit-stops-train", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    _RT_FILTER(2)],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              *R_STOP_BIG],
            "circle-color": ART_KK_RED,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.4,
            "circle-opacity": 0.95,
         }},
        # Parent-station names (one symbol layer, all modes). Only the
        # single representative member per station (is_station_label) is
        # labelled, with the parent-station name — so a big station shows
        # ONE label, not one per platform. Every platform still renders
        # its own dot via the per-mode circle layers above.
        {"id": "transit-stops-label", "type": "symbol",
         "source": "transit-src", "source-layer": "austria-transit",
         "minzoom": 11,
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "is_station_label"], "true"]],
         "layout": {
            "text-field": ["get", "station_name"],
            "text-font": ["Noto Sans Regular"],
            "text-size": [
                "interpolate", ["linear"], ["zoom"],
                11, 10,
                14, 12,
                18, 14,
            ],
            "text-anchor": "top",
            "text-offset": [0, 0.6],
            "text-padding": 2,
         },
         "paint": {
            "text-color": ART_BLACK,
            "text-halo-color": ART_HALO,
            "text-halo-width": 1.5,
            "text-halo-blur": 0.5,
         }},
    ]

    # ---- SATELLITE_OVERLAY_STYLE — zoom-banded transport overlay -----
    # Dedicated style for the satellite-overlay map cell. Aesthetic
    # signature: Artaria 1911 Eisenbahnkarte von Österreich-Ungarn.
    # See the palette constants at the top of this cell for the full
    # operator-color table.
    #
    # Reads from the `austria-ecovoyage` martin source (single
    # UNION-ALL-BY-NAME pmtiles with a `theme` discriminator: railway,
    # cycle, hiking, topo). Every layer's filter anchors `theme` first.
    #
    # DRAW ORDER (top of list = drawn FIRST = visually UNDERNEATH):
    #   1.  Tertiary/minor paved roads (pedestrian-deprioritized — faint)
    #   2.  Pedestrian zones (pale ochre)
    #   3.  Gravel tracks (Forststraße — earthy brown dashed)
    #   4.  Footways (ochre dashed)
    #   5.  SAC trails — 3 visibility layers (sienna ramp)
    #   6.  Hiking long-distance routes (with halo)
    #   7.  Dedicated cycleways
    #   8.  Cycle long-distance routes (with halo)
    #   9.  Rail aux: disused / construction / tunnel / service
    #   10. Aerialway / cable-car (violet light, dotted)
    #   11. Narrow-gauge (grey-dark, fine stipple)
    #   12. Funicular (violet, dot-dash) — with halo
    #   13. Tram (k.u. green) — with halo
    #   14. Light rail / S-Bahn (k.u. green light) — with halo
    #   15. Subway / U-Bahn (k.u. green dark) — with halo
    #   16. Branch rail (k.k. red dark) — with halo
    #   17. Mainline rail (k.k. red) — with halo + z14+ centre stripe
    #   18. Rail station + halt circles (red dots)
    #   19. Rail station name labels (z12+ symbols)
    #
    # GTFS-stop layers (mode-tinted from TRANSIT_STYLE) get APPENDED
    # via the helper's `extra_layers` parameter, so they ride at the
    # very top of the visual stack.
    #
    # White halo casings ride underneath every primary line tier so the
    # operator color reads cleanly against the busy satellite imagery.
    SATELLITE_OVERLAY_STYLE = [
        # ============================================================
        # === WALKABLE STREET TIER (white → grey hierarchy)          ===
        # ============================================================
        # EVERY highway class except motorway + trunk is walkable.
        # Shade encodes pedestrian-friendliness: near-white footways →
        # dark-grey primary/secondary roads. A single shared dark
        # casing (sat-walk-casing) draws first: it is BOTH the
        # continuity backstop (no gaps where adjacent OSM segments
        # change highway class) AND the satellite-contrast edge (grey
        # lines need a dark rim to read against the imagery). Then the
        # colored shades draw major→foot so the most pedestrian-
        # friendly ways render on top.

        # Shared dark casing — all walkable highway classes, all
        # zooms ≥9. Continuity backstop + contrast edge.
        {"id": "sat-walk-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 9,
         "filter": ["all",
                    ["in", ["get", "theme"],
                     ["literal", ["hiking", "topo"]]],
                    ["in", ["get", "highway"],
                     ["literal", ["primary", "primary_link",
                                  "secondary", "secondary_link",
                                  "tertiary", "tertiary_link",
                                  "unclassified", "residential",
                                  "living_street", "service", "track",
                                  "road", "footway", "path",
                                  "pedestrian", "steps", "bridleway"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": WALK_CASING,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_WALK_CASING],
            "line-opacity": 0.5,
         }},

        # Major roads — primary / secondary (dark grey). NOT motorway
        # or trunk: pedestrians are legally banned from those.
        {"id": "sat-walk-major", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 9,
         "filter": ["all",
                    ["==", ["get", "theme"], "topo"],
                    ["in", ["get", "highway"],
                     ["literal", ["primary", "primary_link",
                                  "secondary", "secondary_link"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": WALK_GREY_DK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_WALK_MAJOR],
         }},

        # Mid roads — tertiary / unclassified (mid grey)
        {"id": "sat-walk-mid", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 11,
         "filter": ["all",
                    ["==", ["get", "theme"], "topo"],
                    ["in", ["get", "highway"],
                     ["literal", ["tertiary", "tertiary_link",
                                  "unclassified"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": WALK_GREY_MID,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_WALK_MID],
         }},

        # Local ways — residential / living_street / service / track /
        # road (light grey)
        {"id": "sat-walk-local", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 11,
         "filter": ["all",
                    ["in", ["get", "theme"],
                     ["literal", ["hiking", "topo"]]],
                    ["in", ["get", "highway"],
                     ["literal", ["residential", "living_street",
                                  "service", "track", "road"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": WALK_GREY_LT,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_WALK_LOCAL],
         }},

        # Footways / paths / pedestrian zones / steps / bridleway —
        # near-white (most pedestrian-friendly). Excludes sac_scale-
        # tagged + route=hiking ways (those render green in the
        # hiking tier below).
        {"id": "sat-walk-foot", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 12,
         "filter": ["all",
                    ["in", ["get", "theme"],
                     ["literal", ["hiking", "topo"]]],
                    ["in", ["get", "highway"],
                     ["literal", ["footway", "path", "pedestrian",
                                  "steps", "bridleway"]]],
                    ["==", ["get", "sac_scale"], None],
                    ["!=", ["get", "route"], "hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": WALK_WHITE,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_WALK_FOOT],
         }},

        # ============================================================
        # === HIKING TIER: SAC trails (green scale by difficulty) +   ==
        # ===              long-distance hiking routes                ==
        # ============================================================
        # SAC dasharray semantics: SOLID = walkable (T1, T2);
        # DASHED = "difficult, should be avoided" (T3+ exclusively).
        # Hiking infrastructure renders GREEN — distinct from the
        # white/grey walkable streets.

        # Alpine SAC tier (T4-T6) — DASH_DOT_DASH warning pattern,
        # deepest green. Drawn FIRST so easier paths render OVER it
        # at junctions where a path transitions T3→T4.
        {"id": "sat-hike-trail-alpine", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "hiking"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["alpine_hiking",
                                  "demanding_alpine_hiking",
                                  "difficult_alpine_hiking"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": HIKE_GREEN_DP,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_TRAIL],
            "line-dasharray": DASH_DOT_DASH,
            "line-opacity": 0.95,
         }},

        # Difficult SAC tier (T3 demanding_mountain_hiking) —
        # DASH_LONG pattern, dark green.
        {"id": "sat-hike-trail-difficult", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "hiking"],
                    ["==", ["get", "sac_scale"],
                     "demanding_mountain_hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": HIKE_GREEN_DK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_TRAIL],
            "line-dasharray": DASH_LONG,
            "line-opacity": 0.95,
         }},

        # Easy SAC tier (T1 hiking + T2 mountain_hiking) — SOLID green.
        # Color shade encodes T1 vs T2 via inline match.
        {"id": "sat-hike-trail-easy", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "hiking"],
                    ["in", ["get", "sac_scale"],
                     ["literal", ["hiking", "mountain_hiking"]]]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": [
                "match", ["get", "sac_scale"],
                "hiking",          HIKE_GREEN_LT,
                "mountain_hiking", HIKE_GREEN,
                HIKE_GREEN_LT,
            ],
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_TRAIL],
            "line-opacity": 0.95,
         }},

        # Hiking long-distance routes — deep blue with white halo.
        # Visual headline of the hiking tier; visible from country
        # zoom upward.
        {"id": "sat-hike-route-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["get", "theme"], "hiking"],
                    ["==", ["get", "route"], "hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_LONGDIST_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-hike-route", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["get", "theme"], "hiking"],
                    ["==", ["get", "route"], "hiking"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": HIKE_GREEN_RT,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_LONGDIST],
         }},

        # ============================================================
        # === CYCLE TIER: dedicated cycleways + long-distance routes ==
        # ============================================================

        # Dedicated cycleways (highway=cycleway) — Artaria teal-dark
        {"id": "sat-cycleway", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 11,
         "filter": ["all",
                    ["==", ["get", "theme"], "cycle"],
                    ["==", ["get", "highway"], "cycleway"]],
         "paint": {
            "line-color": ART_TEAL_DARK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           11, 1.2, 14, 2.0, 18, 3.0],
            "line-opacity": 0.9,
         }},

        # Long-distance cycle routes (route=bicycle, any network) —
        # teal-ink with halo
        {"id": "sat-cycle-route-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["get", "theme"], "cycle"],
                    ["==", ["get", "route"], "bicycle"]],
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_LONGDIST_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-cycle-route", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["get", "theme"], "cycle"],
                    ["==", ["get", "route"], "bicycle"]],
         "paint": {
            "line-color": ART_TEAL,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_LONGDIST],
         }},

        # ============================================================
        # === RAILWAY AUXILIARY (disused / construction / tunnel /  ===
        # === service — drawn before primary tiers)                  ===
        # ============================================================

        # Disused / abandoned / razed rail — grey-dark dashed
        {"id": "sat-rail-disused", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["any",
                     ["!=", ["get", "abandoned"], None],
                     ["!=", ["get", "disused"], None],
                     ["!=", ["get", "razed"], None]]],
         "paint": {
            "line-color": ART_GREY_DARK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_AUX_SLIM],
            "line-dasharray": DASH_LONG,
            "line-opacity": 0.6,
         }},

        # Rail construction — gray dashed
        {"id": "sat-rail-construction", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 9,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["!=", ["get", "construction"], None]],
         "paint": {
            "line-color": ART_GREY_LIGHT,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_AUX],
            "line-dasharray": DASH_SHORT,
            "line-opacity": 0.85,
         }},

        # Rail tunnels — dashed Artaria red, partially transparent
        {"id": "sat-rail-tunnel", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["!=", ["get", "tunnel"], None]],
         "paint": {
            "line-color": ART_KK_RED,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_AUX_SLIM],
            "line-dasharray": DASH_LONG,
            "line-opacity": 0.55,
         }},

        # Rail service tracks (sidings) — grey dashed
        {"id": "sat-rail-service", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 13,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "service"], None]],
         "paint": {
            "line-color": ART_GREY_MID,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           13, 0.5, 16, 1.2],
            "line-dasharray": DASH_SHORT,
            "line-opacity": 0.8,
         }},

        # ============================================================
        # === NON-RAIL & SPECIAL RAILWAYS                            ===
        # ============================================================

        # Aerialway / cable-car / gondola — Artaria violet-light dot.
        # Aerialway features are tagged theme=topo (NOT railway) in the
        # unified austria-ecovoyage pmtiles bake; the filter matches
        # that. RCA 2026-05-14.
        {"id": "sat-aerialway", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "topo"],
                    ["!=", ["get", "aerialway"], None]],
         "paint": {
            "line-color": ART_VIOLET_LIGHT,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           10, 1.2, 14, 2.2, 18, 3.0],
            "line-dasharray": DASH_DOT,
            "line-opacity": 0.95,
         }},

        # Narrow-gauge / monorail — grey-dark fine stipple
        {"id": "sat-rail-narrow-gauge", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["in", ["get", "railway"],
                     ["literal", ["narrow_gauge", "monorail"]]]],
         "paint": {
            "line-color": ART_GREY_DARK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN],
            "line-dasharray": DASH_FINE,
            "line-opacity": 0.9,
         }},

        # Funicular / Zahnradbahn — Artaria violet dot-dash, halo
        {"id": "sat-rail-funicular-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "funicular"]],
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-rail-funicular", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "funicular"]],
         "paint": {
            "line-color": ART_VIOLET,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN],
            "line-dasharray": DASH_DOT_DASH,
         }},

        # ============================================================
        # === URBAN RAIL TIERS (tram / light-rail / subway)          ===
        # Each gets its own halo casing + distinct k.u. green hue.
        # ============================================================

        # Tram — Artaria k.u. green
        {"id": "sat-rail-tram-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "tram"]],
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-rail-tram", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "tram"]],
         "paint": {
            "line-color": ART_KU_GREEN,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN],
         }},

        # Light rail / S-Bahn — Artaria k.u. green light
        {"id": "sat-rail-light-rail-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "light_rail"]],
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-rail-light-rail", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "light_rail"]],
         "paint": {
            "line-color": ART_KU_GREEN_LIGHT,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN],
         }},

        # Subway / U-Bahn — Artaria k.u. green dark
        {"id": "sat-rail-subway-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "subway"]],
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-rail-subway", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "subway"]],
         "paint": {
            "line-color": ART_KU_GREEN_DARK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_URBAN],
         }},

        # ============================================================
        # === LOW-ZOOM RAIL CONTINUITY BACKSTOP (z=6-9)              ===
        # ============================================================
        # At country/regional zoom the per-tier mainline/branch split
        # causes visible discontinuities because individual OSM way
        # segments along a single mainline carry mixed usage tags
        # (main / branch / null / service). This base layer absorbs
        # ALL railway=rail in a SINGLE rendering for z=6-9 — one
        # continuous bold k.k. red line per corridor. At z>=10 the
        # per-tier mainline/branch/service layers take over.

        {"id": "sat-rail-base-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 6, "maxzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           6, 4.6, 10, 5.6],
            "line-opacity": 0.95,
         }},
        {"id": "sat-rail-base", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 6, "maxzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_KK_RED,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           6, 2.6, 10, 3.4],
         }},

        # ============================================================
        # === HEAVY RAIL: branch + mainline (Artaria k.k. red, z>=10)===
        # ============================================================

        # Branch rail (rail without usage=main, no service tag) —
        # k.k. red dark with halo. minzoom=10 — at lower zooms the
        # rail-base layer above takes over.
        {"id": "sat-rail-branch-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "usage"], "main"],
                    ["==", ["get", "service"], None]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_BRANCH_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-rail-branch", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"],
                    ["!=", ["get", "usage"], "main"],
                    ["==", ["get", "service"], None]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_KK_RED_DARK,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_BRANCH],
         }},

        # Mainline rail (rail usage=main) — Artaria k.k. red with halo.
        # The visual headline; widest line. minzoom=10 (rail-base
        # above handles z=6-9 continuously).
        {"id": "sat-rail-mainline-casing", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"],
                    ["==", ["get", "usage"], "main"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_HEADLINE_CASING],
            "line-opacity": 0.95,
         }},
        {"id": "sat-rail-mainline", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"],
                    ["==", ["get", "usage"], "main"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_KK_RED,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_HEADLINE],
         }},

        # Mainline rail double-track center stripe — z14+, thin white
        # over the red core. Renders as
        # casing | red | stripe | red | casing — period-printed-map
        # railway signature; only visible at city zoom where the line
        # is wide enough.
        {"id": "sat-rail-mainline-stripe", "type": "line",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 14,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "rail"],
                    ["==", ["get", "usage"], "main"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_HEADLINE_STRIPE],
         }},

        # ============================================================
        # === STATION + HALT CIRCLES (Artaria operator-color dots)   ===
        # ============================================================

        # Rail station — k.k. red dot, halo'd
        {"id": "sat-rail-station", "type": "circle",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 10,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "station"]],
         "paint": {
            "circle-color": ART_KK_RED,
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              10, 3, 14, 5, 18, 6.5],
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.5,
            "circle-opacity": 0.95,
         }},

        # Rail halt / stop — k.k. red dark, smaller
        {"id": "sat-rail-halt", "type": "circle",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 11,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["in", ["get", "railway"],
                     ["literal", ["halt", "stop"]]]],
         "paint": {
            "circle-color": ART_KK_RED_DARK,
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              11, 2, 14, 3.5, 18, 5],
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.0,
            "circle-opacity": 0.9,
         }},

        # Station name labels — black text with halo, z12+
        {"id": "sat-rail-station-label", "type": "symbol",
         "source": "src", "source-layer": "austria-ecovoyage",
         "minzoom": 12,
         "filter": ["all",
                    ["==", ["get", "theme"], "railway"],
                    ["==", ["get", "railway"], "station"]],
         "layout": {
            "text-field": ["get", "name"],
            "text-font": ["Noto Sans Regular"],
            "text-size": [
                "interpolate", ["linear"], ["zoom"],
                12, 11, 16, 13, 18, 15,
            ],
            "text-anchor": "top",
            "text-offset": [0, 0.7],
            "text-padding": 3,
         },
         "paint": {
            "text-color": ART_BLACK,
            "text-halo-color": ART_HALO,
            "text-halo-width": 1.5,
            "text-halo-blur": 0.5,
         }},
    ]
    return (
        CYCLE_STYLE,
        HIKING_STYLE,
        RAILWAY_STYLE,
        SATELLITE_OVERLAY_STYLE,
        TOPO_STYLE,
        TRANSIT_STYLE,
    )


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
    # The query that's IMPOSSIBLE without unification: every parent
    # STATION (platform-granularity GTFS stops rolled up via the 4-tier
    # transit.station_members chain: gtfs_parent → uic_ref → spatial →
    # self) that has GTFS service, ranked by distinct GTFS routes
    # served. station_name / station_feature_id / station_resolution_kind
    # now ride on transit.matched_stops, so this no longer joins
    # osm.features at all — just matched_stops ↔ stop_times ↔ trips.
    # Wien Hauptbahnhof and every other multi-platform station now
    # appears, with its ~44 child platforms rolled into one row (the old
    # `WHERE tags['railway']='station'` filter dropped them: the GTFS
    # match lands on platform features, never the railway=station node).
    top_stations = con.sql("""
        SELECT
            m.station_feature_id,
            min(m.station_name)            AS station_name,
            min(m.station_resolution_kind) AS resolution_kind,
            count(DISTINCT t.route_id)     AS routes_serving,
            count(DISTINCT st.trip_id)     AS trips_serving,
            count(DISTINCT m.stop_id)      AS gtfs_stops_rolled_up
        FROM transit.matched_stops m
        JOIN gtfs.stop_times st USING (stop_id)
        JOIN gtfs.trips t       USING (trip_id)
        WHERE m.station_feature_id IS NOT NULL
        GROUP BY m.station_feature_id
        ORDER BY routes_serving DESC, trips_serving DESC
        LIMIT 25
    """).pl()

    con.close()
    mo.vstack([
        mo.md("**Unified inventory + GTFS↔OSM match-rate** "
              "(stops via wiki-compliant tier chain: gtfs:stop_id → "
              "ref:IFOPT → spatial last-resort)"),
        unified_summary,
        mo.md("**Top 25 parent stations by GTFS service** "
              "(platform-granularity GTFS stops rolled up to their "
              "parent station via the 4-tier chain: gtfs_parent → "
              "uic_ref → spatial → self — cross-dataset query, "
              "impossible without unified DuckDB)"),
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
    Path,
    SATELLITE_OVERLAY_STYLE,
    TRANSIT_STYLE,
    dag_run_states,
    martin,
    mo,
    versatiles_assets,
):
    # Satellite-overlay map — versatiles satellite imagery as the
    # background; transport overlay drawn with a dedicated zoom-banded
    # style (`SATELLITE_OVERLAY_STYLE`) tuned for the satellite
    # background. The aesthetic is inspired by Artaria's 1911
    # Eisenbahnkarte von Österreich-Ungarn — railways are the visual
    # headline (k.k. Staatsbahn red mainline + double-track stripe at
    # city zoom, k.u. green for urban transit), cycle network is
    # secondary (deep teal-ink with halo on long-distance routes),
    # hiking network is also secondary (sienna with halo on
    # long-distance routes), generic footpaths are tertiary.
    #
    # GTFS stops overlay (`TRANSIT_STYLE`) uses uniform white-fill /
    # dark-stroke circles at every zoom + a Noto Sans text label per
    # stop at z11+ (collision-avoided by MapLibre's default). The
    # match_kind discriminator is no longer encoded into colour —
    # it remains queryable in the unified-analysis cell above.
    #
    # Text labels need an SDF glyph source: wired here via the helper's
    # `glyphs_url` kwarg, pointing at the versatiles-frontend layer's
    # /fonts/ re-export (which serves versatiles-fonts in the
    # versatiles-glyphs-rs URL convention).
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
            style_layers=SATELLITE_OVERLAY_STYLE,
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
            glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
        ),
        height="500px",
    )
    return


@app.cell
def _(dag_run_states, mo):
    # Tag-distribution diagnostics — what OSM tag values actually
    # exist in the data the satellite-overlay style filters against.
    #
    # Surfaces the empirical histograms that informed (or revealed
    # gaps in) the filter design. Re-run after any OSM data refresh
    # — e.g. if Austria's cycle network tagging changes upstream,
    # the cycle histogram here surfaces the new values BEFORE the
    # satellite-overlay map's `network=` filter silently matches
    # nothing.
    #
    # Imported from the standalone .duckprobe.py script that drove
    # the RCA of the 2026-05-13 satellite-overlay rendering issue
    # (cycle/icn/ncn matched 0 features because Austrian OSM uses
    # mostly highway=cycleway / cycleway=* tagging, not route-relation
    # tagging).
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    # Aliased + underscore-prefixed so the duckdb import and the
    # connection stay cell-private (marimo flags top-level names
    # that collide across cells; the unified-analysis cell above
    # already imports `duckdb`/`con`).
    import duckdb as _duckdb
    _con = _duckdb.connect(
        "/workspace/duckdb/austria.duckdb",
        read_only=True,
    )

    # Cycle-theme route + network distribution — informs the
    # sat-cycle-route filter on the satellite-overlay map.
    _cycle_network_dist = _con.sql("""
        SELECT
            tags['route']   AS route,
            tags['network'] AS network,
            count(*)        AS n
        FROM osm.features
        WHERE tags['route'] = 'bicycle'
           OR tags['cycleway'] IS NOT NULL
           OR tags['highway'] = 'cycleway'
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 30
    """).pl()

    # Railway-type distribution — informs the sat-rail-* family of
    # filters (mainline vs branch vs urban via railway= + usage=).
    _railway_type_dist = _con.sql("""
        SELECT
            tags['railway'] AS railway,
            count(*)        AS n
        FROM osm.features
        WHERE tags['railway'] IS NOT NULL
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 30
    """).pl()

    # Rail usage distribution — informs sat-rail-mainline (usage=main)
    # vs sat-rail-branch (usage != main, no service).
    _rail_usage_dist = _con.sql("""
        SELECT
            tags['usage'] AS usage,
            count(*)      AS n
        FROM osm.features
        WHERE tags['railway'] = 'rail'
        GROUP BY 1
        ORDER BY n DESC
    """).pl()

    # SAC scale × trail_visibility distribution — informs the three
    # sat-hike-trail-{solid,dashed,dotted} layers that encode the
    # OSM-wiki "Hiking trails rendering proposal 1" matrix.
    _sac_visibility_dist = _con.sql("""
        SELECT
            tags['sac_scale']        AS sac_scale,
            tags['trail_visibility'] AS trail_visibility,
            count(*)                 AS n
        FROM osm.features
        WHERE tags['sac_scale'] IS NOT NULL
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 50
    """).pl()

    _con.close()
    mo.vstack([
        mo.md("**Satellite-overlay style filter diagnostics — "
              "what tag values actually exist in the Austria data?**"),
        mo.md("Cycle infrastructure — `route` × `network` distribution. "
              "Drives the `sat-cycle-route` filter (any `route=bicycle`) "
              "+ `sat-cycleway` (highway=cycleway):"),
        _cycle_network_dist,
        mo.md("Railway `railway=` type distribution — drives the "
              "`sat-rail-mainline` / `sat-rail-branch` / `sat-rail-urban` "
              "/ `sat-rail-tunnel` / `sat-rail-service` family:"),
        _railway_type_dist,
        mo.md("Rail `usage=` distribution — distinguishes mainline "
              "(`usage=main`) from branch (everything else):"),
        _rail_usage_dist,
        mo.md("`sac_scale` × `trail_visibility` distribution — drives "
              "the three `sat-hike-trail-{solid,dashed,dotted}` layers "
              "encoding the OSM-wiki "
              "[Hiking trails rendering proposal 1](https://wiki.openstreetmap.org/wiki/File:Hiking_trails_rendering_proposal_1.png) "
              "matrix (colour = difficulty, pattern = visibility):"),
        _sac_visibility_dist,
    ])
    return


if __name__ == "__main__":
    app.run()
