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

    Maps below — six viewpoints on the same unified dataset:

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
    - **Chronomap** — a
      [chronotrains](https://github.com/benjamintd/chronotrains)-style
      per-station reachability overlay: click a marked station and
      concentric 1–12 h travel-time isochrone bands radiate from it.
      Computed from the REAL GTFS timetable (multi-hop journeys, actual
      ride + transfer waiting times) by a time-dependent Connection
      Scan Algorithm — a smart-multipath, frontier-relaxation,
      GPU-accelerated polars loop in the GTFS DAG's
      `compute_chrono_isochrones` task — from each of the top 25 hubs
      to EVERY other station, baked to the `austria-chrono` PMTiles
      archive.
    - **Fastest connections** — the fastest journey between the top 25
      hubs, drawn as the actual route through every intermediate
      station. Click a hub marker → its fastest connection to each
      other hub lights up (coloured by travel time, hover for time +
      transfers). Reconstructed by backtracking the chronomap CSA's
      predecessor chain in the `compute_fastest_connections` task,
      baked to the `austria-fastlink` PMTiles archive.
    - **Route builder** — left-click a station to start a route,
      shift+click to add stops; the journey through your picked
      stations is drawn with the cumulative leg-by-leg itinerary. Each
      segment is hub-decomposed (`station → hub → … → hub → station`)
      over the `compute_route_network` task's `hub→station` /
      `station→hub` / `hub→hub`-profile network, or routed as a direct
      sub-path of a single journey when one already connects the pair.
      Baked to the `austria-routehub` PMTiles archive.

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


        # === Chronomap (chronotrains-style isochrones) tunables ==========
        # The compute_chrono_isochrones task derives per-origin travel-time
        # isochrones from the REAL Austria railway GTFS timetable — actual
        # scheduled ride times + actual transfer waiting times — via a
        # time-dependent Connection Scan Algorithm earliest-arrival
        # computation, expressed as a GPU-accelerated polars relaxation.
        # chronotrains' 9 km/h short-hop and flat 20-min interchange
        # approximations are deliberately NOT used.
        #
        # The relaxation is a SMART MULTIPATH algorithm: all
        # CHRONO_ORIGIN_COUNT origins are routed simultaneously in one
        # vectorised pass, and each iteration relaxes ONLY from the
        # frontier — the labels that improved last round — instead of
        # re-scanning the whole state. The connection table is joined
        # against the small per-iteration delta, not the full arrival
        # set, which is what keeps the 12 h horizon (origins → ALL
        # reachable stations) cheap. Every knob lives here.
        CHRONO_ORIGIN_COUNT = 25             # top-N stations by hub_rank to seed
        CHRONO_DEPART_S = 8 * 3600           # reference departure time-of-day (08:00)
        CHRONO_DEFAULT_TRANSFER_S = 0        # transfer seconds when transfers.txt is silent
        CHRONO_MAX_LEGS = 40                 # CSA fixpoint iteration cap (safety bound)
        CHRONO_BANDS_H = list(range(1, 13))  # cumulative isochrone bands: every hour to 12 h
        CHRONO_HULL_BUFFER_DEG = 0.03        # ~3 km smoothing buffer on each band hull
        # Hub→hub departure-time profile grid (route builder). Only 25
        # hubs, so a full profile across the service day stays small +
        # tile-able — lets a station→hub leg's arrival be matched to a
        # hub→hub leg's departure.
        CHRONO_HUB_PROFILE_TIMES = list(range(5 * 3600, 22 * 3600, 1800))

        # Sentinel `via_trip` on CSA seed rows ("first boarding from the
        # journey origin charges no transfer time"; also the
        # backtrack-terminates marker). Module-level so _run_csa and
        # _reconstruct_journeys share it.
        _NO_TRIP = 2 ** 32 - 1


        def _run_csa(conns_df, seed_df, horizon_sec, transfer_i,
                     default_transfer_s, max_legs, collect_fn, label):
            """Frontier-relaxation CSA earliest-arrival fixpoint.

            `seed_df` carries (origin, st, sec, via_trip, board_st) with
            via_trip = _NO_TRIP on every seed row — that marks "this is a
            journey origin, the first boarding charges no transfer time".
            `origin` need not equal `st` (the profiled run seeds each
            (hub, grid-time) as a distinct composite `origin` id), so the
            no-transfer check keys on the sentinel, not on origin == st.
            Returns the converged `arr` (origin, st, sec, via_trip,
            board_st). Vectorised over `origin`; GPU via `collect_fn`.
            """
            import polars as pl

            arr = seed_df
            arr_delta = seed_df                       # the whole seed is "new"
            reached = pl.DataFrame(schema={
                "origin": pl.UInt32, "trip": pl.UInt32,
                "board_dep": pl.Int64, "board_st": pl.UInt32,
            })
            reached_delta = reached
            _legs = 0
            for _legs in range(1, max_legs + 1):
                # Transfer rule — relaxed ONLY from the arrival frontier:
                # board a connection that departs at/after you can be
                # ready at its from-station. via_trip == _NO_TRIP marks a
                # seed (the journey origin) → no transfer charged.
                trans = collect_fn(
                    conns_df.lazy()
                    .join(arr_delta.lazy(), left_on="from_st",
                          right_on="st")
                    .join(transfer_i.lazy(), on="from_st", how="left")
                    .with_columns(
                        pl.when(pl.col("via_trip") == _NO_TRIP)
                        .then(pl.lit(0, dtype=pl.Int64))
                        .otherwise(pl.col("transfer_s").fill_null(
                            default_transfer_s))
                        .alias("eff_transfer")
                    )
                    .filter(pl.col("sec") + pl.col("eff_transfer")
                            <= pl.col("dep"))
                    .select("origin", "trip", "from_st",
                            "to_st", "dep", "arr_c")
                )
                # Ride rule — relaxed ONLY from the boarding frontier: a
                # whole reached trip's remaining run expands in ONE
                # iteration (leg count = transfers + 1, not stop count).
                ride = collect_fn(
                    conns_df.lazy()
                    .join(reached_delta.lazy(), on="trip")
                    .filter(pl.col("dep") >= pl.col("board_dep"))
                    .select("origin", "trip", "to_st", "arr_c", "board_st")
                )
                # Candidate arrivals (both rules) → best per (origin, st)
                # by ARGMIN-keep-row (min sec, via_trip tie-break) so the
                # winning row's predecessor pointers survive; arr_delta =
                # rows that STRICTLY beat the running label (or are new).
                arr_delta = collect_fn(
                    pl.concat([
                        trans.lazy().select(
                            "origin", pl.col("to_st").alias("st"),
                            pl.col("arr_c").alias("sec"),
                            pl.col("trip").alias("via_trip"),
                            pl.col("from_st").alias("board_st")),
                        ride.lazy().select(
                            "origin", pl.col("to_st").alias("st"),
                            pl.col("arr_c").alias("sec"),
                            pl.col("trip").alias("via_trip"), "board_st"),
                    ])
                    .filter(pl.col("sec") <= horizon_sec)
                    .group_by("origin", "st")
                    .agg(pl.exclude("origin", "st")
                         .sort_by(["sec", "via_trip"]).first())
                    .join(arr.lazy().select(
                        "origin", "st", pl.col("sec").alias("sec_old")),
                        on=["origin", "st"], how="left")
                    .filter(pl.col("sec_old").is_null()
                            | (pl.col("sec") < pl.col("sec_old")))
                    .select("origin", "st", "sec", "via_trip", "board_st")
                )
                reached_delta = collect_fn(
                    trans.lazy()
                    .select("origin", "trip",
                            pl.col("dep").alias("board_dep"),
                            pl.col("from_st").alias("board_st"))
                    .filter(pl.col("board_dep") <= horizon_sec)
                    .group_by("origin", "trip")
                    .agg(pl.exclude("origin", "trip")
                         .sort_by(["board_dep", "board_st"]).first())
                    .join(reached.lazy().select(
                        "origin", "trip",
                        pl.col("board_dep").alias("board_dep_old")),
                        on=["origin", "trip"], how="left")
                    .filter(pl.col("board_dep_old").is_null()
                            | (pl.col("board_dep")
                               < pl.col("board_dep_old")))
                    .select("origin", "trip", "board_dep", "board_st")
                )
                arr = collect_fn(
                    pl.concat([arr.lazy(), arr_delta.lazy()])
                    .group_by("origin", "st")
                    .agg(pl.exclude("origin", "st")
                         .sort_by(["sec", "via_trip"]).first())
                )
                reached = collect_fn(
                    pl.concat([reached.lazy(), reached_delta.lazy()])
                    .group_by("origin", "trip")
                    .agg(pl.exclude("origin", "trip")
                         .sort_by(["board_dep", "board_st"]).first())
                )
                if arr_delta.height == 0 and reached_delta.height == 0:
                    print(f"[{label}] CSA converged after {_legs} legs "
                          "(frontier exhausted)")
                    break
            else:
                print(f"[{label}] CSA hit max_legs={max_legs} "
                      "(result may be incomplete — raise the cap)")
            return arr


        def _trip_chains(conns_df):
            """Per-trip ordered station sequence + aligned [arr, dep]
            times, from the integer connection table (sorted by dep)."""
            trip_seq, trip_times = {}, {}
            for r in conns_df.sort("trip", "dep").iter_rows(named=True):
                seq = trip_seq.setdefault(r["trip"], [])
                tms = trip_times.setdefault(r["trip"], [])
                if not seq or seq[-1] != r["from_st"]:
                    seq.append(r["from_st"])
                    tms.append([None, r["dep"]])
                else:
                    tms[-1][1] = r["dep"]
                seq.append(r["to_st"])
                tms.append([r["arr_c"], None])
            return trip_seq, trip_times


        def _hhmm(_s):
            """seconds-after-midnight → "HH:MM" ("" for None)."""
            if _s is None:
                return ""
            _s = int(_s)
            return f"{_s // 3600:02d}:{(_s % 3600) // 60:02d}"


        def _reconstruct_journeys(arr, conns_df, st_info, origin_ids,
                                  dest_ids, max_legs, t_ref=None):
            """Backtrack the CSA predecessor chain into journeys.

            For each (origin id ∈ origin_ids, dest id ∈ dest_ids) walk
            `via_trip`/`board_st` from dest until the seed (via_trip ==
            _NO_TRIP — so this works for composite/profiled origin ids
            too), expand every trip segment to its called stations, and
            build the FULL ordered `stops` list ([station_feature_id,
            arr_hhmm, dep_hhmm, leg_idx]) + aligned `coords` ([lon,lat]).
            Transfer stations are kept TWICE (leg k's alight + leg k+1's
            board) so `stops`/`coords` stay aligned and leg structure is
            unambiguous.

            `t_ref` None → forward CSA. A number → the run was on a
            time-reversed connection table seeded T_REF = depart+horizon:
            the journey is un-reversed (stop list + coords reversed, each
            stop's real arr/dep = T_REF − reversed dep/arr, leg ids
            flipped), so it reads origin-station → hub in real time.

            Returns a list of per-journey dicts: osm_id,
            origin_station_id, dest_station_id, origin_name, dest_name,
            travel_min, n_transfers, depart_s (the seed time), stops,
            coords.
            """
            import json as _json  # noqa: F401  (callers may re-serialise)

            arr_lookup = {
                (r["origin"], r["st"]): (
                    r["sec"], r["via_trip"], r["board_st"])
                for r in arr.iter_rows(named=True)
            }
            trip_seq, trip_times = _trip_chains(conns_df)
            journeys = []
            for o in origin_ids:
                for d in dest_ids:
                    key = (o, d)
                    if key not in arr_lookup:
                        continue
                    # backtrack dest → seed (via_trip == _NO_TRIP)
                    segs, st = [], d           # (board_st, alight_st, trip)
                    for _ in range(max_legs + 1):
                        _sec, _via, _bst = arr_lookup[(o, st)]
                        if _via == _NO_TRIP:
                            break              # reached the seed station
                        segs.append((_bst, st, _via))
                        st = _bst
                    else:
                        continue               # chain didn't reach a seed
                    if not segs:
                        continue               # origin == dest
                    o_st = st                  # discovered seed station
                    seed_sec = arr_lookup[(o, o_st)][0]
                    segs.reverse()
                    # expand each segment to its called stations + times
                    stops, coords = [], []
                    ok = True
                    for seg_idx, (_b, _a, _trip) in enumerate(segs):
                        _ts = trip_seq.get(_trip)
                        _tt = trip_times.get(_trip)
                        if _ts is None or _tt is None:
                            ok = False
                            break
                        try:
                            _ib = _ts.index(_b)
                            _ia = _ts.index(_a, _ib)
                        except ValueError:
                            ok = False
                            break
                        for j in range(_ib, _ia + 1):
                            _st = _ts[j]
                            _arr_s, _dep_s = _tt[j]
                            # Clean leg-boundary semantics: at this
                            # segment's BOARD stop the journey has no
                            # arrival (you start the leg here); at its
                            # ALIGHT stop it has no departure (you leave
                            # the leg here). A transfer station is the
                            # alight of leg k AND the board of leg k+1,
                            # so its two entries carry (arr, "") then
                            # ("", dep). The trip's own through-schedule
                            # at a board/alight stop (the train we caught
                            # arriving earlier, or the train we left
                            # continuing on) is NOT the journey's use of
                            # that stop — emitting it produced phantom
                            # "depart 12:20 / arrive 12:11" itineraries
                            # and spurious "transfer at origin" lines.
                            _arr_h = "" if j == _ib else _hhmm(_arr_s)
                            _dep_h = "" if j == _ia else _hhmm(_dep_s)
                            stops.append([
                                st_info[_st][2], _arr_h, _dep_h, seg_idx])
                            coords.append(
                                [st_info[_st][0], st_info[_st][1]])
                    if not ok or len(stops) < 2:
                        continue
                    if t_ref is not None:
                        # un-reverse: the run was on time-reversed conns
                        n_leg = len(segs) - 1
                        _re_stops, _re_coords = [], []
                        for _stp, _crd in zip(reversed(stops),
                                              reversed(coords)):
                            _sid, _rarr, _rdep, _leg = _stp
                            # real arr = T_REF − reversed dep; real dep =
                            # T_REF − reversed arr (reversing a trip swaps
                            # arrival/departure at each stop)
                            _ra = (None if _rdep == "" else
                                   t_ref - (int(_rdep[:2]) * 3600
                                            + int(_rdep[3:]) * 60))
                            _rd = (None if _rarr == "" else
                                   t_ref - (int(_rarr[:2]) * 3600
                                            + int(_rarr[3:]) * 60))
                            _re_stops.append([_sid, _hhmm(_ra),
                                              _hhmm(_rd), n_leg - _leg])
                            _re_coords.append(_crd)
                        stops, coords = _re_stops, _re_coords
                        # real journey reads (real origin = d) → (hub = o_st)
                        src_st, dst_st = d, o_st
                    else:
                        src_st, dst_st = o_st, d
                    # travel time = real arrival at dest − real departure
                    # from origin, derived from the (real-time) stops
                    def _sec_of(_h):
                        return (int(_h[:2]) * 3600 + int(_h[3:]) * 60
                                if _h else None)
                    _dep0 = _sec_of(stops[0][2])
                    _arrN = _sec_of(stops[-1][1])
                    _travel_min = (round((_arrN - _dep0) / 60.0)
                                   if _dep0 is not None
                                   and _arrN is not None else 0)
                    journeys.append({
                        "osm_id": f"{st_info[src_st][2]}->"
                                  f"{st_info[dst_st][2]}",
                        "origin_station_id": st_info[src_st][2],
                        "dest_station_id": st_info[dst_st][2],
                        "origin_name": st_info[src_st][3],
                        "dest_name": st_info[dst_st][3],
                        "travel_min": _travel_min,
                        "n_transfers": len(segs) - 1,
                        "depart_s": seed_sec,
                        "stops": stops,
                        "coords": coords,
                    })
            return journeys


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

                # Transfer-hub importance score per station:
                #   hub_score = sum over LINE-pairs of
                #                 reach(km) x feasibility x terminus_factor
                #             + sum over TERMINATING lines of reach(km)
                # A line that TERMINATES at a station concentrates
                # transfer demand there (every rider must alight) far more
                # than one merely passing through, so it counts twice:
                #  * terminus_factor — each terminating line in a pair
                #    adds 1.0 to that pair's multiplier (neither=1x,
                #    one=2x, both=3x);
                #  * the standalone terminus term — each terminating line
                #    adds its full reach regardless of transfer
                #    feasibility, so a terminus strictly outranks a
                #    through-station of the same line even when its line
                #    pairs have no schedule overlap.
                # hub_rank drives symbol-sort-key in the map's
                # transit-stops-label layer so MapLibre places the
                # important hubs first.
                #
                # Two corrections make the score representative on this
                # all-days feed (RCA findings):
                #  * The Transitous feed fragments every line into many
                #    per-variant route_ids (23k route_ids, ~1.9 trips
                #    each), so pair LINES (route_short_name, long_name
                #    fallback) — not route_ids — or the score collapses to
                #    route_id-fragmentation noise.
                #  * Counting all trips across all service days inflates
                #    frequency. Expand calendar + calendar_dates to count
                #    each line's operating days and divide, yielding a
                #    representative departures-per-day rate so headway /
                #    overlap stay discriminating instead of saturating.
                # Computed at station granularity — platforms are folded
                # to station_feature_id BEFORE the line-pair self-join, so
                # a line pair is never double-counted.
                con.sql("""
                    CREATE OR REPLACE MACRO transit.haversine_km(
                        lat1, lon1, lat2, lon2
                    ) AS
                        2 * 6371.0 * asin(sqrt(
                            pow(sin(radians(lat2 - lat1) / 2), 2)
                            + cos(radians(lat1)) * cos(radians(lat2))
                              * pow(sin(radians(lon2 - lon1) / 2), 2)
                        ))
                """)
                con.sql("""
                    CREATE OR REPLACE TABLE transit.station_hub_scores AS
                    WITH
                      lines AS (
                        SELECT
                            route_id,
                            COALESCE(
                                NULLIF(trim(route_short_name), ''),
                                NULLIF(trim(route_long_name), ''),
                                route_id
                            ) AS line_id
                        FROM gtfs.routes
                      ),
                      -- calendar.txt expansion: (service_id, date) the
                      -- weekly pattern is active over its date range.
                      cal_days AS (
                        SELECT
                            c.service_id,
                            gs.d::DATE AS service_date
                        FROM gtfs.calendar c,
                             generate_series(
                                 c.start_date::TIMESTAMP,
                                 c.end_date::TIMESTAMP,
                                 INTERVAL '1 day'
                             ) AS gs(d)
                        WHERE [c.sunday, c.monday, c.tuesday, c.wednesday,
                               c.thursday, c.friday, c.saturday]
                              [dayofweek(gs.d::DATE) + 1] = 1
                      ),
                      -- apply calendar_dates.txt exceptions
                      -- (1 = service added, 2 = service removed).
                      service_dates AS (
                        (SELECT service_id, service_date FROM cal_days
                         EXCEPT
                         SELECT service_id, date FROM gtfs.calendar_dates
                         WHERE exception_type = 2)
                        UNION
                        (SELECT service_id, date FROM gtfs.calendar_dates
                         WHERE exception_type = 1)
                      ),
                      service_day_count AS (
                        SELECT service_id,
                               count(DISTINCT service_date) AS n_days
                        FROM service_dates
                        GROUP BY service_id
                      ),
                      -- every trip tagged with its line + operating-day count
                      trip_meta AS (
                        SELECT
                            t.trip_id,
                            l.line_id,
                            t.service_id,
                            COALESCE(sdc.n_days, 0) AS svc_days
                        FROM gtfs.trips t
                        JOIN lines l USING (route_id)
                        LEFT JOIN service_day_count sdc USING (service_id)
                      ),
                      -- distinct calendar days each line runs anywhere
                      line_service AS (
                        SELECT DISTINCT line_id, service_id FROM trip_meta
                      ),
                      line_days AS (
                        SELECT ls.line_id,
                               count(DISTINCT sd.service_date)
                                   AS line_service_days
                        FROM line_service ls
                        JOIN service_dates sd USING (service_id)
                        GROUP BY ls.line_id
                      ),
                      -- per (station, line): within-day operating envelope
                      -- (dep_sec is seconds-since-midnight, so min/max over
                      -- all days is still the daily envelope; GTFS >24h
                      -- overnight values preserved) + total feed-window
                      -- departures (each trip counted once per operating
                      -- day). gtfs-parquet stores departure_time as BIGINT
                      -- milliseconds-since-midnight, not a string.
                      station_line AS (
                        SELECT
                            sm.station_feature_id,
                            tm.line_id,
                            min(st.departure_time / 1000.0) AS first_dep,
                            max(st.departure_time / 1000.0) AS last_dep,
                            sum(tm.svc_days)                AS weighted_departures
                        FROM gtfs.stop_times st
                        JOIN trip_meta tm               USING (trip_id)
                        JOIN transit.station_members sm USING (stop_id)
                        WHERE st.departure_time IS NOT NULL
                        GROUP BY sm.station_feature_id, tm.line_id
                      ),
                      -- normalise to a representative departures-per-day
                      -- rate, then a real daily headway.
                      station_line_hw AS (
                        SELECT
                            sl.station_feature_id,
                            sl.line_id,
                            sl.first_dep,
                            sl.last_dep,
                            CASE
                                WHEN sl.weighted_departures
                                     / greatest(ld.line_service_days, 1) > 1
                                THEN ((sl.last_dep - sl.first_dep) / 60.0)
                                     / (sl.weighted_departures
                                        / greatest(ld.line_service_days, 1)
                                        - 1)
                                ELSE NULL
                            END AS avg_headway_min
                        FROM station_line sl
                        LEFT JOIN line_days ld USING (line_id)
                      ),
                      -- per line: geographic reach (km) — greater bbox
                      -- diagonal over all stops of all the line's route_id
                      -- fragments.
                      line_bbox AS (
                        SELECT
                            tm.line_id,
                            min(s.stop_lat) AS min_lat,
                            max(s.stop_lat) AS max_lat,
                            min(s.stop_lon) AS min_lon,
                            max(s.stop_lon) AS max_lon
                        FROM gtfs.stop_times st
                        JOIN trip_meta tm USING (trip_id)
                        JOIN gtfs.stops s USING (stop_id)
                        GROUP BY tm.line_id
                      ),
                      line_reach AS (
                        SELECT
                            line_id,
                            greatest(
                                transit.haversine_km(min_lat, min_lon, max_lat, max_lon),
                                transit.haversine_km(max_lat, min_lon, min_lat, max_lon)
                            ) AS reach_km
                        FROM line_bbox
                      ),
                      -- first + last stop of every trip — the line's
                      -- endpoints. A (station, line) is a TERMINUS pair
                      -- when the station owns an endpoint stop of any of
                      -- the line's trips (the line starts or ends here,
                      -- vs merely passing through).
                      trip_ends AS (
                        SELECT
                            trip_id,
                            arg_min(stop_id, stop_sequence) AS first_stop,
                            arg_max(stop_id, stop_sequence) AS last_stop
                        FROM gtfs.stop_times
                        WHERE stop_sequence IS NOT NULL
                        GROUP BY trip_id
                      ),
                      trip_endpoints AS (
                        SELECT trip_id, first_stop AS endpoint_stop
                        FROM trip_ends
                        UNION ALL
                        SELECT trip_id, last_stop FROM trip_ends
                      ),
                      station_line_terminus AS (
                        SELECT DISTINCT
                            sm.station_feature_id,
                            tm.line_id
                        FROM trip_endpoints ep
                        JOIN trip_meta tm               USING (trip_id)
                        JOIN transit.station_members sm
                          ON sm.stop_id = ep.endpoint_stop
                      ),
                      sl AS (
                        SELECT
                            h.station_feature_id,
                            h.line_id,
                            h.first_dep,
                            h.last_dep,
                            h.avg_headway_min,
                            COALESCE(lr.reach_km, 0.0) AS reach_km,
                            CASE WHEN slt.station_feature_id IS NOT NULL
                                 THEN 1 ELSE 0 END AS is_terminus
                        FROM station_line_hw h
                        LEFT JOIN line_reach lr USING (line_id)
                        LEFT JOIN station_line_terminus slt
                          USING (station_feature_id, line_id)
                      ),
                      pairs AS (
                        SELECT
                            a.station_feature_id,
                            a.reach_km        AS reach_a,
                            b.reach_km        AS reach_b,
                            a.avg_headway_min AS hw_a,
                            b.avg_headway_min AS hw_b,
                            a.is_terminus     AS term_a,
                            b.is_terminus     AS term_b,
                            greatest(0,
                                least(a.last_dep, b.last_dep)
                                - greatest(a.first_dep, b.first_dep)
                            ) / 60.0 AS overlap_min
                        FROM sl a
                        JOIN sl b
                          ON a.station_feature_id = b.station_feature_id
                         AND a.line_id < b.line_id
                      ),
                      weighted AS (
                        SELECT
                            station_feature_id,
                            greatest(reach_a, reach_b) AS reach_score_km,
                            least(1.0, greatest(0.0, overlap_min / 60.0))
                                AS overlap_quality,
                            CASE
                                WHEN hw_a IS NOT NULL AND hw_b IS NOT NULL
                                THEN least(1.0, greatest(0.0,
                                         60.0 / greatest(1.0,
                                             (hw_a + hw_b) / 2.0)))
                                ELSE 0.5
                            END AS headway_quality,
                            -- +1.0 per terminating line in the pair
                            1.0 + term_a + term_b AS terminus_factor
                        FROM pairs
                      ),
                      pair_weight AS (
                        SELECT
                            station_feature_id,
                            reach_score_km,
                            reach_score_km * overlap_quality
                                * headway_quality
                                * terminus_factor AS pair_weight
                        FROM weighted
                      ),
                      station_hub AS (
                        SELECT
                            station_feature_id,
                            sum(pair_weight)    AS hub_score,
                            count(*)            AS n_route_pairs,
                            max(reach_score_km) AS max_reach_km
                        FROM pair_weight
                        GROUP BY station_feature_id
                      ),
                      line_counts AS (
                        SELECT
                            station_feature_id,
                            count(DISTINCT line_id) AS n_routes,
                            count(DISTINCT line_id) FILTER (
                                WHERE is_terminus = 1
                            ) AS n_terminating_lines,
                            -- standalone terminus importance: each line
                            -- ending here contributes its full reach,
                            -- independent of whether any transfer is
                            -- temporally feasible. This is what makes a
                            -- terminus strictly outrank a through-station
                            -- of the same line even when the station's
                            -- line pairs have no schedule overlap (the
                            -- terminus_factor multiplier alone can't lift
                            -- a zero pair-score).
                            COALESCE(sum(reach_km) FILTER (
                                WHERE is_terminus = 1
                            ), 0.0) AS terminus_reach_sum
                        FROM sl
                        GROUP BY station_feature_id
                      ),
                      scored AS (
                        SELECT
                            h.station_feature_id,
                            -- transfer-feasibility term + standalone
                            -- terminus term (weight 1.0 = one unit of
                            -- reach per terminating line).
                            h.hub_score + rc.terminus_reach_sum AS hub_score,
                            rc.n_routes,
                            rc.n_terminating_lines,
                            h.n_route_pairs,
                            h.max_reach_km
                        FROM station_hub h
                        JOIN line_counts rc USING (station_feature_id)
                      )
                    SELECT
                        station_feature_id,
                        hub_score,
                        n_routes,
                        n_terminating_lines,
                        n_route_pairs,
                        max_reach_km,
                        ROW_NUMBER() OVER (
                            ORDER BY hub_score DESC,
                                     n_routes DESC,
                                     station_feature_id
                        ) AS hub_rank
                    FROM scored
                """)
                # hub-score summary — mirrors the roll-up log above.
                # grain MUST hold: one row per scored station_feature_id.
                hub = con.sql("""
                    SELECT
                        count(*)                           AS total_rows,
                        count(DISTINCT station_feature_id) AS distinct_stations,
                        round(max(hub_score), 1)           AS top_score,
                        round(median(hub_score), 1)        AS median_score
                    FROM transit.station_hub_scores
                """).fetchone()
                top5 = con.sql("""
                    SELECT station_feature_id, round(hub_score, 1)
                    FROM transit.station_hub_scores
                    ORDER BY hub_rank
                    LIMIT 5
                """).fetchall()
                print(
                    f"[match_gtfs_stops_to_osm] hub-score: "
                    f"stations_scored={hub[0]}, distinct={hub[1]}, "
                    f"top_score={hub[2]}, median_score={hub[3]} "
                    f"(grain OK = {hub[0] == hub[1]}); top5={top5}"
                )

                # Export the joined view as parquet for freestiler
                # ingestion (freestiler can't ATTACH a duckdb file mid-
                # query, so we round-trip through parquet — the same
                # pattern every other freestiler task already uses).
                #
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
                            m.osm_feature_id,
                            -- Station roll-up identity. Joined from
                            -- transit.station_members (one row per GTFS
                            -- stop_id — covers EVERY stop incl. the
                            -- unmatched, unlike matched_stops). CAST to
                            -- VARCHAR because freestiler drops numeric-
                            -- nullable columns on the parquet → MVT
                            -- round-trip. The id is already a string
                            -- ('node/..' | 'way/..' | 'gtfs/..' | a raw
                            -- stop_id) — the CAST makes the contract
                            -- explicit and null-safe.
                            CAST(sm.station_feature_id AS VARCHAR)
                                AS station_feature_id,
                            sm.station_name,
                            -- Exactly ONE row per station_feature_id is
                            -- 'true' — the member point closest to the
                            -- station anchor coords. transit-station-dot
                            -- and transit-stops-label both filter on this
                            -- so a big station shows ONE dot + ONE label,
                            -- not one per platform. String, not bool —
                            -- bools are dropped on the MVT round-trip
                            -- just like numeric-nullables.
                            CASE WHEN ROW_NUMBER() OVER (
                                PARTITION BY sm.station_feature_id
                                ORDER BY ST_Distance(
                                    ST_Point(s.stop_lon, s.stop_lat),
                                    ST_Point(sm.station_lon, sm.station_lat)
                                ), s.stop_id
                            ) = 1 THEN 'true' ELSE 'false' END
                                AS is_station_label,
                            -- Transfer-hub importance, per station_feature_id
                            -- (so every member row carries the station's
                            -- value). hub_rank drives both the progressive
                            -- label-disclosure filter and symbol-sort-key
                            -- in transit-stops-label. CAST to VARCHAR for
                            -- the same MVT-round-trip reason as the columns
                            -- above; COALESCE so a station with <2 routes
                            -- (no route pairs, absent from
                            -- station_hub_scores) still sorts last.
                            COALESCE(CAST(hs.hub_score AS VARCHAR), '0')
                                AS hub_score,
                            COALESCE(CAST(hs.hub_rank AS VARCHAR), '999999')
                                AS hub_rank
                        FROM gtfs.stops s
                        LEFT JOIN transit.matched_stops m USING (stop_id)
                        LEFT JOIN transit.station_members sm USING (stop_id)
                        LEFT JOIN transit.station_hub_scores hs
                          ON hs.station_feature_id = sm.station_feature_id
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
                           station_feature_id,
                           station_name,
                           is_station_label,
                           hub_score,
                           hub_rank
                    FROM read_parquet('{transit_parquet_path}')
                """
                # drop_rate MUST stay None (disables feature thinning).
                # The transit-stops-label map layer filters to the single
                # is_station_label='true' member row per station; an
                # exponential drop_rate randomly thins features below
                # base_zoom and can drop exactly that representative row,
                # making station labels (and the hub_rank-driven
                # symbol-sort-key prioritisation) appear erratically. The
                # stop set is small (~7.6k points), so keeping every
                # feature at every zoom costs little and is correct.
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-transit",
                    min_zoom=0,
                    max_zoom=14,
                    base_zoom=14,
                    drop_rate=None,
                    coalesce=True,
                )
                return str(out)

            @task
            def compute_chrono_isochrones(db_path: str) -> str:
                # Chronotrains-style per-station travel-time isochrones,
                # computed from the REAL Austria railway timetable.
                #
                # chronotrains (github.com/benjamintd/chronotrains) builds
                # a static station graph with 9 km/h short-hop edges and a
                # flat 20-min interchange penalty. Per the operator we
                # REPLACE those approximations with the actual schedule:
                # ride times AND transfer waiting times come straight from
                # gtfs.stop_times. The routing is a time-dependent
                # Connection Scan Algorithm (CSA) earliest-arrival
                # computation seeded at CHRONO_DEPART_S — inherently
                # multi-hop and trip/route-aware (each connection belongs
                # to a trip, hence a route).
                #
                # The CSA is a vectorised polars fixpoint: each iteration
                # relaxes EVERY connection once (join + group_by-min),
                # adding journeys with one more leg. It runs on the GPU via
                # cudf-polars when available (engine="gpu"), CPU otherwise.
                # Output: one buffered convex-hull polygon per (origin
                # station, hour band) -> austria-chrono-isochrones.parquet,
                # the freestiler intermediate for the austria-chrono tile.
                #
                # Reads austria.duckdb READ-ONLY; the DAG chain orders this
                # task after the match_* writers so no DuckDB write lock is
                # held by another process (R4 — deterministic ordering).
                import duckdb
                import polars as pl

                TILES_WORK.mkdir(parents=True, exist_ok=True)
                out = TILES_WORK / "austria-chrono-isochrones.parquet"
                if not _needs_regen(out):
                    return str(out)

                # ---- GPU engine probe (once) ----------------------------
                try:
                    pl.LazyFrame({"_p": [1]}).select(
                        pl.col("_p").sum()
                    ).collect(engine="gpu")
                    _engine = "gpu"
                except Exception as _exc:  # noqa: BLE001
                    _engine = "cpu"
                    print(
                        "[compute_chrono_isochrones] GPU engine unavailable "
                        f"({_exc!r}) - falling back to CPU"
                    )
                print(f"[compute_chrono_isochrones] polars engine={_engine}")

                def _collect(lf):
                    # GPU collect with a per-call CPU fallback — one
                    # iteration failing over must not abort the run.
                    if _engine == "gpu":
                        try:
                            return lf.collect(engine="gpu")
                        except Exception:  # noqa: BLE001
                            return lf.collect()
                    return lf.collect()

                def _to_seconds(df, src, dst):
                    # Normalise a GTFS clock field to seconds-after-
                    # midnight, branching on dtype:
                    #   * Utf8  -> "H:MM:SS" string (may exceed 24h for
                    #     overnight services) -> parse to seconds.
                    #   * numeric -> gtfs_parquet hands GTFS times over as
                    #     integer MILLISECONDS after midnight (verified
                    #     against the at_Railway feed: 43800000 = 12:10:00,
                    #     2220000 = 00:37:00) -> scale down by 1000.
                    if df[src].dtype == pl.Utf8:
                        _parts = pl.col(src).str.split(":")
                        return df.with_columns(
                            (
                                _parts.list.get(0, null_on_oob=True)
                                .cast(pl.Int64, strict=False).fill_null(0)
                                * 3600
                                + _parts.list.get(1, null_on_oob=True)
                                .cast(pl.Int64, strict=False).fill_null(0)
                                * 60
                                + _parts.list.get(2, null_on_oob=True)
                                .cast(pl.Int64, strict=False).fill_null(0)
                            ).alias(dst)
                        )
                    return df.with_columns(
                        (pl.col(src).cast(pl.Int64) // 1000).alias(dst)
                    )

                horizon = CHRONO_DEPART_S + max(CHRONO_BANDS_H) * 3600

                # ---- Pull the real timetable from DuckDB (read-only) ----
                con = duckdb.connect(db_path, read_only=True)

                # UNION TIMETABLE: connections are built from EVERY trip
                # the feed ships, across all service_ids — NOT one
                # reference service day. A single busiest-service_id
                # snapshot left ~half the stations unreachable (weekend-
                # only / seasonal / specific-date services were excluded);
                # clicking those in the route builder returned "no route
                # found". The union is the composite-day model: a station
                # served on ANY service pattern is routable. Documented
                # approximation: the CSA may chain two trips whose service
                # patterns never coincide on a real calendar date — an
                # accepted trade-off for full-network reachability, shared
                # by the chronomap, fastest-connections and route-builder
                # maps. Still format-agnostic — no calendar-date parsing —
                # and deterministic.

                # Consecutive stop_times pairs per trip -> connections,
                # rolled up stop -> station_feature_id. Clock fields are
                # pulled raw (parsed in polars by _to_seconds).
                conns = con.sql("""
                    WITH svc_trips AS (
                        SELECT trip_id FROM gtfs.trips
                    ),
                    seq AS (
                        SELECT
                            st.trip_id,
                            st.stop_id,
                            st.departure_time             AS dep_raw,
                            LEAD(st.stop_id)      OVER w   AS next_stop_id,
                            LEAD(st.arrival_time) OVER w   AS arr_raw
                        FROM gtfs.stop_times st
                        JOIN svc_trips USING (trip_id)
                        WINDOW w AS (
                            PARTITION BY st.trip_id
                            ORDER BY st.stop_sequence
                        )
                    )
                    SELECT
                        s.trip_id,
                        sm_from.station_feature_id AS from_station,
                        sm_to.station_feature_id   AS to_station,
                        s.dep_raw,
                        s.arr_raw
                    FROM seq s
                    JOIN transit.station_members sm_from
                      ON sm_from.stop_id = s.stop_id
                    JOIN transit.station_members sm_to
                      ON sm_to.stop_id = s.next_stop_id
                    WHERE s.next_stop_id IS NOT NULL
                      AND sm_from.station_feature_id IS NOT NULL
                      AND sm_to.station_feature_id IS NOT NULL
                      AND sm_from.station_feature_id
                          <> sm_to.station_feature_id
                """).pl()

                # Station catalogue (one row per parent station) +
                # top-N origins by hub_rank.
                stations = con.sql("""
                    SELECT
                        station_feature_id,
                        any_value(station_name) AS station_name,
                        any_value(station_lon)  AS station_lon,
                        any_value(station_lat)  AS station_lat
                    FROM transit.station_members
                    WHERE station_feature_id IS NOT NULL
                    GROUP BY station_feature_id
                """).pl()
                origins = con.sql(f"""
                    SELECT station_feature_id
                    FROM transit.station_hub_scores
                    ORDER BY hub_rank
                    LIMIT {CHRONO_ORIGIN_COUNT}
                """).pl()

                # Real transfer times from transfers.txt when the feed
                # shipped one (same-station entries, rolled to station
                # level); otherwise CHRONO_DEFAULT_TRANSFER_S.
                _has_transfers = con.sql(
                    "SELECT count(*) FROM information_schema.tables "
                    "WHERE table_schema = 'gtfs' "
                    "AND table_name = 'transfers'"
                ).fetchone()[0] > 0
                if _has_transfers:
                    transfers = con.sql("""
                        SELECT
                            sm_f.station_feature_id AS station_feature_id,
                            min(TRY_CAST(tr.min_transfer_time AS BIGINT))
                                AS transfer_s
                        FROM gtfs.transfers tr
                        JOIN transit.station_members sm_f
                          ON sm_f.stop_id = tr.from_stop_id
                        JOIN transit.station_members sm_t
                          ON sm_t.stop_id = tr.to_stop_id
                        WHERE sm_f.station_feature_id
                              = sm_t.station_feature_id
                          AND tr.min_transfer_time IS NOT NULL
                        GROUP BY sm_f.station_feature_id
                    """).pl()
                else:
                    transfers = pl.DataFrame(
                        schema={
                            "station_feature_id": pl.Utf8,
                            "transfer_s": pl.Int64,
                        }
                    )
                con.close()
                print(
                    "[compute_chrono_isochrones] connections="
                    f"{conns.height}, stations={stations.height}, "
                    f"origins={origins.height}, "
                    f"transfers.txt={'yes' if _has_transfers else 'no'}"
                )

                # ---- Parse clock fields + assign integer node ids -------
                conns = conns.filter(
                    pl.col("dep_raw").is_not_null()
                    & pl.col("arr_raw").is_not_null()
                )
                if conns["dep_raw"].dtype == pl.Utf8:
                    conns = conns.filter(
                        (pl.col("dep_raw").str.len_chars() > 0)
                        & (pl.col("arr_raw").str.len_chars() > 0)
                    )
                conns = _to_seconds(conns, "dep_raw", "dep")
                conns = _to_seconds(conns, "arr_raw", "arr_c")
                conns = conns.filter(pl.col("arr_c") >= pl.col("dep"))

                station_ids = stations.select(
                    "station_feature_id"
                ).with_row_index("st")
                trip_ids = conns.select(
                    "trip_id"
                ).unique().with_row_index("trip")

                conns_df = (
                    conns
                    .join(
                        station_ids.rename({
                            "station_feature_id": "from_station",
                            "st": "from_st",
                        }),
                        on="from_station",
                    )
                    .join(
                        station_ids.rename({
                            "station_feature_id": "to_station",
                            "st": "to_st",
                        }),
                        on="to_station",
                    )
                    .join(trip_ids, on="trip_id")
                    .select("trip", "from_st", "to_st", "dep", "arr_c")
                )
                # NOTE: conns_df is the FULL-DAY connection table (no
                # departure-time window filter) — the profiled CSA needs
                # the whole service day. The forward/backward passes
                # naturally ignore out-of-window connections (a seed at
                # 08:00 can't board a 07:00 departure; an arrival pruned
                # at the horizon can't lead anywhere) so their results
                # are unchanged vs. the old pre-filtered table.

                origin_st = (
                    origins
                    .join(station_ids, on="station_feature_id")
                    .get_column("st")
                    .to_list()
                )
                if not origin_st:
                    raise RuntimeError(
                        "compute_chrono_isochrones: no origin stations "
                        "resolved from transit.station_hub_scores"
                    )

                transfer_i = (
                    transfers
                    .join(station_ids, on="station_feature_id")
                    .select(
                        pl.col("st").alias("from_st"),
                        pl.col("transfer_s").cast(pl.Int64),
                    )
                )

                # ---- CSA passes ---------------------------------------
                # The frontier-relaxation CSA fixpoint is the module-level
                # `_run_csa` helper. compute_chrono_isochrones runs it
                # THREE ways off the SAME full-day connection table:
                #   * forward 08:00 — isochrones + hub→station (h2s)
                #   * backward      — station→hub (s2h), on a time-
                #                     reversed connection table
                #   * profiled      — hub→hub at every 30-min grid time
                #                     (h2h), so route legs match by time
                # The forward `arr` carries predecessor pointers
                # (via_trip / board_st, seeds marked _NO_TRIP) so the
                # route tasks backtrack full journeys without re-running
                # any CSA. All three results are persisted for them.

                # forward 08:00 — every hub origin seeded at CHRONO_DEPART_S
                _fwd_seed = pl.DataFrame(
                    {
                        "origin": origin_st,
                        "st": origin_st,
                        "sec": [CHRONO_DEPART_S] * len(origin_st),
                        "via_trip": [_NO_TRIP] * len(origin_st),
                        "board_st": origin_st,
                    },
                    schema={
                        "origin": pl.UInt32, "st": pl.UInt32,
                        "sec": pl.Int64, "via_trip": pl.UInt32,
                        "board_st": pl.UInt32,
                    },
                )
                arr = _run_csa(
                    conns_df, _fwd_seed, horizon, transfer_i,
                    CHRONO_DEFAULT_TRANSFER_S, CHRONO_MAX_LEGS,
                    _collect, "compute_chrono_isochrones forward",
                )

                # backward — station→hub. A time-reversed connection
                # table (T_REF = CHRONO_DEPART_S + horizon span):
                # (trip,from,to,dep,arr) -> (trip,to,from, T_REF-arr,
                # T_REF-dep). The forward CSA on it, seeded from the hubs
                # at reversed-time 0, yields the fastest journey TO each
                # hub FROM every station — un-reversed by the route task.
                _t_ref = CHRONO_DEPART_S + max(CHRONO_BANDS_H) * 3600
                conns_rev = conns_df.select(
                    "trip",
                    pl.col("to_st").alias("from_st"),
                    pl.col("from_st").alias("to_st"),
                    (_t_ref - pl.col("arr_c")).alias("dep"),
                    (_t_ref - pl.col("dep")).alias("arr_c"),
                ).filter(
                    (pl.col("dep") >= 0)
                    & (pl.col("arr_c") >= pl.col("dep"))
                )
                _bwd_seed = pl.DataFrame(
                    {
                        "origin": origin_st,
                        "st": origin_st,
                        "sec": [0] * len(origin_st),
                        "via_trip": [_NO_TRIP] * len(origin_st),
                        "board_st": origin_st,
                    },
                    schema={
                        "origin": pl.UInt32, "st": pl.UInt32,
                        "sec": pl.Int64, "via_trip": pl.UInt32,
                        "board_st": pl.UInt32,
                    },
                )
                arr_back = _run_csa(
                    conns_rev, _bwd_seed, _t_ref, transfer_i,
                    CHRONO_DEFAULT_TRANSFER_S, CHRONO_MAX_LEGS,
                    _collect, "compute_chrono_isochrones backward",
                )

                # profiled forward — hub→hub at every grid departure
                # time. Each (hub, grid-time) is a DISTINCT composite
                # `origin` id; a side table maps it back. _run_csa is
                # vectorised over `origin`, so this is ONE batched run.
                _profile_seed_rows = []
                _profile_map_rows = []
                _sid = 0
                for _h_st in origin_st:
                    for _g in CHRONO_HUB_PROFILE_TIMES:
                        _profile_seed_rows.append({
                            "origin": _sid, "st": _h_st, "sec": _g,
                            "via_trip": _NO_TRIP, "board_st": _h_st,
                        })
                        _profile_map_rows.append({
                            "origin": _sid, "hub_st": _h_st,
                            "depart_s": _g,
                        })
                        _sid += 1
                _prof_seed = pl.DataFrame(
                    _profile_seed_rows,
                    schema={
                        "origin": pl.UInt32, "st": pl.UInt32,
                        "sec": pl.Int64, "via_trip": pl.UInt32,
                        "board_st": pl.UInt32,
                    },
                )
                _prof_horizon = (
                    max(CHRONO_HUB_PROFILE_TIMES)
                    + max(CHRONO_BANDS_H) * 3600
                )
                arr_profile = _run_csa(
                    conns_df, _prof_seed, _prof_horizon, transfer_i,
                    CHRONO_DEFAULT_TRANSFER_S, CHRONO_MAX_LEGS,
                    _collect, "compute_chrono_isochrones profiled",
                )
                # Persisted in FULL (every reached station, not just hub
                # destinations): compute_route_network must backtrack the
                # predecessor chain through intermediate stations, so it
                # needs the whole profiled arr — it then keeps only the
                # hub→hub journeys at reconstruction time.

                # ---- Persist the CSA results for the route tasks ------
                # The converged arr states (with predecessor pointers),
                # the integer connection tables (forward + reversed), the
                # station catalogue, and the profile seed map. These let
                # compute_fastest_connections / compute_route_network
                # backtrack journeys without re-running any CSA or
                # re-deriving the connection SQL (R3 — one source of
                # truth for the routing graph).
                arr.select(
                    "origin", "st", "sec", "via_trip", "board_st"
                ).write_parquet(
                    TILES_WORK / "austria-chrono-arr.parquet"
                )
                arr_back.select(
                    "origin", "st", "sec", "via_trip", "board_st"
                ).write_parquet(
                    TILES_WORK / "austria-chrono-arr-back.parquet"
                )
                arr_profile.select(
                    "origin", "st", "sec", "via_trip", "board_st"
                ).write_parquet(
                    TILES_WORK / "austria-chrono-arr-profile.parquet"
                )
                pl.DataFrame(
                    _profile_map_rows,
                    schema={
                        "origin": pl.UInt32, "hub_st": pl.UInt32,
                        "depart_s": pl.Int64,
                    },
                ).write_parquet(
                    TILES_WORK / "austria-chrono-profile-map.parquet"
                )
                conns_df.select(
                    "trip", "from_st", "to_st", "dep", "arr_c"
                ).write_parquet(
                    TILES_WORK / "austria-chrono-conns.parquet"
                )
                conns_rev.select(
                    "trip", "from_st", "to_st", "dep", "arr_c"
                ).write_parquet(
                    TILES_WORK / "austria-chrono-conns-rev.parquet"
                )
                station_ids.join(
                    stations, on="station_feature_id"
                ).select(
                    "st", "station_feature_id", "station_name",
                    "station_lon", "station_lat",
                ).write_parquet(
                    TILES_WORK / "austria-chrono-stations.parquet"
                )
                print(
                    "[compute_chrono_isochrones] persisted CSA results — "
                    f"arr={arr.height}, arr_back={arr_back.height}, "
                    f"arr_profile(h2h)={arr_profile.height}"
                )

                # ---- Reduce to per-origin reachability ------------------
                fid = station_ids.rename(
                    {"station_feature_id": "_fid", "st": "_st"}
                )
                reach = (
                    arr
                    .with_columns(
                        (pl.col("sec") - CHRONO_DEPART_S).alias(
                            "travel_seconds"
                        )
                    )
                    .filter(pl.col("travel_seconds") >= 0)
                    .join(
                        fid.rename(
                            {"_st": "st", "_fid": "dest_station_id"}
                        ),
                        on="st",
                    )
                    .join(
                        fid.rename(
                            {"_st": "origin", "_fid": "origin_station_id"}
                        ),
                        on="origin",
                    )
                    .join(
                        stations.rename({
                            "station_feature_id": "dest_station_id",
                            "station_lon": "dest_lon",
                            "station_lat": "dest_lat",
                        }).select(
                            "dest_station_id", "dest_lon", "dest_lat"
                        ),
                        on="dest_station_id",
                    )
                    .join(
                        stations.rename({
                            "station_feature_id": "origin_station_id",
                            "station_name": "origin_name",
                            "station_lon": "origin_lon",
                            "station_lat": "origin_lat",
                        }).select(
                            "origin_station_id", "origin_name",
                            "origin_lon", "origin_lat",
                        ),
                        on="origin_station_id",
                    )
                    .select(
                        "origin_station_id",
                        "origin_name",
                        "origin_lon",
                        "origin_lat",
                        "dest_station_id",
                        "dest_lon",
                        "dest_lat",
                        "travel_seconds",
                    )
                )
                if reach.height == 0:
                    raise RuntimeError(
                        "compute_chrono_isochrones: CSA produced no "
                        "reachable stations — check the GTFS timetable"
                    )
                _reach_max_h = reach.select(
                    pl.col("travel_seconds").max() / 3600.0
                ).item()
                print(
                    "[compute_chrono_isochrones] reachability pairs="
                    f"{reach.height}, max travel={_reach_max_h:.1f} h"
                )

                # ---- Isochrone RINGS + clickable origin markers ---------
                # DuckDB Spatial. Per band: the CUMULATIVE point set
                # (every dest within k hours) -> buffered convex hull. The
                # rendered geometry is then the RING — this band's hull
                # MINUS the previous band's — so every pixel carries
                # exactly ONE band colour (no muddy 12-layer translucent
                # stack). Plus one POINT per origin station, theme
                # 'chrono-origin', so the chronomap can render the
                # clickable stations as distinct markers. The freestiler
                # intermediate parquet carries STRING attributes only (the
                # parquet -> MVT round-trip drops numeric-nullable / bool
                # columns — the same constraint the transit tile documents).
                reach_parquet = TILES_WORK / "austria-chrono-reach.parquet"
                reach.write_parquet(reach_parquet)
                _bands_values = ", ".join(
                    f"({h})" for h in CHRONO_BANDS_H
                )
                con2 = duckdb.connect()
                con2.sql("INSTALL spatial; LOAD spatial;")
                con2.sql(f"""
                    COPY (
                        WITH reach AS (
                            SELECT * FROM read_parquet('{reach_parquet}')
                        ),
                        bands(band_hours) AS (VALUES {_bands_values}),
                        per_band AS (
                            SELECT
                                r.origin_station_id,
                                any_value(r.origin_name) AS origin_name,
                                b.band_hours,
                                ST_Buffer(ST_ConvexHull(ST_Collect(LIST(
                                    ST_Point(r.dest_lon, r.dest_lat)
                                ))), {CHRONO_HULL_BUFFER_DEG}) AS hull
                            FROM reach r
                            JOIN bands b
                              ON r.travel_seconds <= b.band_hours * 3600
                            GROUP BY r.origin_station_id, b.band_hours
                        ),
                        -- Enforce EXACT nesting: nested(k) = the union of
                        -- hull(1..k). Buffering each hull SEPARATELY can
                        -- leave tiny approximation slivers where a blunter
                        -- inner hull's buffered arc pokes past a sharper
                        -- outer one; the cumulative union heals that so the
                        -- rings below are guaranteed disjoint annuli.
                        nested AS (
                            SELECT
                                p.origin_station_id,
                                any_value(p.origin_name) AS origin_name,
                                p.band_hours,
                                ST_Union_Agg(p2.hull) AS nhull
                            FROM per_band p
                            JOIN per_band p2
                              ON p2.origin_station_id = p.origin_station_id
                             AND p2.band_hours <= p.band_hours
                            GROUP BY p.origin_station_id, p.band_hours
                        ),
                        rings AS (
                            SELECT
                                origin_station_id, origin_name, band_hours,
                                CASE WHEN prev_nhull IS NULL THEN nhull
                                     ELSE ST_Difference(nhull, prev_nhull)
                                END AS geometry
                            FROM (
                                SELECT *, LAG(nhull) OVER (
                                    PARTITION BY origin_station_id
                                    ORDER BY band_hours
                                ) AS prev_nhull
                                FROM nested
                            )
                        ),
                        origins AS (
                            SELECT DISTINCT
                                origin_station_id, origin_name,
                                origin_lon, origin_lat
                            FROM reach
                        )
                        SELECT
                            origin_station_id || '-'
                                || CAST(band_hours AS VARCHAR) AS osm_id,
                            geometry,
                            'chrono'                           AS theme,
                            origin_station_id,
                            origin_name,
                            CAST(band_hours AS VARCHAR)         AS band_hours
                        FROM rings
                        WHERE geometry IS NOT NULL
                          AND NOT ST_IsEmpty(geometry)
                        UNION ALL
                        SELECT
                            origin_station_id || '-origin'     AS osm_id,
                            ST_Point(origin_lon, origin_lat)    AS geometry,
                            'chrono-origin'                    AS theme,
                            origin_station_id,
                            origin_name,
                            '0'                                AS band_hours
                        FROM origins
                    ) TO '{out}' (FORMAT 'parquet')
                """)
                _n_feat = con2.sql(
                    f"SELECT count(*) FROM read_parquet('{out}')"
                ).fetchone()[0]
                con2.close()
                print(
                    "[compute_chrono_isochrones] wrote "
                    f"{_n_feat} chrono features (isochrone rings + origin "
                    f"markers) -> {out}"
                )
                return str(out)

            @task
            def freestiler_chrono_convert(chrono_parquet_path: str) -> str:
                # Bake the per-origin isochrone polygons to PMTiles —
                # EXACTLY the freestiler -> PMTiles path every other
                # on-map dataset uses (cf. freestiler_transit_convert).
                # martin auto-discovers the archive; the chronomap cell
                # consumes it as the `austria-chrono` vector source.
                # max_zoom=10 — these are coarse country-scale polygons.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-chrono.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT osm_id,
                           geometry,
                           theme,
                           origin_station_id,
                           origin_name,
                           band_hours
                    FROM read_parquet('{chrono_parquet_path}')
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-chrono",
                    min_zoom=0,
                    max_zoom=10,
                    drop_rate=None,
                    simplification=True,
                    coalesce=False,
                )
                return str(out)

            @task
            def compute_fastest_connections(chrono_iso: str) -> str:
                # Fastest journeys BETWEEN the top hub stations, drawn as
                # the actual route through every called station.
                #
                # The CSA in compute_chrono_isochrones already found the
                # earliest arrival from each of the CHRONO_ORIGIN_COUNT
                # hub origins to every reachable station AND (via the
                # predecessor pointers) recorded HOW. This task just
                # BACKTRACKS that — via the module-level
                # `_reconstruct_journeys` helper (shared with
                # compute_route_network, R3) — for every ordered
                # hub→hub pair: no CSA, no GPU. `chrono_iso` is the
                # upstream dependency.
                #
                # Output: one LineString per (origin, dest) journey
                # through its called stations + one POINT per origin
                # marker, baked into austria-fastlink-paths.parquet. Each
                # journey carries a leg-endpoint `itinerary` JSON string
                # (derived here from the helper's full `stops` list) —
                # the leg-by-leg schedule the map cell's click handler
                # renders.
                import duckdb
                import json
                import polars as pl

                TILES_WORK.mkdir(parents=True, exist_ok=True)
                out = TILES_WORK / "austria-fastlink-paths.parquet"
                if not _needs_regen(out):
                    return str(out)

                arr = pl.read_parquet(
                    TILES_WORK / "austria-chrono-arr.parquet"
                )
                conns = pl.read_parquet(
                    TILES_WORK / "austria-chrono-conns.parquet"
                )
                stations = pl.read_parquet(
                    TILES_WORK / "austria-chrono-stations.parquet"
                )
                # st (int) -> (lon, lat, station_feature_id, name)
                st_info = {
                    r["st"]: (
                        r["station_lon"], r["station_lat"],
                        r["station_feature_id"], r["station_name"],
                    )
                    for r in stations.iter_rows(named=True)
                }
                _fid_name = {v[2]: v[3] for v in st_info.values()}

                # hub→hub journeys via the shared backtrack helper.
                origins = sorted(set(arr["origin"].to_list()))
                journeys = _reconstruct_journeys(
                    arr, conns, st_info, origins, origins,
                    CHRONO_MAX_LEGS, t_ref=None,
                )
                if not journeys:
                    raise RuntimeError(
                        "compute_fastest_connections: no journeys "
                        "reconstructed — check the CSA intermediates"
                    )

                # Long-format per-vertex rows for the DuckDB geometry
                # build; the leg-endpoint `itinerary` JSON is derived
                # from the helper's full `stops` list (group by leg_idx;
                # first/last stop of each leg = its board/alight) so the
                # tile's itinerary format is unchanged for the fastlink
                # map's click panel.
                legs_rows = []
                _transfer_hist = {}
                for _j in journeys:
                    _stops = _j["stops"]
                    _itin_legs = []
                    for _k in range(_j["n_transfers"] + 1):
                        _ls = [s for s in _stops if s[3] == _k]
                        if not _ls:
                            continue
                        _itin_legs.append([
                            _fid_name.get(_ls[0][0], _ls[0][0]),
                            _ls[0][2],
                            _fid_name.get(_ls[-1][0], _ls[-1][0]),
                            _ls[-1][1],
                        ])
                    _itinerary = json.dumps(
                        _itin_legs, separators=(",", ":")
                    )
                    for _seq_i, _crd in enumerate(_j["coords"]):
                        legs_rows.append({
                            "osm_id": _j["osm_id"],
                            "origin_station_id": _j["origin_station_id"],
                            "dest_station_id": _j["dest_station_id"],
                            "origin_name": _j["origin_name"],
                            "dest_name": _j["dest_name"],
                            "travel_min": str(_j["travel_min"]),
                            "n_transfers": str(_j["n_transfers"]),
                            "itinerary": _itinerary,
                            "seq": _seq_i,
                            "lon": _crd[0],
                            "lat": _crd[1],
                        })
                    _transfer_hist[_j["n_transfers"]] = (
                        _transfer_hist.get(_j["n_transfers"], 0) + 1
                    )

                legs = pl.DataFrame(legs_rows)
                legs_parquet = (
                    TILES_WORK / "austria-chrono-fastlink-legs.parquet"
                )
                legs.write_parquet(legs_parquet)
                print(
                    "[compute_fastest_connections] reconstructed "
                    f"{len(journeys)} hub->hub journeys; transfer-count "
                    f"histogram={dict(sorted(_transfer_hist.items()))}"
                )

                # ---- Build geometry ------------------------------------
                # Python built the per-vertex legs; DuckDB stitches each
                # journey's vertices (ordered by seq) into a LINESTRING
                # via ST_GeomFromText, plus one ST_Point per origin
                # (theme='fastlink-origin') baked into the SAME tile —
                # mirroring austria-chrono's rings + origin markers.
                con2 = duckdb.connect()
                con2.sql("INSTALL spatial; LOAD spatial;")
                con2.sql(f"""
                    COPY (
                        WITH legs AS (
                            SELECT * FROM read_parquet('{legs_parquet}')
                        ),
                        lines AS (
                            SELECT
                                osm_id,
                                any_value(origin_station_id)
                                    AS origin_station_id,
                                any_value(dest_station_id)
                                    AS dest_station_id,
                                any_value(origin_name) AS origin_name,
                                any_value(dest_name)   AS dest_name,
                                any_value(travel_min)  AS travel_min,
                                any_value(n_transfers) AS n_transfers,
                                any_value(itinerary)   AS itinerary,
                                ST_GeomFromText(
                                    'LINESTRING(' || string_agg(
                                        CAST(lon AS VARCHAR) || ' '
                                        || CAST(lat AS VARCHAR),
                                        ', ' ORDER BY seq
                                    ) || ')'
                                ) AS geometry
                            FROM legs
                            GROUP BY osm_id
                        ),
                        origin_pts AS (
                            SELECT
                                origin_station_id,
                                any_value(origin_name) AS origin_name,
                                arg_min(lon, seq)      AS lon,
                                arg_min(lat, seq)      AS lat
                            FROM legs
                            GROUP BY origin_station_id
                        )
                        SELECT
                            osm_id,
                            geometry,
                            'fastlink'          AS theme,
                            origin_station_id,
                            dest_station_id,
                            origin_name,
                            dest_name,
                            travel_min,
                            n_transfers,
                            itinerary
                        FROM lines
                        UNION ALL
                        SELECT
                            origin_station_id || '-origin' AS osm_id,
                            ST_Point(lon, lat)             AS geometry,
                            'fastlink-origin'              AS theme,
                            origin_station_id,
                            ''                  AS dest_station_id,
                            origin_name,
                            ''                  AS dest_name,
                            '0'                 AS travel_min,
                            '0'                 AS n_transfers,
                            ''                  AS itinerary
                        FROM origin_pts
                    ) TO '{out}' (FORMAT 'parquet')
                """)
                _n = con2.sql(
                    f"SELECT count(*) FROM read_parquet('{out}')"
                ).fetchone()[0]
                con2.close()
                print(
                    "[compute_fastest_connections] wrote "
                    f"{_n} fastlink features (journeys + origin markers) "
                    f"-> {out}"
                )
                return str(out)

            @task
            def freestiler_fastlink_convert(fastlink_paths: str) -> str:
                # Bake the hub->hub fastest-journey lines + origin markers
                # to PMTiles — the same freestiler -> PMTiles path every
                # other on-map dataset uses. martin auto-discovers the
                # archive; the fastest-connections cell consumes it as the
                # `austria-fastlink` vector source.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-fastlink.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT osm_id,
                           geometry,
                           theme,
                           origin_station_id,
                           dest_station_id,
                           origin_name,
                           dest_name,
                           travel_min,
                           n_transfers,
                           itinerary
                    FROM read_parquet('{fastlink_paths}')
                """
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-fastlink",
                    min_zoom=0,
                    max_zoom=10,
                    drop_rate=None,
                    simplification=True,
                    coalesce=False,
                )
                return str(out)

            @task
            def compute_route_network(chrono_iso: str) -> str:
                # The hub-decomposition routing network for the route
                # builder: hub→station (h2s), station→hub (s2h), and the
                # hub→hub departure-time profile (h2h). All three are
                # backtracked via the shared `_reconstruct_journeys`
                # helper from the CSA results compute_chrono_isochrones
                # persisted — no CSA, no GPU. `chrono_iso` is the upstream
                # dependency.
                #
                # Output: austria-routehub-paths.parquet — one journey
                # per row, each carrying its FULL ordered `stops` list
                # (JSON) so the client route builder can slice a direct
                # sub-path between any two stations that share a journey,
                # falling back to hub-decomposition otherwise. Geometry
                # is a degenerate origin->dest 2-point line only: the
                # route-builder JS rebuilds on-map geometry from the
                # austria-transit station points + each journey's `stops`
                # and never reads this tile's geometry. The tile is baked
                # z0-only (freestiler_routehub_convert) so every journey
                # lives in the single always-loaded z0 tile and
                # querySourceFeatures sees them all.
                import duckdb
                import json
                import polars as pl

                TILES_WORK.mkdir(parents=True, exist_ok=True)
                out = TILES_WORK / "austria-routehub-paths.parquet"
                if not _needs_regen(out):
                    return str(out)

                arr = pl.read_parquet(
                    TILES_WORK / "austria-chrono-arr.parquet"
                )
                arr_back = pl.read_parquet(
                    TILES_WORK / "austria-chrono-arr-back.parquet"
                )
                arr_profile = pl.read_parquet(
                    TILES_WORK / "austria-chrono-arr-profile.parquet"
                )
                conns = pl.read_parquet(
                    TILES_WORK / "austria-chrono-conns.parquet"
                )
                conns_rev = pl.read_parquet(
                    TILES_WORK / "austria-chrono-conns-rev.parquet"
                )
                stations = pl.read_parquet(
                    TILES_WORK / "austria-chrono-stations.parquet"
                )
                st_info = {
                    r["st"]: (
                        r["station_lon"], r["station_lat"],
                        r["station_feature_id"], r["station_name"],
                    )
                    for r in stations.iter_rows(named=True)
                }
                _t_ref = CHRONO_DEPART_S + max(CHRONO_BANDS_H) * 3600

                hubs = sorted(set(arr["origin"].to_list()))
                fwd_dests = sorted(set(arr["st"].to_list()))
                bwd_dests = sorted(set(arr_back["st"].to_list()))
                prof_origins = sorted(set(arr_profile["origin"].to_list()))

                # hub → station (forward CSA). The osm_id is theme-
                # prefixed: a hub→hub pair reachable both ways yields one
                # h2s journey AND one s2h journey with identical endpoint
                # ids — without the prefix they collide on the DuckDB
                # GROUP BY osm_id below and one journey is silently lost.
                h2s = _reconstruct_journeys(
                    arr, conns, st_info, hubs, fwd_dests,
                    CHRONO_MAX_LEGS, t_ref=None,
                )
                for _j in h2s:
                    _j["theme"] = "h2s"
                    _j["depart_grid"] = ""
                    _j["osm_id"] = f"h2s/{_j['osm_id']}"
                # station → hub (backward CSA on conns_rev; un-reversed
                # by the helper's t_ref branch so it reads station→hub)
                s2h = _reconstruct_journeys(
                    arr_back, conns_rev, st_info, hubs, bwd_dests,
                    CHRONO_MAX_LEGS, t_ref=_t_ref,
                )
                for _j in s2h:
                    _j["theme"] = "s2h"
                    _j["depart_grid"] = ""
                    _j["osm_id"] = f"s2h/{_j['osm_id']}"
                # hub → hub PROFILE (profiled forward CSA). The helper
                # discovers the real hub origin + the seed (grid) time
                # from each composite origin id; the osm_id is widened
                # with the grid time so every (hub_a, grid, hub_b) is
                # distinct (and theme-prefixed like h2s/s2h).
                h2h = _reconstruct_journeys(
                    arr_profile, conns, st_info, prof_origins, hubs,
                    CHRONO_MAX_LEGS, t_ref=None,
                )
                for _j in h2h:
                    _j["theme"] = "h2h"
                    _grid = _hhmm(_j["depart_s"])
                    _j["depart_grid"] = _grid
                    _j["osm_id"] = (
                        f"h2h/{_j['origin_station_id']}@{_grid}->"
                        f"{_j['dest_station_id']}"
                    )

                journeys = h2s + s2h + h2h
                if not journeys:
                    raise RuntimeError(
                        "compute_route_network: no journeys "
                        "reconstructed — check the CSA intermediates"
                    )

                # Long-format rows for the DuckDB geometry build — two
                # per journey (origin + dest endpoints only). The geometry
                # is a degenerate origin->dest line the JS never reads
                # (see above); the full ordered `stops` list rides every
                # row as a JSON string.
                legs_rows = []
                for _j in journeys:
                    _stops_json = json.dumps(
                        _j["stops"], separators=(",", ":")
                    )
                    _depart_hhmm = _j["stops"][0][2]
                    _arrive_hhmm = _j["stops"][-1][1]
                    _endpoints = (_j["coords"][0], _j["coords"][-1])
                    for _seq_i, _crd in enumerate(_endpoints):
                        legs_rows.append({
                            "osm_id": _j["osm_id"],
                            "theme": _j["theme"],
                            "origin_station_id": _j["origin_station_id"],
                            "dest_station_id": _j["dest_station_id"],
                            "travel_min": str(_j["travel_min"]),
                            "n_transfers": str(_j["n_transfers"]),
                            "depart_hhmm": _depart_hhmm,
                            "arrive_hhmm": _arrive_hhmm,
                            "depart_grid": _j["depart_grid"],
                            "stops": _stops_json,
                            "seq": _seq_i,
                            "lon": _crd[0],
                            "lat": _crd[1],
                        })
                _legs = pl.DataFrame(legs_rows)
                _legs_parquet = (
                    TILES_WORK / "austria-routehub-legs.parquet"
                )
                _legs.write_parquet(_legs_parquet)
                print(
                    "[compute_route_network] reconstructed journeys — "
                    f"h2s={len(h2s)}, s2h={len(s2h)}, h2h={len(h2h)}"
                )

                con2 = duckdb.connect()
                con2.sql("INSTALL spatial; LOAD spatial;")
                con2.sql(f"""
                    COPY (
                        WITH legs AS (
                            SELECT * FROM read_parquet('{_legs_parquet}')
                        )
                        SELECT
                            osm_id,
                            ST_GeomFromText(
                                'LINESTRING(' || string_agg(
                                    CAST(lon AS VARCHAR) || ' '
                                    || CAST(lat AS VARCHAR),
                                    ', ' ORDER BY seq
                                ) || ')'
                            ) AS geometry,
                            any_value(theme)             AS theme,
                            any_value(origin_station_id)
                                AS origin_station_id,
                            any_value(dest_station_id)
                                AS dest_station_id,
                            any_value(travel_min)        AS travel_min,
                            any_value(n_transfers)       AS n_transfers,
                            any_value(depart_hhmm)       AS depart_hhmm,
                            any_value(arrive_hhmm)       AS arrive_hhmm,
                            any_value(depart_grid)       AS depart_grid,
                            any_value(stops)             AS stops
                        FROM legs
                        GROUP BY osm_id
                    ) TO '{out}' (FORMAT 'parquet')
                """)
                _n = con2.sql(
                    f"SELECT count(*) FROM read_parquet('{out}')"
                ).fetchone()[0]
                con2.close()
                # The GROUP BY osm_id must be 1:1 with the input journeys
                # — a mismatch means osm_id is not unique and journeys
                # were merged/lost (see the theme-prefix above).
                if _n != len(journeys):
                    raise RuntimeError(
                        "compute_route_network: osm_id collision — "
                        f"{len(journeys)} journeys in, {_n} rows out"
                    )
                print(
                    "[compute_route_network] wrote "
                    f"{_n} routehub journeys ({out.stat().st_size // 1024}"
                    f" KiB) -> {out}"
                )
                return str(out)

            @task
            def freestiler_routehub_convert(routehub_paths: str) -> str:
                # Bake the hub-decomposition routing network to PMTiles —
                # the same freestiler -> PMTiles path every other on-map
                # dataset uses. The route-builder cell consumes it as the
                # `austria-routehub` vector source.
                import freestiler
                TILES.mkdir(parents=True, exist_ok=True)
                out = TILES / "austria-routehub.pmtiles"
                if not _needs_regen(out):
                    return str(out)
                query = f"""
                    SELECT osm_id,
                           geometry,
                           theme,
                           origin_station_id,
                           dest_station_id,
                           travel_min,
                           n_transfers,
                           depart_hhmm,
                           arrive_hhmm,
                           depart_grid,
                           stops
                    FROM read_parquet('{routehub_paths}')
                """
                # z0-only: this tile is a "load the whole dataset"
                # delivery channel, not a spatial map layer. One z0 tile
                # holds every journey, is always loaded at any display
                # zoom (the map sets source maxzoom 0 so MapLibre
                # overzooms it), and querySourceFeatures sees them all.
                # Baking z0-10 with full polylines ballooned the archive
                # to 440 MB via per-zoom tile-crossing line replication.
                freestiler.freestile_query(
                    query=query,
                    output=str(out),
                    layer_name="austria-routehub",
                    min_zoom=0,
                    max_zoom=0,
                    drop_rate=None,
                    simplification=True,
                    coalesce=False,
                )
                return str(out)

            @task
            def reload_martin(pmtiles_paths: list) -> list:
                # Reload martin so it picks up the freshly-baked PMTiles
                # (austria-transit + austria-chrono + austria-fastlink +
                # austria-routehub). Uses the same flock + readiness-probe
                # primitives as the OSM DAG's reload_martin task — and
                # takes a LIST of paths, exactly like that task does, so
                # the two reload surfaces no longer diverge (R3). The OSM
                # DAG runs its own reload_martin first (over the OSM-side
                # tiles); this reload picks up THIS DAG's transit + chrono
                # + fastlink + routehub tiles. Serialized restarts are
                # fine — flock keeps them ordered, /catalog membership
                # verifies end-state.
                import fcntl
                import json as _json
                import socket
                import subprocess
                import time as _time
                import urllib.request
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
                missing = [s for s in expected if s not in available]
                if missing:
                    raise RuntimeError(
                        f"martin /catalog missing sources {missing} "
                        f"after reload; available={available}",
                    )
                return pmtiles_paths

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
            #          ↘ freestiler_transit_convert ───────────────┐
            #          ↘ compute_chrono_isochrones ────────────────┤
            #               → freestiler_chrono_convert ───────────┤
            #               → compute_fastest_connections          │
            #                    → freestiler_fastlink_convert ────┤
            #                                          → reload_martin
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
            transit_tile = freestiler_transit_convert(stops_task)
            # Chronomap isochrones: a time-dependent CSA over the real GTFS
            # timetable. Reads austria.duckdb READ-ONLY but is ORDERED AFTER
            # trips_task so every match_* writer has closed its write
            # connection first — DuckDB forbids a read-only open from one
            # process while another holds the file open read-write.
            # Deterministic ordering, not a sleep (R4).
            chrono_isochrones = compute_chrono_isochrones(db)
            trips_task >> chrono_isochrones
            chrono_tile = freestiler_chrono_convert(chrono_isochrones)
            # Fastest hub→hub connections + the hub-decomposition routing
            # network (hub→station, station→hub, hub→hub profile): both
            # backtrack the CSA's predecessor chain (no CSA, no GPU) over
            # the intermediates compute_chrono_isochrones persisted —
            # hence ORDERED after it.
            fastlinks = compute_fastest_connections(chrono_isochrones)
            fastlink_tile = freestiler_fastlink_convert(fastlinks)
            route_net = compute_route_network(chrono_isochrones)
            routehub_tile = freestiler_routehub_convert(route_net)
            # ONE reload picks up all four freshly-baked tiles.
            reload_martin([transit_tile, chrono_tile, fastlink_tile,
                           routehub_tile])


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
    source_maxzoom: int = 12,
    terrain: bool = False,
    satellite_background: bool = False,
    pitch: int = 0,
    max_pitch: int = 60,
    hillshade: bool = True,
    glyphs_url: str | None = None,
    extra_js: str | None = None,
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

    `extra_js` is an optional block of JavaScript appended inside the
    map's <script> immediately after the `window.map_<var>` hook. The
    map instance is in scope as `map_<var>` (also on `window`). Used
    by the chronomap cell to wire a station-click handler that
    re-filters the isochrone band layers. Default `None` preserves
    byte-identical output for every existing caller.
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
        # Force source/source-layer to this cell's `src` ONLY for layers
        # that use the default `src` source. A layer that explicitly names
        # another source (e.g. a second martin tier passed via
        # extra_sources, like the satellite-overlay map's `paths-src`) is
        # left untouched so multi-source styles work. The copy above keeps
        # this non-mutating even for shared module-level style constants.
        if _layer.get("type") != "background" and _layer.get("source", "src") == "src":
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
    # `source_maxzoom` MUST match the highest zoom freestiler baked into
    # this source's PMTiles archive. MapLibre AUTO-OVERZOOMS for display
    # zoom > source_maxzoom — renders the top-zoom vector features
    # upscaled. Without an accurate hint MapLibre fetches non-existent
    # higher-zoom tiles from martin, gets 4xx, and returns 0 features at
    # high zoom. Every caller passes the value matching its own source
    # (e.g. austria-rail / austria-routes / austria-paths = 14,
    # austria-ecovoyage = 12).
    source_dict = {
        "type": "vector",
        "url": f"{martin}/{source_name}",
        "maxzoom": source_maxzoom,
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

    # Optional caller-supplied JavaScript, appended after the window
    # hook so the `map_<var>` instance is already in scope.
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
    # for funicular, etc.), plus ART_HUB for the neutral GTFS station
    # dot and ART_FERRY for OSM ferry routes.
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

    # GTFS station overlay + OSM ferry routes
    ART_HUB             = "#2b2b2b"  # GTFS station dot (neutral, mode-agnostic)
    ART_FERRY           = "#3a6a9a"  # ferry / ship routes (deep blue-grey)

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

    # ---- TRANSIT_STYLE — GTFS station overlay -----------------------
    # The GTFS feed resolves platform-granularity stops up to parent
    # stations (transit.station_members). Exactly ONE representative
    # member per station carries is_station_label='true'; every layer in
    # this style keys off that flag, so a multi-platform station shows a
    # single dot + a single name — not the ~7,600 platform circles the
    # former per-mode circle layers drew at every zoom (removed
    # 2026-05-14: they were the dominant paint cost and pure clutter at
    # low/mid zoom).
    #
    # transit-station-dot: neutral mode-agnostic marker (ART_HUB) — "a
    # GTFS-served station is here". Deliberately distinct from the
    # k.k.-red OSM railway=station circles (sat-rail-station) so the two
    # datasets stay visually separable. transit-stops-label: the station
    # name in ART_BLACK + ART_HALO halo (versatiles-glyphs-rs SDF fonts
    # via the helper's `glyphs_url` kwarg), disclosed progressively by
    # hub_rank as zoom increases — top transfer hubs first, minor stops
    # on zoom-in. symbol-sort-key = hub_rank gives MapLibre's collision
    # solver the same priority order within each eligible set.

    # Progressive label disclosure: a station's name shows once its
    # hub_rank clears the per-zoom threshold below. hub_rank rides the
    # tile as a string (parquet COPY), so `to-number` first. 999999 at
    # z>=13 admits every station (no-score stations default to 999999).
    _HUB_RANK_STEP = ["step", ["zoom"],
                      15,        # z6-7  : top 15 hubs only
                      8,  50,    # z8-9  : top 50
                      10, 150,   # z10-11: top 150
                      12, 400,   # z12   : top 400
                      13, 999999]  # z13+ : all stations

    TRANSIT_STYLE = [
        # One dot per GTFS parent station (is_station_label='true'),
        # disclosed progressively by hub_rank — the SAME _HUB_RANK_STEP
        # gate as the label below, so a station's dot and name appear
        # together. Without the gate, ~1,500 dots render at country zoom
        # (measured) — the step keeps it to the top ~15 hubs there.
        {"id": "transit-station-dot", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "is_station_label"], "true"],
                    ["<=", ["to-number", ["get", "hub_rank"]],
                     _HUB_RANK_STEP]],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              6, 1.6, 10, 2.6, 14, 4.0, 18, 5.5],
            "circle-color": ART_HUB,
            "circle-stroke-color": ART_HALO,
            "circle-stroke-width": 1.2,
            "circle-opacity": 0.95,
         }},
        # Parent-station names — progressive by hub_rank.
        {"id": "transit-stops-label", "type": "symbol",
         "source": "transit-src", "source-layer": "austria-transit",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "is_station_label"], "true"],
                    ["<=", ["to-number", ["get", "hub_rank"]],
                     _HUB_RANK_STEP]],
         "layout": {
            "symbol-sort-key": ["to-number", ["get", "hub_rank"]],
            "text-field": ["get", "station_name"],
            "text-font": ["noto_sans_regular"],
            "text-size": [
                "interpolate", ["linear"], ["zoom"],
                6, 9,
                10, 10,
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
    # Reads from THREE importance-tiered line/point-only OSM martin
    # sources (built by osm-austria.py with NO random drop_rate — see
    # that notebook). Each tier is only built for the zooms it is needed
    # at, so every zoom level loads the minimum:
    #   * `src` = austria-rail — railways + aerialways + ferries. The
    #     continuity headline, carried at EVERY zoom. Layers reading it
    #     keep `"source": "src"` (the helper force-sets src/source-layer).
    #   * `routes-src` = austria-routes — long-distance hiking + cycle
    #     routes, built z6+. Layers carry an EXPLICIT `"source":
    #     "routes-src"` (sat-hike-route* / sat-cycle-route*).
    #   * `paths-src` = austria-paths — the bulk walkable-street /
    #     cycleway / SAC-trail context, built z12+ only. Layers carry an
    #     EXPLICIT `"source": "paths-src"` (sat-walk-* / sat-hike-trail-*
    #     / sat-cycleway). Layers with an explicit non-`src` source are
    #     left untouched by the helper.
    # All three pmtiles use a `theme` discriminator (railway / cycle /
    # hiking / topo) — every layer's filter anchors `theme` first.
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
    #   9.  Ferry routes (ship-blue, dashed) — modes with no GTFS data
    #   10. Rail aux: disused / construction / tunnel / service
    #   11. Aerialway / cable-car (violet light, dotted) — ropeways
    #   12. Narrow-gauge (grey-dark, fine stipple)
    #   13. Funicular (violet, dot-dash) — with halo
    #   14. Tram (k.u. green) — with halo
    #   15. Light rail / S-Bahn (k.u. green light) — with halo
    #   16. Subway / U-Bahn (k.u. green dark) — with halo
    #   17. Branch rail (k.k. red dark) — with halo
    #   18. Mainline rail (k.k. red) — with halo + z14+ centre stripe
    #   19. Rail station + halt circles (red dots)
    #
    # GTFS station overlay (TRANSIT_STYLE — one dot + progressive name
    # label per parent station) gets APPENDED via the helper's
    # `extra_layers` parameter, so it rides at the very top of the
    # visual stack. Station NAMES come exclusively from there.
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
        # zooms ≥12. Continuity backstop + contrast edge. Reads the
        # austria-paths tier (bulk highway network, built z12+).
        {"id": "sat-walk-casing", "type": "line",
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "paths-src", "source-layer": "austria-paths",
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "routes-src", "source-layer": "austria-routes",
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
         "source": "routes-src", "source-layer": "austria-routes",
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
         "source": "paths-src", "source-layer": "austria-paths",
         "minzoom": 12,
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
         "source": "routes-src", "source-layer": "austria-routes",
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
         "source": "routes-src", "source-layer": "austria-routes",
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
        # === FERRY ROUTES (ships — no GTFS data, OSM-only)          ===
        # ============================================================
        # route=ferry ways land in the topo theme and are routed by
        # _RAIL_PRED into the austria-rail tier (`src`). Ship-blue dashed
        # line over a white halo casing, drawn from z6 like the other
        # long-distance route tiers — ferries are notable transport
        # connections and the Austria railway GTFS feed never carries them.
        {"id": "sat-ferry-route-casing", "type": "line",
         "source": "src", "source-layer": "austria-rail",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["get", "theme"], "topo"],
                    ["==", ["get", "route"], "ferry"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_HALO,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_LONGDIST_CASING],
            "line-opacity": 0.85,
         }},
        {"id": "sat-ferry-route", "type": "line",
         "source": "src", "source-layer": "austria-rail",
         "minzoom": 6,
         "filter": ["all",
                    ["==", ["get", "theme"], "topo"],
                    ["==", ["get", "route"], "ferry"]],
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": ART_FERRY,
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           *W_LONGDIST],
            "line-dasharray": DASH_LONG,
         }},

        # ============================================================
        # === RAILWAY AUXILIARY (disused / construction / tunnel /  ===
        # === service — drawn before primary tiers)                  ===
        # ============================================================

        # Disused / abandoned / razed rail — grey-dark dashed
        {"id": "sat-rail-disused", "type": "line",
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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

        # Aerialway / cable-car / gondola — ropeways (no GTFS data, so
        # they only exist on the OSM side). Aerialway features are
        # tagged theme=topo (NOT railway) but _RAIL_PRED routes them
        # into the austria-rail tier (`src`); the filter matches
        # theme=topo. RCA 2026-05-14.
        {"id": "sat-aerialway", "type": "line",
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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
         "source": "src", "source-layer": "austria-rail",
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

        # Station NAMES are not labelled from the OSM source here — the
        # GTFS-driven transit-stops-label layer (TRANSIT_STYLE) is the
        # single station-labelling system, with hub_rank progressive
        # disclosure the OSM railway=station set has no equivalent of.
        # sat-rail-station / sat-rail-halt above still draw the OSM
        # infrastructure dots.
    ]
    # ---- CHRONO_STYLE — chronotrains-style travel-time isochrones ----
    # Style for the chronomap cell. TWELVE travel-time bands (every hour
    # to <=12h) computed from the real GTFS timetable by the GTFS DAG's
    # compute_chrono_isochrones task and baked to the `austria-chrono`
    # martin source. The compute task emits each band as a RING
    # (hull(k) minus hull(k-1)), so every pixel carries exactly ONE band
    # colour and a healthy fill-opacity stays legible — no muddy
    # 12-layer translucent stack.
    #
    # The chronomap cell passes `source_name="austria-chrono"`, so the
    # helper's single `src` source IS austria-chrono; these layers use
    # `"source": "src"` and the helper force-sets source-layer to
    # `austria-chrono` for them.
    #
    # DRAW ORDER: band 12 (largest) is appended FIRST so it sits
    # visually UNDERNEATH band 1 (smallest) — concentric zones,
    # innermost on top. Each band's filter starts pinned to
    # origin_station_id == "" (matches nothing) so the map opens clean;
    # the cell's click handler (extra_js) rewrites the origin_station_id
    # term to the clicked station's feature id.
    #
    # 12-step ramp: RdYlGn reversed (green = nearest) + one darker red.
    _CHRONO_BAND_COLORS = {
        1: "#006837", 2: "#1a9850", 3: "#66bd63", 4: "#a6d96a",
        5: "#d9ef8b", 6: "#ffffbf", 7: "#fee08b", 8: "#fdae61",
        9: "#f46d43", 10: "#d73027", 11: "#a50026", 12: "#6d0026",
    }
    CHRONO_STYLE = []
    for _band in range(12, 0, -1):   # band 12 first → drawn underneath
        CHRONO_STYLE.append({
            "id": f"chrono-band-{_band}", "type": "fill",
            "source": "src", "source-layer": "austria-chrono",
            "filter": ["all",
                       ["==", ["get", "theme"], "chrono"],
                       ["==", ["get", "band_hours"], str(_band)],
                       ["==", ["get", "origin_station_id"], ""]],
            "paint": {
                "fill-color": _CHRONO_BAND_COLORS[_band],
                "fill-opacity": 0.5,
                "fill-outline-color": "#33333a",
            }})

    # ---- CHRONO_ORIGIN_STYLE — the clickable origin markers ----------
    # The compute task also emits one POINT per origin station
    # (theme='chrono-origin'). These markers are the ONLY station
    # markers on the chronomap, so every marker the user sees IS a
    # station with a precomputed chronomap — "easily identifiable and
    # clickable" by construction. `chrono-origin-selected` highlights
    # the currently-chosen origin (filter rewritten by the click
    # handler). Drawn ON TOP of the bands via the cell's `extra_layers`.
    CHRONO_ORIGIN_STYLE = [
        {"id": "chrono-origin", "type": "circle",
         "source": "src", "source-layer": "austria-chrono",
         "filter": ["==", ["get", "theme"], "chrono-origin"],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              3, 3.5, 7, 5.5, 11, 8, 14, 10],
            "circle-color": "#ffffff",
            "circle-stroke-color": "#1b3a5c",
            "circle-stroke-width": 2.4,
            "circle-opacity": 1.0,
         }},
        {"id": "chrono-origin-selected", "type": "circle",
         "source": "src", "source-layer": "austria-chrono",
         "filter": ["all",
                    ["==", ["get", "theme"], "chrono-origin"],
                    ["==", ["get", "origin_station_id"], ""]],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              3, 5.5, 7, 8, 11, 11, 14, 13],
            "circle-color": "#ffcc00",
            "circle-stroke-color": "#1b3a5c",
            "circle-stroke-width": 3.0,
            "circle-opacity": 1.0,
         }},
        {"id": "chrono-origin-label", "type": "symbol",
         "source": "src", "source-layer": "austria-chrono",
         "filter": ["==", ["get", "theme"], "chrono-origin"],
         "layout": {
            "text-field": ["get", "origin_name"],
            "text-font": ["noto_sans_regular"],
            "text-size": ["interpolate", ["linear"], ["zoom"],
                          4, 9, 8, 11, 12, 13],
            "text-anchor": "top",
            "text-offset": [0, 0.8],
            "text-padding": 2,
         },
         "paint": {
            "text-color": "#1a1a1a",
            "text-halo-color": "#ffffff",
            "text-halo-width": 1.6,
            "text-halo-blur": 0.4,
         }},
    ]

    # ---- FASTLINK_STYLE — fastest hub→hub connections ----------------
    # Self-contained style for the one `austria-fastlink` source (the
    # fastest-connections map passes source_name="austria-fastlink", so
    # the helper's `src` IS that tile; layers use "source": "src"). The
    # tile carries two feature kinds via `theme`:
    #   * theme='fastlink'        — one LineString per (origin, dest)
    #     journey, routed through every called station, coloured by
    #     travel time. White casing underneath for legibility on
    #     satellite.
    #   * theme='fastlink-origin' — one POINT per top hub: the ONLY
    #     markers on the map, so every marker is a clickable origin
    #     (mirrors CHRONO_ORIGIN_STYLE). `fastlink-origin-selected`
    #     highlights the chosen one.
    # All filters open pinned to origin_station_id == "" (map opens
    # clean); the click handler (extra_js) rewrites them to the clicked
    # origin. travel_min rides the tile as a string → `to-number` first.
    _FASTLINK_FILTER = ["all",
                        ["==", ["get", "theme"], "fastlink"],
                        ["==", ["get", "origin_station_id"], ""]]
    FASTLINK_STYLE = [
        {"id": "fastlink-line-casing", "type": "line",
         "source": "src", "source-layer": "austria-fastlink",
         "filter": _FASTLINK_FILTER,
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": "#ffffff",
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           3, 3.0, 7, 5.0, 11, 7.5],
            "line-opacity": 0.7,
         }},
        {"id": "fastlink-line", "type": "line",
         "source": "src", "source-layer": "austria-fastlink",
         "filter": _FASTLINK_FILTER,
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            # green (fast) → red (slow), by travel time in minutes
            "line-color": ["interpolate", ["linear"],
                           ["to-number", ["get", "travel_min"]],
                           0,   "#1a9850",
                           60,  "#a6d96a",
                           120, "#fee08b",
                           240, "#fdae61",
                           360, "#d73027",
                           600, "#6d0026"],
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           3, 1.4, 7, 2.6, 11, 4.0],
            "line-opacity": 0.95,
         }},
        {"id": "fastlink-origin", "type": "circle",
         "source": "src", "source-layer": "austria-fastlink",
         "filter": ["==", ["get", "theme"], "fastlink-origin"],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              3, 3.5, 7, 5.5, 11, 8, 14, 10],
            "circle-color": "#ffffff",
            "circle-stroke-color": "#1b3a5c",
            "circle-stroke-width": 2.4,
            "circle-opacity": 1.0,
         }},
        {"id": "fastlink-origin-selected", "type": "circle",
         "source": "src", "source-layer": "austria-fastlink",
         "filter": ["all",
                    ["==", ["get", "theme"], "fastlink-origin"],
                    ["==", ["get", "origin_station_id"], ""]],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              3, 5.5, 7, 8, 11, 11, 14, 13],
            "circle-color": "#ffcc00",
            "circle-stroke-color": "#1b3a5c",
            "circle-stroke-width": 3.0,
            "circle-opacity": 1.0,
         }},
        {"id": "fastlink-origin-label", "type": "symbol",
         "source": "src", "source-layer": "austria-fastlink",
         "filter": ["==", ["get", "theme"], "fastlink-origin"],
         "layout": {
            "text-field": ["get", "origin_name"],
            "text-font": ["noto_sans_regular"],
            "text-size": ["interpolate", ["linear"], ["zoom"],
                          4, 9, 8, 11, 12, 13],
            "text-anchor": "top",
            "text-offset": [0, 0.8],
            "text-padding": 2,
         },
         "paint": {
            "text-color": "#1a1a1a",
            "text-halo-color": "#ffffff",
            "text-halo-width": 1.6,
            "text-halo-blur": 0.4,
         }},
    ]

    # ---- ROUTEBUILD_STYLE — the build-your-own-route map -------------
    # The route-builder cell passes source_name="austria-routehub" (the
    # hub-decomposition routing network) and extra_sources for the
    # `austria-transit` station dots + two client-side GeoJSON sources
    # (`route-src` = the stitched route, `pick-src` = the picked
    # waypoints) which _ROUTEBUILD_JS keeps up to date.
    #   * routehub-loader   — an always-false-filtered line on the
    #     `austria-routehub` source: renders nothing, but forces martin
    #     to load the source's tiles so querySourceFeatures can read the
    #     journey `stops` lists client-side.
    #   * routebuild-station-dot / -label — every parent station (the
    #     `austria-transit` is_station_label points), the click targets;
    #     NO hub_rank gate so any station is pickable.
    #   * route-leg-casing / route-leg — the stitched route, from the
    #     client `route-src` GeoJSON.
    #   * route-pick / route-pick-label — the numbered picked waypoints,
    #     from the client `pick-src` GeoJSON.
    ROUTEBUILD_STYLE = [
        {"id": "routehub-loader", "type": "line",
         "source": "src", "source-layer": "austria-routehub",
         "filter": ["==", ["get", "theme"], ""],
         "paint": {"line-opacity": 0.0}},
        {"id": "routebuild-station-dot", "type": "circle",
         "source": "transit-src", "source-layer": "austria-transit",
         "minzoom": 7,
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "is_station_label"], "true"]],
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              7, 2.4, 11, 4.0, 14, 5.5],
            "circle-color": "#ffffff",
            "circle-stroke-color": "#1b3a5c",
            "circle-stroke-width": 1.3,
            "circle-opacity": 0.95,
         }},
        {"id": "routebuild-station-label", "type": "symbol",
         "source": "transit-src", "source-layer": "austria-transit",
         "minzoom": 9,
         "filter": ["all",
                    ["==", ["geometry-type"], "Point"],
                    ["==", ["get", "is_station_label"], "true"]],
         "layout": {
            "text-field": ["get", "station_name"],
            "text-font": ["noto_sans_regular"],
            "text-size": ["interpolate", ["linear"], ["zoom"],
                          9, 9, 13, 12],
            "text-anchor": "top",
            "text-offset": [0, 0.6],
            "text-padding": 2,
         },
         "paint": {
            "text-color": "#1a1a1a",
            "text-halo-color": "#ffffff",
            "text-halo-width": 1.4,
            "text-halo-blur": 0.4,
         }},
        {"id": "route-leg-casing", "type": "line",
         "source": "route-src",
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": "#ffffff",
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           3, 4.0, 7, 6.5, 11, 9.0],
            "line-opacity": 0.8,
         }},
        {"id": "route-leg", "type": "line",
         "source": "route-src",
         "layout": {"line-join": "round", "line-cap": "round"},
         "paint": {
            "line-color": "#1b5fa8",
            "line-width": ["interpolate", ["linear"], ["zoom"],
                           3, 2.0, 7, 3.4, 11, 5.0],
            "line-opacity": 0.95,
         }},
        {"id": "route-pick", "type": "circle",
         "source": "pick-src",
         "paint": {
            "circle-radius": ["interpolate", ["linear"], ["zoom"],
                              3, 6, 7, 9, 11, 12, 14, 14],
            "circle-color": "#ffcc00",
            "circle-stroke-color": "#1b3a5c",
            "circle-stroke-width": 2.6,
            "circle-opacity": 1.0,
         }},
        {"id": "route-pick-label", "type": "symbol",
         "source": "pick-src",
         "layout": {
            "text-field": ["get", "order"],
            "text-font": ["noto_sans_regular"],
            "text-size": ["interpolate", ["linear"], ["zoom"],
                          3, 9, 7, 11, 14, 14],
            "text-allow-overlap": True,
         },
         "paint": {
            "text-color": "#1b3a5c",
         }},
    ]
    return (
        CHRONO_ORIGIN_STYLE,
        CHRONO_STYLE,
        CYCLE_STYLE,
        FASTLINK_STYLE,
        HIKING_STYLE,
        RAILWAY_STYLE,
        ROUTEBUILD_STYLE,
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
    # (reload_martin) only succeeds after materialize_duckdb
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
            source_maxzoom=14,
            extra_sources={
                "transit-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-transit",
                    "maxzoom": 14,
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
                    "maxzoom": 14,
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
    # This map reads THREE importance-tiered line/point-only OSM sources
    # built by osm-austria.py (NO random drop_rate — see
    # SATELLITE_OVERLAY_STYLE), so each zoom loads only what it needs:
    #   * `src`        = austria-rail — railways + aerialways + ferries;
    #     carried at EVERY zoom so the rail headline never fragments.
    #     Passed as the helper's `source_name`.
    #   * `routes-src` = austria-routes — long-distance hiking + cycle
    #     routes, built z6+. Passed via `extra_sources`.
    #   * `paths-src`  = austria-paths — the bulk walkable-street /
    #     cycleway / SAC-trail context, built z12+ only. Passed via
    #     `extra_sources`.
    # All baked to z14, so `source_maxzoom=14` for `src` and
    # `"maxzoom": 14` on the routes-src / paths-src entries. Ferries +
    # ropeways live in austria-rail — modes with no GTFS data — so ships
    # and ropeways still show.
    #
    # GTFS stops overlay (`TRANSIT_STYLE`): one neutral dot + one
    # progressively-disclosed name per parent station (both gated by
    # hub_rank) — NOT the former ~7,600 per-platform circles. The
    # `austria-transit` source is baked to z14 (`"maxzoom": 14`).
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
    _osm_line_tiles = [
        Path("/workspace/tiles/pmtiles/austria-rail.pmtiles"),
        Path("/workspace/tiles/pmtiles/austria-routes.pmtiles"),
        Path("/workspace/tiles/pmtiles/austria-paths.pmtiles"),
    ]
    mo.stop(
        any(not p.exists() or p.stat().st_size == 0 for p in _osm_line_tiles),
        "`austria-rail` / `austria-routes` / `austria-paths` .pmtiles "
        "not yet present — open and run `osm-austria.py` first. Its OSM "
        "DAG produces them via the `freestiler_rail_convert`, "
        "`freestiler_routes_convert` and `freestiler_paths_convert` tasks.",
    )
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-rail",
            layer_name="austria-rail",
            center=[13.3, 47.7],
            zoom=7,
            style_layers=SATELLITE_OVERLAY_STYLE,
            source_maxzoom=14,
            extra_sources={
                "routes-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-routes",
                    "maxzoom": 14,
                },
                "paths-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-paths",
                    "maxzoom": 14,
                },
                "transit-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-transit",
                    "maxzoom": 14,
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
def _(mo):
    mo.md("""
    ## Chronomap — how far by train, hour by hour

    A [chronotrains](https://github.com/benjamintd/chronotrains)-style
    per-station reachability overlay: **click a marked station** and
    concentric **1–12 h** travel-time bands radiate outward, shading
    every region you can reach from there within each hour. The only
    markers on this map are the stations that *have* a precomputed
    chronomap — every marker is clickable; the chosen one turns gold.

    Unlike chronotrains — which builds a static station graph with a
    9 km/h "short-hop" assumption and a flat 20-minute interchange
    penalty — this map is computed from the **real Austria railway
    GTFS timetable**. The GTFS DAG's `compute_chrono_isochrones` task
    runs a time-dependent **Connection Scan Algorithm** earliest-
    arrival search: every edge is an actual scheduled connection, so
    multi-hop journeys, the actual ride times *and* the real waiting
    time between transfers all come straight from `gtfs.stop_times` —
    you board the next service that really departs.

    The search is a **smart multipath** relaxation: all top-25 origins
    are routed simultaneously, and each iteration relaxes only from the
    frontier (the labels that just improved) rather than re-scanning
    the whole state — a vectorised **polars** join/group-by loop on the
    **GPU** (cudf-polars) when the host has one. The connection graph is
    the **union timetable** — every trip the feed ships, across all
    service patterns — so a station served on ANY service day is
    reachable. From each of the **top 25 transfer hubs** (by `hub_rank`,
    routed from an 08:00 reference departure) it reaches **every other
    station in the network** within 12 h, then bakes the per-band
    isochrone rings + origin markers to the `austria-chrono` PMTiles
    archive — exactly like every other dataset on these maps.
    """)
    return


@app.cell
def _(
    CHRONO_ORIGIN_STYLE,
    CHRONO_STYLE,
    Path,
    dag_run_states,
    martin,
    mo,
    versatiles_assets,
):
    # Chronomap cell — chronotrains-style per-station travel-time
    # isochrones over the REAL GTFS timetable.
    #
    # ONE martin source: `austria-chrono` (baked z0-10 by the GTFS DAG's
    # compute_chrono_isochrones + freestiler_chrono_convert tasks). It
    # carries two feature kinds, discriminated by `theme`:
    #   * theme='chrono'        — the 12 isochrone RINGS per origin,
    #     rendered by CHRONO_STYLE (passed as `style_layers`, drawn
    #     UNDERNEATH).
    #   * theme='chrono-origin' — one POINT per origin station, rendered
    #     by CHRONO_ORIGIN_STYLE (passed as `extra_layers`, drawn ON TOP)
    #     — the ONLY markers on the map, so every marker is a clickable
    #     station with a chronomap.
    #
    # source_name="austria-chrono" → the helper's single `src` source IS
    # austria-chrono, the in-page map instance is `map_austria_chrono`
    # and the container is `map-austria-chrono` — the names
    # `_CHRONO_CLICK_JS` (passed via the helper's `extra_js` kwarg)
    # reaches for. The band layers open filtered to origin_station_id ==
    # "" (nothing); the click handler rewrites that to the clicked
    # origin and highlights it via `chrono-origin-selected`.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    _chrono_pmtiles = Path("/workspace/tiles/pmtiles/austria-chrono.pmtiles")
    mo.stop(
        not _chrono_pmtiles.exists() or _chrono_pmtiles.stat().st_size == 0,
        "`austria-chrono.pmtiles` not yet present — the GTFS DAG's "
        "`compute_chrono_isochrones` + `freestiler_chrono_convert` tasks "
        "produce it (re-run the DAG if this notebook predates them).",
    )
    # Origin-click handler + 12-band legend, injected via
    # build_pipeline_maplibre_html's `extra_js` kwarg. Coupled to
    # source_name="austria-chrono" (hence map var `map_austria_chrono` /
    # container `map-austria-chrono`) and to the chrono-band-* /
    # chrono-origin-selected layer ids from CHRONO_STYLE /
    # CHRONO_ORIGIN_STYLE.
    _CHRONO_CLICK_JS = """
    (function () {
      var M = map_austria_chrono;
      var COLORS = ['#006837','#1a9850','#66bd63','#a6d96a','#d9ef8b',
                    '#ffffbf','#fee08b','#fdae61','#f46d43','#d73027',
                    '#a50026','#6d0026'];
      M.on('load', function () {
    var box = document.getElementById('map-austria-chrono');
    box.style.position = 'relative';
    var leg = document.createElement('div');
    leg.style.cssText = 'position:absolute;left:8px;bottom:8px;z-index:2;'
      + 'background:rgba(255,255,255,0.9);padding:6px 9px;'
      + 'border-radius:4px;font:11px/1.45 sans-serif;color:#222;'
      + 'box-shadow:0 1px 4px rgba(0,0,0,0.35);';
    var html = '<b>Reachable by train</b>'
      + '<div id="chrono-hint" style="color:#666;">'
      + 'click a marked station</div>';
    for (var i = 0; i < 12; i++) {
      html += '<div><span style="display:inline-block;width:11px;'
        + 'height:11px;margin-right:5px;background:' + COLORS[i]
        + ';"></span>&le; ' + (i + 1) + ' h</div>';
    }
    leg.innerHTML = html;
    box.appendChild(leg);
      });
      function applyOrigin(fid, name) {
    for (var b = 1; b <= 12; b++) {
      M.setFilter('chrono-band-' + b, ['all',
        ['==', ['get', 'theme'], 'chrono'],
        ['==', ['get', 'band_hours'], String(b)],
        ['==', ['get', 'origin_station_id'], fid]]);
    }
    M.setFilter('chrono-origin-selected', ['all',
      ['==', ['get', 'theme'], 'chrono-origin'],
      ['==', ['get', 'origin_station_id'], fid]]);
    var hint = document.getElementById('chrono-hint');
    if (hint) { hint.textContent = 'from: ' + (name || fid); }
      }
      M.on('click', 'chrono-origin', function (e) {
    if (!e.features || !e.features.length) { return; }
    var p = e.features[0].properties || {};
    applyOrigin(p.origin_station_id, p.origin_name);
      });
      M.on('mouseenter', 'chrono-origin', function () {
    M.getCanvas().style.cursor = 'pointer';
      });
      M.on('mouseleave', 'chrono-origin', function () {
    M.getCanvas().style.cursor = '';
      });
    })();
    """
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-chrono",
            layer_name="austria-chrono",
            center=[13.34, 47.6],
            zoom=7,
            style_layers=CHRONO_STYLE,
            source_maxzoom=10,
            extra_layers=CHRONO_ORIGIN_STYLE,
            satellite_background=True,
            terrain=True,
            hillshade=False,
            pitch=0,
            max_pitch=85,
            glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
            extra_js=_CHRONO_CLICK_JS,
        ),
        height="500px",
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Fastest connections — hub to hub

    The complement to the chronomap: the **fastest journey between the
    top 25 hub stations**, drawn as the actual route it takes through
    every intermediate station it calls at. **Click a hub marker** and
    its fastest connection to each of the other 24 hubs lights up,
    coloured by travel time; **hover a line** for the travel time and
    transfer count.

    The chronomap's Connection Scan Algorithm already found the
    earliest arrival from each hub to every station — this view adds
    *predecessor tracking* to that CSA (which trip carried you into
    each station, and where you boarded it), then **backtracks** the
    predecessor chain for every hub→hub pair to reconstruct the full
    journey: the sequence of called stations, and the transfer count
    (one per change of trip). No second routing pass — it reuses the
    same real-timetable CSA result, then bakes the journeys to the
    `austria-fastlink` PMTiles archive.
    """)
    return


@app.cell
def _(FASTLINK_STYLE, Path, dag_run_states, martin, mo, versatiles_assets):
    # Fastest-connections cell — the fastest hub→hub journeys, drawn as
    # the route through their called stations.
    #
    # ONE martin source: `austria-fastlink` (baked z0-10 by the GTFS
    # DAG's compute_fastest_connections + freestiler_fastlink_convert
    # tasks). Two feature kinds via `theme`:
    #   * theme='fastlink'        — one LineString per (origin, dest)
    #     journey, rendered by FASTLINK_STYLE's line + casing layers.
    #   * theme='fastlink-origin' — one POINT per top hub, rendered by
    #     FASTLINK_STYLE's marker layers — the ONLY markers, so every
    #     marker is a clickable origin.
    #
    # source_name="austria-fastlink" → the helper's `src` IS that tile,
    # the map instance is `map_austria_fastlink` / container
    # `map-austria-fastlink` (the names `_FASTLINK_CLICK_JS` reaches
    # for). All line/selected filters open pinned to
    # origin_station_id == "" (nothing); the click handler rewrites
    # them to the clicked origin.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    _fastlink_pmtiles = Path(
        "/workspace/tiles/pmtiles/austria-fastlink.pmtiles"
    )
    mo.stop(
        not _fastlink_pmtiles.exists()
        or _fastlink_pmtiles.stat().st_size == 0,
        "`austria-fastlink.pmtiles` not yet present — the GTFS DAG's "
        "`compute_fastest_connections` + `freestiler_fastlink_convert` "
        "tasks produce it (re-run the DAG if this notebook predates "
        "them).",
    )
    # Origin-click handler + travel-time legend + hover summary popup +
    # a connection-LINE click handler that opens a fixed itinerary
    # panel (the journey's exact leg-by-leg clock times, parsed from
    # the feature's `itinerary` JSON property). Injected via
    # build_pipeline_maplibre_html's `extra_js` kwarg; coupled to
    # source_name="austria-fastlink" (map var `map_austria_fastlink` /
    # container `map-austria-fastlink`) and the fastlink-* layer ids
    # from FASTLINK_STYLE.
    _FASTLINK_CLICK_JS = """
    (function () {
      var M = map_austria_fastlink;
      var STOPS = [['#1a9850','0h'], ['#a6d96a','1h'], ['#fee08b','2h'],
                   ['#fdae61','4h'], ['#d73027','6h'], ['#6d0026','10h']];
      M.on('load', function () {
    var box = document.getElementById('map-austria-fastlink');
    box.style.position = 'relative';
    var leg = document.createElement('div');
    leg.style.cssText = 'position:absolute;left:8px;bottom:8px;z-index:2;'
      + 'background:rgba(255,255,255,0.9);padding:6px 9px;'
      + 'border-radius:4px;font:11px/1.45 sans-serif;color:#222;'
      + 'box-shadow:0 1px 4px rgba(0,0,0,0.35);';
    var html = '<b>Fastest train connection</b>'
      + '<div id="fastlink-hint" style="color:#666;">'
      + 'click a hub marker</div>';
    for (var i = 0; i < STOPS.length; i++) {
      html += '<div><span style="display:inline-block;width:11px;'
        + 'height:11px;margin-right:5px;background:' + STOPS[i][0]
        + ';"></span>' + STOPS[i][1] + ' travel</div>';
    }
    leg.innerHTML = html;
    box.appendChild(leg);
    // hidden itinerary panel — populated on a fastlink-line click
    var panel = document.createElement('div');
    panel.id = 'fastlink-panel';
    panel.style.cssText = 'position:absolute;top:8px;right:8px;z-index:3;'
      + 'display:none;max-width:320px;max-height:440px;overflow-y:auto;'
      + 'background:rgba(255,255,255,0.96);padding:8px 10px;'
      + 'border-radius:4px;font:12px/1.5 sans-serif;color:#222;'
      + 'box-shadow:0 1px 6px rgba(0,0,0,0.4);';
    box.appendChild(panel);
      });
      function applyOrigin(fid, name) {
    var lineFlt = ['all', ['==', ['get', 'theme'], 'fastlink'],
                   ['==', ['get', 'origin_station_id'], fid]];
    M.setFilter('fastlink-line-casing', lineFlt);
    M.setFilter('fastlink-line', lineFlt);
    M.setFilter('fastlink-origin-selected', ['all',
      ['==', ['get', 'theme'], 'fastlink-origin'],
      ['==', ['get', 'origin_station_id'], fid]]);
    var hint = document.getElementById('fastlink-hint');
    if (hint) { hint.textContent = 'from: ' + (name || fid); }
      }
      M.on('click', 'fastlink-origin', function (e) {
    if (!e.features || !e.features.length) { return; }
    var p = e.features[0].properties || {};
    applyOrigin(p.origin_station_id, p.origin_name);
      });
      // click a connection LINE -> open the exact-travel-times panel
      function hhmmDiff(a, b) {       // minutes, b - a, "HH:MM" strings
    function mins(s) { var q = s.split(':'); return (+q[0]) * 60 + (+q[1]); }
    return mins(b) - mins(a);
      }
      M.on('click', 'fastlink-line', function (e) {
    if (!e.features || !e.features.length) { return; }
    var p = e.features[0].properties || {};
    var legs;
    try { legs = JSON.parse(p.itinerary || '[]'); } catch (err) { legs = []; }
    var panel = document.getElementById('fastlink-panel');
    if (!panel || !legs.length) { return; }
    var nt = p.n_transfers
      + (p.n_transfers === '1' ? ' transfer' : ' transfers');
    var h = '<span id="fastlink-close" style="float:right;cursor:pointer;'
      + 'font-weight:bold;color:#888;">&times;</span>'
      + '<b>' + p.origin_name + ' &rarr; ' + p.dest_name + '</b><br>'
      + '<span style="color:#666;">depart ' + legs[0][1] + ' &middot; '
      + 'arrive ' + legs[legs.length - 1][3] + ' &middot; ' + nt
      + '</span><hr style="border:none;border-top:1px solid #ddd;'
      + 'margin:5px 0;">';
    for (var i = 0; i < legs.length; i++) {
      if (i > 0) {
        h += '<div style="color:#b06000;margin:3px 0;">&#8693; transfer at '
          + legs[i][0] + ', wait '
          + hhmmDiff(legs[i - 1][3], legs[i][1]) + ' min</div>';
      }
      h += '<div style="margin:2px 0;">&#128642; <b>' + legs[i][1]
        + '</b> ' + legs[i][0] + '<br>&nbsp;&nbsp;&#8595; <b>'
        + legs[i][3] + '</b> ' + legs[i][2] + '</div>';
    }
    panel.innerHTML = h;
    var cb = document.getElementById('fastlink-close');
    if (cb) { cb.addEventListener('click', function () {
      panel.style.display = 'none';
    }); }
    panel.style.display = 'block';
      });
      var popup = new maplibregl.Popup({
    closeButton: false, closeOnClick: false,
      });
      M.on('mousemove', 'fastlink-line', function (e) {
    M.getCanvas().style.cursor = 'pointer';
    var p = e.features[0].properties || {};
    var m = parseInt(p.travel_min, 10) || 0;
    var hh = Math.floor(m / 60);
    var tt = (hh ? hh + 'h ' : '') + (m % 60) + 'm';
    var nt = p.n_transfers
      + (p.n_transfers === '1' ? ' transfer' : ' transfers');
    popup.setLngLat(e.lngLat).setHTML(
      '<b>' + p.origin_name + ' &rarr; ' + p.dest_name + '</b><br>'
      + tt + ', ' + nt).addTo(M);
      });
      M.on('mouseleave', 'fastlink-line', function () {
    M.getCanvas().style.cursor = '';
    popup.remove();
      });
      M.on('mouseenter', 'fastlink-origin', function () {
    M.getCanvas().style.cursor = 'pointer';
      });
      M.on('mouseleave', 'fastlink-origin', function () {
    M.getCanvas().style.cursor = '';
      });
    })();
    """
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-fastlink",
            layer_name="austria-fastlink",
            center=[13.34, 47.6],
            zoom=7,
            style_layers=FASTLINK_STYLE,
            source_maxzoom=10,
            satellite_background=True,
            terrain=True,
            hillshade=False,
            pitch=0,
            max_pitch=85,
            glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
            extra_js=_FASTLINK_CLICK_JS,
        ),
        height="500px",
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Route builder — pick your own journey

    **Left-click** a station to start a route, **shift+left-click**
    each additional station to add a stop. The tool draws the
    journey through your picked stations and shows the cumulative
    leg-by-leg itinerary.

    Routing every station pair (~1,650²) would explode, so each
    segment is **hub-decomposed**: `station → hub → … → hub →
    station`, over the routing network the GTFS DAG precomputes —
    `hub→station`, `station→hub`, and a **hub→hub departure-time
    profile** so the hub-core leg is matched to when the
    `station→hub` leg actually arrives. And because every computed
    journey carries its full ordered stop list, a waypoint pair
    that already shares one journey is routed as a **direct
    sub-path** of it — no hub detour for nearby stations. All of it
    is the `austria-routehub` PMTiles archive + a client-side
    stitcher; no per-route compute.
    """)
    return


@app.cell
def _(Path, ROUTEBUILD_STYLE, dag_run_states, martin, mo, versatiles_assets):
    # Route-builder cell — click stations, get a hub-decomposed route.
    #
    # source_name="austria-routehub" → the helper's `src` IS the
    # routing-network tile (hub→station / station→hub / hub→hub-profile
    # journeys, each carrying its full ordered `stops` list). The
    # `routehub-loader` style layer (always-false filter, renders
    # nothing) forces martin to load that source's tiles so
    # _ROUTEBUILD_JS can read journey `stops` via querySourceFeatures.
    # extra_sources also wires `austria-transit` (the clickable station
    # dots + the station→coord lookup) and two empty client-side
    # GeoJSON sources (`route-src`, `pick-src`) the JS keeps updated.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    _routehub_pmtiles = Path(
        "/workspace/tiles/pmtiles/austria-routehub.pmtiles"
    )
    mo.stop(
        not _routehub_pmtiles.exists()
        or _routehub_pmtiles.stat().st_size == 0,
        "`austria-routehub.pmtiles` not yet present — the GTFS DAG's "
        "`compute_route_network` + `freestiler_routehub_convert` tasks "
        "produce it (re-run the DAG if this notebook predates them).",
    )
    # The route-builder interaction, injected via the helper's
    # `extra_js`. Coupled to source_name="austria-routehub" (map var
    # `map_austria_routehub` / container `map-austria-routehub`), the
    # `routebuild-station-dot` click layer, and the `route-src` /
    # `pick-src` client GeoJSON sources from ROUTEBUILD_STYLE.
    _ROUTEBUILD_JS = """
    (function () {
      var M = map_austria_routehub;
      // MapLibre's default boxZoom handler claims shift+mousedown
      // (shift+drag = box zoom) and suppresses the click that would
      // otherwise follow — silently breaking shift+click-to-add-a-stop,
      // the core gesture of this map. Disable it here (route-builder
      // only; the other five maps keep boxZoom) so a shift+click
      // reaches the routebuild-station-dot handler.
      M.boxZoom.disable();
      var route = [];        // picked station_feature_ids, in order
      var routeSegs = [];    // one entry per consecutive waypoint pair
      var coordOf = {};      // station_feature_id -> [lon,lat]
      var nameOf = {};       // station_feature_id -> name
      var h2h = {};          // hub_a -> hub_b -> [opts sorted by depart_s]
      var allDirect = [];    // every journey's stops, for sub-path search

      function hhSec(h) {
    if (!h) { return null; }
    return (+h.slice(0, 2)) * 3600 + (+h.slice(3)) * 60;
      }
      function dedup(feats) {              // tile-clip dedup by osm_id
    var seen = {}, out = [];
    for (var i = 0; i < feats.length; i++) {
      var p = feats[i].properties || {};
      if (p.osm_id && !seen[p.osm_id]) { seen[p.osm_id] = 1; out.push(p); }
    }
    return out;
      }
      function coordsOf(stops) {           // stops -> [lon,lat] list
    var c = [];
    for (var i = 0; i < stops.length; i++) {
      var xy = coordOf[stops[i][0]];
      if (xy) { c.push(xy); }
    }
    return c;
      }

      M.on('load', function () {
    var box = document.getElementById('map-austria-routehub');
    box.style.position = 'relative';
    var panel = document.createElement('div');
    panel.id = 'route-panel';
    panel.style.cssText = 'position:absolute;top:8px;right:8px;z-index:3;'
      + 'max-width:330px;max-height:460px;overflow-y:auto;'
      + 'background:rgba(255,255,255,0.96);padding:8px 10px;'
      + 'border-radius:4px;font:12px/1.5 sans-serif;color:#222;'
      + 'box-shadow:0 1px 6px rgba(0,0,0,0.4);';
    box.appendChild(panel);
    // station -> coord / name, from the austria-transit dots
    var tf = M.querySourceFeatures('transit-src',
      { sourceLayer: 'austria-transit',
        filter: ['==', ['get', 'is_station_label'], 'true'] });
    for (var i = 0; i < tf.length; i++) {
      var p = tf[i].properties || {};
      if (p.station_feature_id && !coordOf[p.station_feature_id]) {
        coordOf[p.station_feature_id] = tf[i].geometry.coordinates;
        nameOf[p.station_feature_id] =
          p.station_name || p.station_feature_id;
      }
    }
    // pre-load every journey's stops (for direct sub-path search) +
    // the hub->hub profile lookup
    var jf = dedup(M.querySourceFeatures('src',
      { sourceLayer: 'austria-routehub' }));
    for (var i = 0; i < jf.length; i++) {
      var p = jf[i];
      var stops;
      try { stops = JSON.parse(p.stops || '[]'); } catch (e) { stops = []; }
      if (stops.length < 2) { continue; }
      allDirect.push({ stops: stops, theme: p.theme });
      if (p.theme === 'h2h') {
        var a = p.origin_station_id, b = p.dest_station_id;
        (h2h[a] = h2h[a] || {});
        (h2h[a][b] = h2h[a][b] || []);
        h2h[a][b].push({
          depart_s: hhSec(p.depart_hhmm),
          arrive_s: hhSec(p.arrive_hhmm),
          travel_min: +p.travel_min, n_transfers: +p.n_transfers,
          stops: stops,
        });
      }
    }
    for (var a in h2h) {
      for (var b in h2h[a]) {
        h2h[a][b].sort(function (x, y) {
          return x.depart_s - y.depart_s;
        });
      }
    }
    renderPanel();
      });

      // a direct sub-path of a single computed journey covering A then B.
      // ia = where the journey DEPARTS a (its entry with a non-empty
      // dep — for a transfer station that is the board twin, not the
      // alight twin); ib = where it ARRIVES at b (non-empty arr). Slicing
      // a clean board..alight run keeps renderLegs free of phantom
      // "transfer at origin" lines and the time math honest.
      function directSubpath(a, b) {
    var best = null;
    for (var i = 0; i < allDirect.length; i++) {
      var stops = allDirect[i].stops, ia = -1, ib = -1;
      for (var k = 0; k < stops.length; k++) {
        if (stops[k][0] === a && stops[k][2] !== '') { ia = k; break; }
      }
      if (ia < 0) { continue; }
      for (var k = ia + 1; k < stops.length; k++) {
        if (stops[k][0] === b && stops[k][1] !== '') { ib = k; break; }
      }
      if (ib < 0) { continue; }
      var sub = stops.slice(ia, ib + 1);
      var t = (hhSec(sub[sub.length - 1][1]) - hhSec(sub[0][2])) / 60;
      if (t >= 0 && (!best || t < best.travel_min)) {
        best = {
          direct: true, coords: coordsOf(sub),
          parts: [{ stops: sub }],
          travel_min: Math.round(t),
          n_transfers: sub[sub.length - 1][3] - sub[0][3],
        };
      }
    }
    return best;
      }

      // hub-decompose A -> B: station->hub + hub->hub(time-matched) +
      // hub->station, argmin total arrival
      function hubDecompose(a, b) {
    function pieces(theme, prop, val) {
      return dedup(M.querySourceFeatures('src', {
        sourceLayer: 'austria-routehub',
        filter: ['all', ['==', ['get', 'theme'], theme],
                 ['==', ['get', prop], val]],
      })).map(function (p) {
        var st;
        try { st = JSON.parse(p.stops || '[]'); } catch (e) { st = []; }
        return {
          hub: (theme === 's2h' ? p.dest_station_id
                                : p.origin_station_id),
          arrive_s: hhSec(p.arrive_hhmm),
          travel_min: +p.travel_min, n_transfers: +p.n_transfers,
          stops: st,
        };
      });
    }
    var access = {}, egress = {};
    pieces('s2h', 'origin_station_id', a).forEach(function (x) {
      if (!access[x.hub] || x.arrive_s < access[x.hub].arrive_s) {
        access[x.hub] = x;
      }
    });
    pieces('h2s', 'dest_station_id', b).forEach(function (x) {
      if (!egress[x.hub] || x.travel_min < egress[x.hub].travel_min) {
        egress[x.hub] = x;
      }
    });
    var best = null;
    for (var ha in access) {
      var ac = access[ha];
      for (var hb in egress) {
        var eg = egress[hb], mid;
        if (ha === hb) {
          mid = { travel_min: 0, n_transfers: 0, stops: [] };
        } else {
          var opts = (h2h[ha] || {})[hb];
          if (!opts) { continue; }
          mid = null;
          for (var k = 0; k < opts.length; k++) {
            if (opts[k].depart_s >= ac.arrive_s) { mid = opts[k]; break; }
          }
          if (!mid) { continue; }
        }
        var total = ac.travel_min + mid.travel_min + eg.travel_min;
        if (!best || total < best.total) {
          best = { total: total, access: ac, mid: mid, egress: eg };
        }
      }
    }
    if (!best) { return null; }
    var parts = [{ stops: best.access.stops, label: 'to hub' }];
    if (best.mid.stops.length) {
      parts.push({ stops: best.mid.stops, label: 'hub \\u2192 hub' });
    }
    parts.push({ stops: best.egress.stops, label: 'from hub' });
    var coords = [];
    parts.forEach(function (pt) {
      coords = coords.concat(coordsOf(pt.stops));
    });
    return {
      direct: false, coords: coords, parts: parts, travel_min: best.total,
      n_transfers: best.access.n_transfers + best.mid.n_transfers
                   + best.egress.n_transfers,
    };
      }

      function addSegment(prev, cur) {
    return directSubpath(prev, cur) || hubDecompose(prev, cur);
      }

      // render a part's leg-by-leg schedule (group stops by leg_idx)
      function renderLegs(stops) {
    var html = '', byLeg = {};
    for (var i = 0; i < stops.length; i++) {
      (byLeg[stops[i][3]] = byLeg[stops[i][3]] || []).push(stops[i]);
    }
    var legIds = Object.keys(byLeg).sort(function (x, y) {
      return (+x) - (+y);
    });
    for (var li = 0; li < legIds.length; li++) {
      var ls = byLeg[legIds[li]];
      var a = ls[0], z = ls[ls.length - 1];
      if (li > 0) {
        html += '<div style="color:#b06000;">&#8597; transfer at '
          + (nameOf[a[0]] || a[0]) + '</div>';
      }
      html += '<div>&#128642; <b>' + a[2] + '</b> '
        + (nameOf[a[0]] || a[0]) + '<br>&nbsp;&nbsp;&#8595; <b>'
        + z[1] + '</b> ' + (nameOf[z[0]] || z[0]) + '</div>';
    }
    return html;
      }

      function renderPanel() {
    var panel = document.getElementById('route-panel');
    if (!panel) { return; }
    if (!route.length) {
      panel.innerHTML = '<b>Route builder</b><br>'
        + '<span style="color:#666;">left-click a station to start '
        + '&middot; shift+click to add stops</span>';
      return;
    }
    var h = '<span id="route-clear" style="float:right;cursor:pointer;'
      + 'font-weight:bold;color:#888;">&times;</span><b>Your route</b>'
      + '<div style="color:#666;">';
    for (var i = 0; i < route.length; i++) {
      h += (i ? ' &rarr; ' : '') + (i + 1) + '.&nbsp;'
        + (nameOf[route[i]] || route[i]);
    }
    h += '</div>';
    var totMin = 0, totTr = 0, okAll = true;
    for (var s = 0; s < routeSegs.length; s++) {
      var seg = routeSegs[s];
      h += '<hr style="border:none;border-top:1px solid #ddd;'
        + 'margin:5px 0;">';
      if (!seg) {
        h += '<div style="color:#b00;">leg ' + (s + 1)
          + ': no route found</div>';
        okAll = false;
        continue;
      }
      totMin += seg.travel_min;
      totTr += seg.n_transfers;
      h += '<div style="color:#666;">leg ' + (s + 1) + ' &mdash; '
        + (seg.direct ? 'direct' : 'via hub') + ', '
        + seg.travel_min + ' min, ' + seg.n_transfers + ' transfer'
        + (seg.n_transfers === 1 ? '' : 's') + '</div>';
      for (var pi = 0; pi < seg.parts.length; pi++) {
        h += renderLegs(seg.parts[pi].stops);
      }
    }
    if (routeSegs.length && okAll) {
      h += '<hr style="border:none;border-top:1px solid #ddd;'
        + 'margin:5px 0;"><b>Total: ' + totMin + ' min, ' + totTr
        + ' transfer' + (totTr === 1 ? '' : 's') + '</b>';
    }
    panel.innerHTML = h;
    var cb = document.getElementById('route-clear');
    if (cb) {
      cb.addEventListener('click', function () {
        route = []; routeSegs = []; redraw();
      });
    }
      }

      function redraw() {
    var rf = [];
    for (var s = 0; s < routeSegs.length; s++) {
      if (routeSegs[s] && routeSegs[s].coords.length > 1) {
        rf.push({ type: 'Feature', properties: {},
          geometry: { type: 'LineString',
                      coordinates: routeSegs[s].coords } });
      }
    }
    M.getSource('route-src').setData(
      { type: 'FeatureCollection', features: rf });
    var pf = [];
    for (var i = 0; i < route.length; i++) {
      var xy = coordOf[route[i]];
      if (xy) {
        pf.push({ type: 'Feature',
          properties: { order: String(i + 1) },
          geometry: { type: 'Point', coordinates: xy } });
      }
    }
    M.getSource('pick-src').setData(
      { type: 'FeatureCollection', features: pf });
    renderPanel();
      }

      M.on('click', 'routebuild-station-dot', function (e) {
    if (!e.features || !e.features.length) { return; }
    var sid = e.features[0].properties.station_feature_id;
    if (!sid) { return; }
    if (e.originalEvent && e.originalEvent.shiftKey && route.length) {
      var prev = route[route.length - 1];
      if (prev === sid) { return; }
      routeSegs.push(addSegment(prev, sid));
      route.push(sid);
    } else {
      route = [sid];
      routeSegs = [];
    }
    redraw();
      });
      M.on('mouseenter', 'routebuild-station-dot', function () {
    M.getCanvas().style.cursor = 'pointer';
      });
      M.on('mouseleave', 'routebuild-station-dot', function () {
    M.getCanvas().style.cursor = '';
      });
    })();
    """
    mo.iframe(
        build_pipeline_maplibre_html(
            martin,
            "austria-routehub",
            layer_name="austria-routehub",
            center=[13.34, 47.6],
            zoom=7,
            style_layers=ROUTEBUILD_STYLE,
            # austria-routehub is baked z0-only — one tile carries every
            # journey; MapLibre overzooms it at all display zooms so
            # querySourceFeatures always sees the whole dataset.
            source_maxzoom=0,
            extra_sources={
                "transit-src": {
                    "type": "vector",
                    "url": f"{martin}/austria-transit",
                    "maxzoom": 14,
                },
                "route-src": {
                    "type": "geojson",
                    "data": {"type": "FeatureCollection", "features": []},
                },
                "pick-src": {
                    "type": "geojson",
                    "data": {"type": "FeatureCollection", "features": []},
                },
            },
            satellite_background=True,
            terrain=True,
            hillshade=False,
            pitch=0,
            max_pitch=85,
            glyphs_url=f"{versatiles_assets}/fonts/{{fontstack}}/{{range}}.pbf",
            extra_js=_ROUTEBUILD_JS,
        ),
        height="500px",
    )
    return


@app.cell
def _(dag_run_states, mo):
    # Top transfer hubs — ranks every parent station by hub_score, the
    # transfer-hub importance metric the GTFS DAG's
    # match_gtfs_stops_to_osm task computes into
    # transit.station_hub_scores (sum over LINE-pairs at the station of
    # line reach in km x transfer feasibility x terminus factor).
    # hub_rank from this same table drives symbol-sort-key in the
    # transit map's transit-stops-label layer, so the labels that
    # survive a crowded viewport are the genuine interchange hubs.
    mo.stop(
        dag_run_states.get("notebook_austria_gtfs_pipeline") != "success",
        f"Waiting for notebook_austria_gtfs_pipeline (state="
        f"{dag_run_states.get('notebook_austria_gtfs_pipeline')!r})",
    )
    import duckdb as _duckdb_hub
    _hub_con = _duckdb_hub.connect(
        "/workspace/duckdb/austria.duckdb",
        read_only=True,
    )
    _hub_con.sql("INSTALL spatial; LOAD spatial;")
    _top_hubs = _hub_con.sql("""
        SELECT
            h.hub_rank,
            min(m.station_name)        AS station_name,
            h.station_feature_id,
            round(h.hub_score, 1)      AS hub_score,
            h.n_routes,
            h.n_terminating_lines,
            h.n_route_pairs,
            round(h.max_reach_km, 1)   AS max_reach_km
        FROM transit.station_hub_scores h
        JOIN transit.station_members m
          ON m.station_feature_id = h.station_feature_id
        GROUP BY h.hub_rank, h.station_feature_id, h.hub_score,
                 h.n_routes, h.n_terminating_lines, h.n_route_pairs,
                 h.max_reach_km
        ORDER BY h.hub_rank
        LIMIT 25
    """).pl()
    _hub_con.close()
    mo.vstack([
        mo.md("**Top 25 transfer hubs** "
              "(hub_score = sum over line-pairs at the station of line "
              "reach in km x transfer feasibility x terminus factor; "
              "hub_rank drives label-placement priority in the "
              "transit map)"),
        _top_hubs,
    ])
    return


if __name__ == "__main__":
    app.run()
