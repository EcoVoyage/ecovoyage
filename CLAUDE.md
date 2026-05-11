# EcoVoyage workspace

Bind-mounted workspace for the `versa/ecovoyage` deploy — an instance
of the versa image bundling marimo + Airflow + OSM/GTFS analytics +
martin vector tiles + 3D terrain via MapLibre.

- **Container path**: `/workspace`
- **Host path**: `~/Sync/Atrapub/ecovoyage`
- **Container name**: `ov-versa-ecovoyage`
- **Quadlet**: `~/.config/containers/systemd/ov-versa-ecovoyage.container`
- **Lifecycle**: `disposable: true`, `lifecycle: dev`

## Skills (load before reading code or running ov verbs)

Per the overthink R0 dispatcher rule, always invoke the matching skill
via the `Skill` tool BEFORE touching code or running ov commands. For
this workspace, the most-relevant skills are:

| Skill | Coverage |
|---|---|
| `/ov-versa:versa` | image overview, ports, R10 acceptance |
| `/ov-versa:marimo-layer` | marimo pixi env + supervisord service |
| `/ov-versa:marimo-mcp` | marimo MCP server tool catalog |
| `/ov-versa:airflow-layer` | Airflow LocalExecutor + SQLite wiring |
| `/ov-versa:airflow-mcp` | airflow REST→MCP wrapper tool catalog |
| `/ov-versa:notebook-osm` | canonical Monaco OSM + GTFS notebook |
| `/ov-versa:osm-tools-layer` | tippecanoe + martin + reload pattern |
| `/ov-versa:maputnik-layer` | maputnik style editor (Vite `--base=/`) |
| `/ov-versa:pmtiles-viewer` | PMTiles viewer SPA on port 28001 |
| `/ov-versa:shortbread` | tilemaker + shortbread schema generation |
| `/ov-versa:versatiles` | versatiles-rs CLI + serve |

Lifecycle / config / deploy / live-eval verbs: see `/ov-core:*` and
`/ov-eval:eval`.

## MCP servers (host-port form, reachable from this directory)

This workspace's `.mcp.json` registers two HTTP MCP servers:

- `marimo` — `http://localhost:32718/mcp/server` (10 notebook-inspection
  tools; the marimo runtime exposes these alongside the editor on the
  same port)
- `airflow` — `http://localhost:39999/mcp` (~70 REST-API-wrapping tools
  for DAG management)

URLs use the instance-specific host-port mappings from the
`versa/ecovoyage` deploy entry. Container-internal `localhost:2718`
and `localhost:19999` are only reachable from inside the pod.

The MCP server names (`marimo`, `airflow`) deliberately did NOT change
in the 2026-05 image rename — they reflect upstream-software identity,
not OUR image identity. See `/ov-versa:versa` "MCP Name Decoupling".

## Notebook

`notebooks/osm-monaco-viz.py` is seeded from the versa image's
`notebook-osm` data layer. Edit interactively in the marimo editor:

  open http://127.0.0.1:32718/

The notebook self-authors six Airflow DAGs at runtime (writing to
`AIRFLOW_DAGS_DIR=/workspace/dags`) covering OSM analytics + 4 PMTiles
generation pipelines + GTFS transit parquet + Shortbread schema tiles.
See `/ov-versa:notebook-osm` for the full architecture.

## Force-seed semantics

`ov update --force-seed versa -i ecovoyage` will OVERWRITE
`~/Sync/Atrapub/ecovoyage/notebooks/osm-monaco-viz.py` (and other
seeded files) via a `cp -a` seeder container. The git history is the
recovery surface — commit local edits before force-seeding.
