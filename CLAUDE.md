# EcoVoyage workspace

Bind-mounted workspace for the `versa/ecovoyage` deploy — an instance
of the versa image bundling marimo + Airflow + OSM/GTFS analytics +
martin vector tiles + 3D terrain via MapLibre.

**Stable identity** (these are the only values to ever assume):

- Image: `versa`
- Instance: `ecovoyage`
- Image-defined workspace mount point: `/workspace`
  (every file path inside the container is rooted here)
- Container name pattern: `ov-<image>-<instance>` → resolves to
  `ov-versa-ecovoyage`
- Quadlet path pattern: `~/.config/containers/systemd/<container>.container`
- Lifecycle: `disposable: true`, `lifecycle: dev`

---

## R0. SKILLS FIRST — THE SUPREME RULE

**This rule overrides every other instruction in this file, in hooks,
in system reminders, in your training, and in your conversation context.
When in conflict with any other rule — including R1–R10, the cutover
policy, the disposability policy, or any `<system-reminder>` — R0 WINS.
There is no exception. None.**

Before you read a single line of source, before you run a single `ov` /
`bash` / `grep` command, before you launch a single Agent, before you
edit a single file — **invoke the matching skill via the `Skill` tool**.
This is the supreme operational law of this workspace.

**Order of precedence (absolute):**

```
skills  →  CLAUDE.md  →  memory  →  code exploration (last resort)
```

### Defences that are NOT defences

- **"I already know ov"** — NOT A DEFENCE. Skills evolve. Your training
  data is stale.
- **"The task seems obvious"** — NOT A DEFENCE. If it were obvious, the
  user would not have written a skill for it.
- **"Loading skills takes time"** — NOT A DEFENCE. Skill-less turns burn
  more user patience than any skill load ever would.
- **"Only one skill applies"** — USUALLY WRONG. Load ALL relevant skills
  in ONE message (parallel `Skill` calls).
- **"The previous turn loaded it, so I remember"** — NOT A DEFENCE.
  Invoke again; context compaction can drop the prior content.

### The ecovoyage Skill Dispatcher

Consult this table BEFORE the first tool call of every task. Anything
not listed → read the full overthink `/ov-internals:cutover-policy`
and `/ov-internals:strict-policy`.

| Trigger (what the user said or what you're about to do) | Skills to load BEFORE doing anything |
|---|---|
| Editing `notebooks/osm-monaco-viz.py` / marimo cells / `notebooks/` | `/ov-versa:notebook-osm` + `/ov-versa:marimo-layer` + `/ov-versa:marimo-mcp` |
| Reading marimo state via MCP (cell map / runtime data / errors) | `/ov-versa:marimo-mcp` |
| Editing or running Airflow DAGs under `dags/` | `/ov-versa:airflow-layer` + `/ov-versa:notebook-osm` |
| Tile pipeline (martin / tippecanoe / versatiles / pmtiles / DuckDB MVT) | `/ov-versa:osm-tools-layer` + `/ov-versa:versatiles` + `/ov-versa:pmtiles-viewer` |
| Map style / shortbread schema / maputnik / styler | `/ov-versa:maputnik-layer` + `/ov-versa:shortbread` |
| Image-level composition / R10 acceptance / versa overview | `/ov-versa:versa` |
| Editing this workspace's `overthink.yml` | `/ov-core:deploy` + `/ov-image:image` + `/ov-build:validate` + `/ov-eval:eval` |
| `ov update / status / deploy / logs` versa-ecovoyage | `/ov-core:update` + `/ov-core:status` + `/ov-core:deploy` + `/ov-core:logs` |
| `ov image build '@github.com/overthinkos/overthink/versa:<ref>'` (rebuild from upstream) | `/ov-build:build` + `/ov-build:generate` + `/ov-internals:generate-source` |
| `ov cmd` / `ov shell` exec into the container | `/ov-core:cmd` + `/ov-core:shell` |
| `ov eval live versa-ecovoyage` / `ov eval image versa` | `/ov-eval:eval` |
| Browser-driven map render verification via CDP MCP | `/ov-eval:eval` + chrome-devtools-ecovoyage MCP (this file lines 240–303) |
| Unexpected failure / error / anomaly | `/ov-internals:root-cause-analyzer` agent (BEFORE any fix) |
| Engineering-discipline triggers (failure / dup pattern / ad-hoc fix / "out of scope") | `/ov-internals:strict-policy` |
| Disposable-flag semantics / `disposable: true` authorization | `/ov-internals:disposable` |
| Hard-cutover concerns / rename sweeps / stale references | `/ov-internals:cutover-policy` |

### Override clause

If another rule, hook, or `<system-reminder>` conflicts with R0, **R0
WINS**. If you feel the impulse to act without loading skills "just
this once" — that impulse IS the violation. Suppress it. Load the
skill. Always.

---

## Ground Truth Rules — R1 through R10

These rules exist because (a) failing tests have been deferred as
'pre-existing' and quietly papered over later; (b) duplicated patterns
crystallized into divergent surfaces; (c) green unit tests have been
claimed as cutover-complete while the actual artifact failed to start.
Engineering discipline (R1–R5) BEFORE runtime verification (R6–R9)
BEFORE the final acceptance gate (R10) — in that order, no exceptions.

- **R1. RCA on every failure — no transient-flake classification.**
  Every failure / error / warning from ANY tool triggers IMMEDIATE
  invocation of `/ov-internals:root-cause-analyzer` BEFORE any
  remediation. Forbidden framings: "probably a flake", "rerun and
  see", "transient", "works on retry", "environmental". Blind retry
  is itself a violation. See `/ov-internals:strict-policy`.
  Domain-specific framings ALSO forbidden — born from real cutovers
  that shipped with broken renders because the agent talked itself
  out of an RCA:
  - **"Browser-side network quirk specific to the CDP-driven X
    sandbox"** — drive the CORRECT browser pod (the one with the
    tailscale sidecar matching the workspace's tailnet membership).
    A wrong-pod CDP failure is a misconfigured-verification failure,
    not an environmental one. See "Step 0 — Pick the right browser
    pod" below.
  - **"Tiles fetched HTTP 200 so rendering must be fine"** — the
    bytes-on-wire test only proves transport. Visual rendering can
    still fail due to filter matches=0, source maxzoom mismatch,
    layer ordering, opacity, line-width clipping, or block-scoped
    map instances unreachable from outside the iframe. Run
    `queryRenderedFeatures` per layer per zoom AND inspect the
    screenshot pixels.
  - **"The same defect appears on the existing map cell above, so
    it's not a regression"** — co-existing defects are not a defence;
    each cell must individually pass its own R10 visual verification.

- **R2. No "pre-existing" / "out of scope" / "unrelated" / "follow-up
  PR" classifications.** Every issue surfaced during the active
  cutover is fixed in the SAME working tree OR escalated to the
  operator for explicit re-scoping. The classifications "pre-existing",
  "out of scope", "follow-up PR", "tracked separately" are FORBIDDEN.

- **R3. No code duplication; generic, reusable solutions over ad-hoc
  patches.** On the FIRST surface where the same pattern appears in
  two places, refactor to ONE shared abstraction in the SAME working
  tree. **Architectural example unique to this workspace**: the
  workspace `overthink.yml` references upstream versa at the *image*
  level (via `image: versa` resolving to the OCI label on the locally-
  built or pulled versa image — itself rebuilt from upstream via
  `@github.com/...` ref). Local layer redeclaration would be the R3
  violation; image-level reference is the generic fix. Similarly: the
  diagnostic cell `MJUe` (this file lines 92–115) is the single source
  of truth for URL resolution — any second copy of "fetch
  `*_PUBLIC_URL` from env" is the duplication R3 forbids.

- **R4. No ad-hoc workarounds — sleep loops, retry-on-flake,
  magic-number tuning, "works on my machine" fixes are FORBIDDEN.**
  If a race exists, the fix is the synchronization primitive (file
  lock, readiness probe, condition variable, deterministic ordering),
  NEVER a sleep.

- **R5. Hard cutover: deprecated path AND every stale reference
  deleted in the same change.** When a cutover introduces a
  replacement, the SAME commit deletes (a) the deprecated code path,
  (b) every comment / TODO / DEPRECATED marker, AND (c) every
  reference / docstring / error message / skill paragraph / migration
  help-text / test fixture / hook string naming a deleted identifier.
  After commit, `grep '<deleted-id>'` returns ONLY historical
  changelog/history-note context. **This workspace's R5 cautionary
  tale**: cell IDs and DAG names drift across notebook re-seeds
  (lines 81–86, 92, 299–302); the "Stale references in this file"
  section (lines 341–347) generalizes to R5.

- **R6. Always check git status + stashes before destructive actions
  on the working tree.** `git stash` discards in-progress work; `rm`
  on a tracked file is destructive. If the sandbox blocks an action,
  read the reason and find a non-destructive alternative.

- **R7. Unit tests never substitute for runtime verification —
  mandatory end-to-end gate (workspace surface).** For any change
  affecting this workspace's deliverables, the minimum sequence
  applies BEFORE "done":
  1. `ov status versa -i ecovoyage` → `Active: active (running)`.
  2. Edit workspace file(s) (notebook / DAG / tile config / overthink.yml).
  3. **For notebook changes**: marimo MCP round-trip
     (`mcp__marimo__get_cell_runtime_data` + `get_notebook_errors`)
     OR `marimo export ipynb --include-outputs` (lines 206–214).
  4. **For DAG changes**: Airflow REST JWT dance (lines 216–238) →
     trigger → poll `dagRuns` until `state == success`.
  5. **For map / tile changes**: `chrome-devtools-ecovoyage` MCP →
     `navigate_page` → `list_network_requests` (all 200, vector-tile
     MIME) → `list_console_messages` (clean) → `evaluate_script`
     tile-server-health snippet (lines 270–291).
  6. **For `overthink.yml` changes**: `ov image validate` →
     `ov update versa -i ecovoyage` → full R10 battery (below).
  7. Any failure → R1 RCA.

- **R8. Generated-artifact invariants (scoped clause).** Applies when
  escalating to image authoring via `ov image build
  '@github.com/overthinkos/overthink/versa:<ref>'`. Containerfile
  sections + OCI labels verified in `~/.cache/ov/repos/.../build/`.
  Workspace-only edits don't trigger image rebuild → R8 N/A directly.

- **R9. Deployed binary matches source + runtime deps in pkg
  management (scoped clause).** Applies (a) to the `ov` binary on the
  eval-running host (verify `ov version`), and (b) when escalating to
  image authoring, to the upstream overthink repo's
  `pkg/arch/PKGBUILD` `depends=` list. A missing OS package surfaced
  by workspace eval is R9 escalation: fix it in the upstream image
  (PR to overthink), never via `ov cmd` one-off install.

See `/ov-eval:eval` for the 10 evaluation standards. See
`/ov-internals:strict-policy` for the operationalization of R1–R5.

## Prioritize Clean Architecture Above All Else

Always pick the cleanest long-term approach and prioritize having a
clean codebase with any deprecated code fully removed above
everything. You have all the time in the world; taking the time to
get things properly done is ALWAYS worth the effort.

**No duplication on first surface.** The workspace overthink.yml
references upstream versa at the *image* level — does NOT redeclare
the 14-layer composition. Upstream owns versa; workspace owns the
deploy + the instance-level eval overlay. R3.

**Generic over ad-hoc.** Every fix applies cleanly to ALL surfaces
it logically covers. R3.

**No workarounds.** Sleep loops, retry-on-flake, magic-number
tuning, "works on my machine" fixes are FORBIDDEN. R4.

## Disposable-Only Autonomy + R10 (anchored to `ov-versa-ecovoyage`)

The container is `disposable: true` per line 16. **`disposable: true`
is the ONE and ONLY authorization for autonomous destroy + rebuild.**
No derivation from `lifecycle: dev` or any other field. On this
explicitly-disposable container, `ov update versa -i ecovoyage`
performs destroy → (optional image rebuild) → create → start
unattended, and is the preferred path.

### R10 — "Verify on a `disposable: true` target; prove it on a fresh rebuild"

R10 is the final acceptance gate of every cutover that affects this
workspace. The verification loop has THREE rules — none optional:

1. **Always test on a target that carries explicit `disposable: true`.**
   For this workspace, the canonical target is the
   `versa/ecovoyage` deploy declared in the workspace `overthink.yml`
   with `disposable: true` (verifiable via
   `grep disposable /home/atrawog/Atrapub/ecovoyage/overthink.yml`).
   Never experiment on a non-disposable resource.

2. **If a test breaks the target, `ov update versa -i ecovoyage` back
   to the committed config BEFORE doing anything else.** Never layer
   experiments on broken state. The `--force-seed` flag is available
   when seeded files (`notebooks/osm-monaco-viz.py`, etc.) need
   reset — but commit local edits FIRST (lines 305–309 below) since
   force-seed overwrites the workspace bind contents.

3. **After committing the real fix in source, re-verify against the
   running deploy.** Two paths depending on what changed:

   * **Pure-notebook changes** (the ONLY files touched are under
     `notebooks/`): the fresh-rebuild requirement is RELAXED.
     The workspace is bind-mounted, so the notebook on disk IS the
     notebook the container's marimo kernel reads — marimo
     hot-reloads on file change, no `ov update` needed. The
     acceptance gate becomes the CDP-driven full-notebook run
     described below (see "Notebook-only changes — CDP-driven gate").
   * **Image / overthink.yml / DAG-author-cell / helper-Python /
     anything else**: re-verify on a FRESH `ov update versa -i
     ecovoyage` (with image rebuild via `ov image build
     '@github.com/overthinkos/overthink/versa:main'` when the
     image-side changed). A fix that passes only on a hand-patched
     target is not a real fix — it's a regression waiting for the
     next unrelated rebuild. Pasteable proof of the fresh-rebuild
     re-verification is the acceptance gate.

**A `--dry-run` does NOT count as an R10 test.** Dry-run renders
prompts / scope / plans WITHOUT invoking the runner or producing
artifacts — it proves nothing about runtime behavior. R10 requires a
FULL live run of every new or changed surface against the running
container (freshly-rebuilt for image / config changes; warm-running
for pure-notebook changes).

**A REBUILD by itself does NOT count as an R10 test either.** The
rebuild is preflight setup. R10 means the cutover's NEW or CHANGED
code path — the runner / verb evaluation / subprocess / deploy_eval
overlay / NEW notebook cell — actually executed AGAINST the target
and produced output you pasted.

**The 9-bullet R10 acceptance battery** runs against the SAME fresh
rebuild. Every bullet required:

1. **`ov eval live versa-ecovoyage`** — primary acceptance probe.
   Runs the full three-section live battery: image probes baked into
   upstream versa (flow through automatically) + layer probes from
   upstream's 18 layers + the NEW workspace `eval:` overlay declared
   in this workspace's `overthink.yml`. Every probe must pass.
2. **`ov eval kind deploy`** when the change touched
   `overthink.yml` — per-kind R10 sequence dispatcher.
3. **`ov eval kind image`** when the change rebuilt the image —
   exercises image probes plus a fresh
   `ov image build '@github.com/overthinkos/overthink/versa:<ref>'`.
4. **`ov status versa -i ecovoyage`** — `Active: active (running)`,
   every declared port green, tunnel mode matches `overthink.yml`.
5. **`ov logs versa -i ecovoyage` (pipe to `tail -200` host-side; `ov logs` itself has no --tail flag)** — clean of
   crash-loops, supervisord restarts, Python tracebacks.
6. **Marimo MCP round-trip** —
   `mcp__marimo__get_active_notebooks` returns session,
   `get_lightweight_cell_map` shows expected cells,
   `get_notebook_errors` empty, `get_cell_runtime_data` on cell
   `MJUe` resolves every `*_PUBLIC_URL`, `lint_notebook` clean.
7. **Airflow REST round-trip** — JWT dance (lines 216–238),
   `GET /api/v2/dags?limit=30` lists all 6 self-authored DAGs, at
   least one triggered reaches `state: success`.
8. **chrome-devtools-ecovoyage CDP MCP — full map-render proof**:
   - `navigate_page` to notebook URL (HTTPS — tailnet listener is
     TLS-only).
   - `wait_for` notebook loaded.
   - `list_network_requests` (fetch+xhr) — every tile fetch HTTP
     200, vector-tile content-type, payload > 0. No 4xx/5xx.
   - `list_console_messages` (`types=["error","warn"]`) clean of
     MapLibre / sprite / glyph / WebGL diagnostics.
   - `evaluate_script` running tile-server-health snippet (lines
     270–291) — every martin source returns non-empty tile at
     `minzoom` inside `bounds`.
   - For each of 5 MapLibre maps + 1 folium map (lines 319–326):
     DOM-walk + visual confirmation.
9. **`--force-seed` re-verification** when seeded files touched:
   commit local edits first (lines 305–309), then
   `ov update --force-seed versa -i ecovoyage`, then repeat bullets
   1, 4, 6, 7, 8 on the post-seed container.

10. **`cd /home/atrawog/Atrapub/ecovoyage && ov image build versa`
    with NO flags MUST succeed.** This proves the workspace
    `overthink.yml`'s `include: '@github.com/overthinkos/overthink/overthink.yml:main'`
    resolves end-to-end and the upstream `versa` image rebuilds
    locally from cached source — no operator flags, no per-host
    setup. The fresh `ghcr.io/overthinkos/versa:<calver>` tag must
    land in local podman storage. This is the
    fully-automated-build-from-workspace gate.

All ten bullets execute against the SAME fresh rebuild. Pasteable
output for each is the deliverable. Missing any bullet caps the
tier at `analysed on a live system`, never
`fully tested and validated`.

### Notebook-only changes — CDP-driven gate (relaxed R10)

When the cutover touched ONLY `notebooks/*.py` files (no image, no
overthink.yml, no upstream layer, no Python helper outside the
notebook itself), the fresh-`ov update` requirement above is
DROPPED. The acceptance gate is instead a **CDP-driven full-notebook
run** against the WARM container — drive the actual rendered browser
DOM to prove every cell executes correctly and every map renders
real tiles.

#### Step 0 — Pick the right browser pod (mandatory preflight)

The `chrome-devtools-ecovoyage` MCP at `localhost:9232/mcp` proxies
the chromium running inside `ov-sway-browser-vnc-ecovoyage`. THAT
pod has a tailscale sidecar (`ov-sway-browser-vnc-ecovoyage-
tailscale`, tailscale name `ecovoyage-browser`, reachable from the
ecovoyage tailnet); it CAN resolve `ac.armadillo-quail.ts.net` and
fetch `MARTIN_PUBLIC_URL` / `AIRFLOW_PUBLIC_URL` / etc.

The agent session may ALSO auto-load
`mcp__plugin_ov-selkies_chrome-devtools__*` tools — those point at
`ov-openclaw-sway-browser-pod`, a DIFFERENT pod with NO tailscale
sidecar. Using that pod for R10 yields `ERR_NAME_NOT_RESOLVED` on
every tailnet URL and the agent then talks itself into "browser-
side network quirk" — a textbook R1 violation. **Do not use those
tools for ecovoyage R10.** They drive the wrong browser.

Before the first CDP call, verify:

```bash
# 1. The correct browser pod is up + has its tailscale sidecar.
podman ps --format "{{.Names}}" | grep ov-sway-browser-vnc-ecovoyage
# Must list BOTH:
#   ov-sway-browser-vnc-ecovoyage
#   ov-sway-browser-vnc-ecovoyage-tailscale

# 2. The sidecar's tailscale is online + the pod can reach martin
#    (substitute the actual MARTIN_PUBLIC_URL host):
podman exec ov-sway-browser-vnc-ecovoyage-tailscale tailscale status \
  | head -3
podman exec ov-sway-browser-vnc-ecovoyage \
  curl -sk -o /dev/null -w "%{http_code}\n" \
       https://ac.armadillo-quail.ts.net:33000/austria-railway
# Must be HTTP 200. If 4xx/5xx — R1 RCA on the network path BEFORE
# any rendering work; never proceed with a half-broken browser pod.
```

If `chrome-devtools-ecovoyage` is in the `mcp__*` tool surface →
preferred. If not → see step 4's "raw HTTP MCP fallback" below
(drive the same `localhost:9232` endpoint via curl-based MCP
handshake — NOT a fall-back to the wrong-pod selkies CDP).

The notebook-change acceptance battery, in order:

1. **`ov status versa -i ecovoyage`** — still required. `Active:
   active (running)`; the container the CDP browser will hit must
   be up.
2. **Marimo MCP round-trip** — same as the full battery's bullet 6.
   `get_active_notebooks` shows a session for the edited file;
   `get_lightweight_cell_map` shows expected cells with
   `runtime_state=idle`, `has_output=true`, `has_errors=false`;
   `get_notebook_errors` empty; `lint_notebook` clean.
3. **Headless `marimo export ipynb --include-outputs`** of the
   edited notebook — proves Python-side wellformedness from a cold
   kernel. ALL cells must execute with 0 errors (use the JSON
   scanner pattern: load the .ipynb, grep `output_type == "error"`,
   assert empty).
4. **CDP per-zoom verification matrix.** Connect via the
   `chrome-devtools-ecovoyage` MCP at `localhost:9232/mcp` (after
   the Step 0 preflight passed). The protocol:

   **a. Iframe map-instance hook (helper-side requirement).** The
   `build_pipeline_maplibre_html` helper MUST emit
   `window.map_<js_var> = map_<js_var>;` after the
   `addControl(NavigationControl)` line. The MapLibre
   `const map_<js_var>` declared inside the iframe's `<script>` is
   block-scoped; without the window assignment the agent cannot
   reach `iframe.contentWindow.map_<js_var>.jumpTo(...)` from the
   parent page to drive zoom-stepping. Any helper change that
   regresses this assignment FAILS this step.

   **b. Per-source `maxzoom` configuration (helper-side requirement).**
   When the vector pmtiles are baked at `max_zoom=N` (austria-
   ecovoyage = 12, monaco = 14, etc.), the helper's source dict
   MUST declare `"maxzoom": N` so MapLibre auto-overzooms for
   display zoom > N. Without this, MapLibre fetches non-existent
   z=N+1 tiles, gets 4xx, and silently returns 0 features at high
   zoom — the cell looks fine at default zoom but breaks the moment
   the user scrolls in. Verify by reading the source dict literal
   in the cell's exported HTML.

   **c. Navigate + scroll the target iframe into view.**
   `navigate_page` to the notebook URL. `resize_page` to a known
   viewport (default 1280×900). `wait_for` the iframe's content
   marker. `evaluate_script` to find the iframe
   (`document.querySelectorAll('iframe')`), match on a content
   discriminator (e.g. inner HTML contains
   `satellite-bg-austria-ecovoyage`), call `scrollIntoView`.

   **d. Canonical test centers (Austria workspace).** Every
   zoom-by-zoom verification pass for an Austria-scoped notebook
   map runs the loop TWICE — once around each of these two centers,
   chosen to exercise every visual axis of the satellite-overlay
   style:

   | Center | `[lon, lat]` | Why |
   |---|---|---|
   | **Innsbruck** | `[11.392778, 47.267222]` | Alpine + rural. Steep tonal variation on the satellite imagery (snow / rock / forest). Has the Brenner mainline + branch lines + 6 tram lines + the Hungerburgbahn funicular (`railway=funicular`) + intense SAC-scale variety (T1 city walks through T5 alpine routes) + the national Inn-Radweg (`route=bicycle` long-distance) + dense `route=hiking` long-distance trails. Best for surfacing line-width / halo / colour-contrast issues. |
   | **Wien Hauptbahnhof** | `[16.377778, 48.185]` | Dense urban. Vienna's mainline rail hub (8+ platforms, multiple branch and S-Bahn lines), tram network, U-Bahn subway (`railway=subway`), Wiener Linien GTFS stops at high density. Best for surfacing urban-rail filter coverage, GTFS-dot clustering, text-label collision behaviour, double-track centre stripe rendering at z=14. |

   Run the sequence:
   ```
   for center in [Innsbruck, Wien Hauptbahnhof]:
       for z in [6, 8, 10, 12, 14]:
           ...
   ```
   yielding **10 screenshots + 10 feature-count records** per
   verification pass. A cutover that passes Innsbruck but fails
   Wien Hauptbahnhof (or vice versa) is NOT green — both centers
   must clear every assertion.

   **e. Per-zoom-per-center step.** For each `(center, z)`:
   - `evaluate_script` calls
     `iframe.contentWindow.map_<var>.jumpTo({center: <CENTER>,
     zoom: z, pitch: 0, bearing: 0})`. Pitch reset to 0 for the
     verification pass — camera tilt occlusion is a separate
     visual concern handled at the default-view check.
   - Wait 5 s for tiles to settle.
   - `evaluate_script` calls
     `map.queryRenderedFeatures(undefined, {layers: [<id>]}).length`
     for EVERY expected layer ID. Record counts.
   - `take_screenshot` to a known filename
     (`/tmp/<cell-id>-<center-slug>-z<z>.png`, e.g.
     `/tmp/sat-overlay-innsbruck-z10.png`,
     `/tmp/sat-overlay-wien-z14.png`). `podman cp` the file from
     the browser pod to the host so the agent can `Read` it.

   **f. Assertions per layer per zoom.** Each expected layer has
   a min-feature-count expectation per zoom (declared in the
   cell's docstring or surrounding skill). At z=6 country view in
   Austria, mainline rail must show >100 features (340 is
   typical). At z=14 single-coordinate city zoom, expect 0–N per
   layer depending on whether the viewport intersects features —
   but the layer must EXIST in the style
   (`map.getStyle().layers` contains its id). A layer that returns
   0 features at EVERY tested zoom AT BOTH CENTERS is a filter bug
   — fix BEFORE shipping.

   **g. Console + network checks at each (center, zoom).**
   `list_console_messages(types=["error","warn"])` must be clean
   of MapLibre / sprite / glyph / WebGL / tile-fetch diagnostics.
   The marimo-side preload-warning noise from `noise-*.png` /
   `gradient-*.png` is benign and may be ignored.
   `list_network_requests(resourceTypes=["xhr","fetch"])` must
   show every tile fetch HTTP 200 with the expected MIME
   (`application/x-protobuf` for martin vector tiles, `image/webp`
   for versatiles satellite raster, `application/json` for
   mapterhorn raster-DEM tilejson, font PBFs for glyphs).
   `ERR_NAME_NOT_RESOLVED` on any tailnet host → STOP, return to
   Step 0.

   **h. Visual inspection of each screenshot.** Pull the PNGs
   from the browser pod (`podman cp
   ov-sway-browser-vnc-ecovoyage:/tmp/<name>.png /tmp/<name>.png`)
   and `Read` them. Each frame must show:
   - The primary tier (railways) as the visual headline — bold,
     visible against satellite imagery, halo intact.
   - The secondary tiers (cycle + hiking long-distance routes)
     visible from z=6 with halo.
   - The tertiary tier (footpaths / SAC trails) appearing at the
     expected zoom thresholds without obscuring the satellite
     imagery.
   - No "broken polygons" — line layers must not render as solid
     fills, dashed layers must show dashes, dotted layers must
     show dots.
   - GTFS dot overlay uniform; text labels appearing at z≥11.

   Visual inconsistencies are R1 RCA triggers — query the layer's
   actual paint properties, compare against expected, fix in the
   style constant. Don't ship the cutover with a known-defect
   documented as "follow-up" in the commit body.
5. **Airflow REST round-trip** — only when the notebook touched a
   self-author DAG cell: confirm the freshly-written DAG file lands
   in `/workspace/dags/`, registers in `GET /api/v2/dags`, and
   reaches `state: success` on a triggered run.
6. **DuckDB / tile-output integrity** — only when the notebook
   change affects downstream artifacts: confirm
   `/workspace/duckdb/austria.duckdb` schemas are intact OR the
   relevant `austria-*.pmtiles` file refreshed, AS APPLICABLE to
   what the notebook touched.

Missing the CDP step caps the tier at `analysed on a live system`,
NEVER `fully tested and validated` — the CDP step IS what proves
the notebook actually runs end-to-end in the browser (the marimo
export alone doesn't execute the embedded MapLibre JS).

When the `chrome-devtools-ecovoyage` MCP is NOT in the agent
session's loaded tool surface (the `.mcp.json` entry registered
the server but the harness didn't auto-load its tools), the
procedure is NOT to skip the CDP step. The MCP server at
`localhost:9232/mcp` is still reachable; drive it via a raw HTTP
MCP handshake:

1. `POST localhost:9232/mcp` with JSON-RPC `initialize` → capture
   the `Mcp-Session-Id` response header.
2. `POST` `notifications/initialized` with the same session-id
   header.
3. `POST` `tools/call` with `name="navigate_page"` /
   `name="evaluate_script"` / `name="take_screenshot"` etc. The
   tool name + argument schema match the equivalent
   `mcp__chrome-devtools-ecovoyage__*` tools exactly. The
   Streamable HTTP transport returns either JSON or
   `event: message\ndata: <json>` SSE chunks — handle both.
4. Save the driver as a one-off Python script (e.g.
   `.cdp-driver.py`); delete it after the run so it doesn't end
   up in the commit.

DO NOT fall back to a different chrome-devtools-* MCP that happens
to be in the session's tool surface (e.g.
`mcp__plugin_ov-selkies_chrome-devtools__*` points at the wrong
browser pod, NO tailnet — every tailnet tile fetch fails). Using
the wrong pod's CDP and then explaining away the failures is the
exact R1 violation this protocol exists to prevent.

DO NOT substitute curl-against-tile-URLs for the CDP-side proof;
curl bypasses CORS, sees no JS, and tests a connection the user's
browser never makes.

The CDP step is mandatory for any cutover that touches a map
cell. If a hard infrastructure failure (the browser pod itself is
down, the tailscale sidecar is offline) prevents driving the
correct pod, STOP — fix the infrastructure first. Don't ship a
"tier downgraded because CDP skipped" commit when the agent had a
working pod available all along.

## Hard Cutover by Default — ONE PHASE, test EVERYTHING at the end

Every refactor / schema change / API rename / deprecation ships as
ONE PHASE — hard cutover, no intermediate coexistence, no
"verify this bit now and the next bit later". Multi-phase rollouts
that split a single refactor across conversation turns leave the
system half-migrated and un-testable. FORBIDDEN.

**Forbidden precisely:**

- **Committing intermediate states.** No `git commit` of a
  half-migrated tree. ONE atomic commit — schema + code + migration +
  fixtures + skill updates together.
- **Verifying / claiming success on an intermediate state.**
- **Splitting one cutover across conversation turns.** ABSOLUTELY
  FORBIDDEN, with NO exception. Once a plan is approved, it executes
  end-to-end through R10 in the same conversation. ALWAYS push as far
  as you can. Compact context and continue. Time / context / session
  budget are NEVER valid stop reasons.
- **Premature R10 launch.** Starting `ov update`, `ov image build`,
  `ov eval run` while ANY implementation task is `pending` or
  `in_progress`. R10 runs ONCE, AT THE END, against the FINAL code.

**Permitted equally precisely:**

- Intermediate in-memory states during implementation (compile-clean
  between edits is normal, NOT done).
- Transitional aliases / legacy-accepting paths DURING implementation
  (DELETED before the cutover ends).
- Cheap smoke between tasks (`ov image validate`, syntax checks).
  NOT the acceptance gate.

See `/ov-internals:cutover-policy` for the full anti-pattern catalog.

## Post-Execution Policies — what happens AFTER R10 passes

1. **Commit.** ONE atomic commit covering the entire cutover —
   every YAML edit, every doc edit, every new probe, every deletion,
   in a single `git commit`. Multiple commits for the same cutover
   are FORBIDDEN.
2. **AI attribution trailer.** EVERY commit ships with
   `Assisted-by: Claude (<confidence>)`. Tier determined by what was
   proven (see AI Attribution below).
3. **Push only if the user asked to push.** A successful R10 +
   commit is NOT implicit authorization to push.
4. **Working-tree cleanliness.** After commit, `git status` clean.
5. **Report.** Final message: what was committed, confidence tier
   with proof, whether pushed.

### If R10 fails

R10 failure is NOT a stopping point — it's a return-to-implementation
signal. Run `/ov-internals:root-cause-analyzer`, fix in the same
working tree, re-run FULL R10 from scratch, only commit when R10
passes end-to-end on the FINAL code.

## AI Attribution (Fedora Policy Compliant)

Per [Fedora AI Contribution Policy](https://docs.fedoraproject.org/en-US/council/policy/ai-contribution-policy/),
ALL commits MUST include `Assisted-by: Claude (<confidence>)` trailer.

| Confidence | When to Use |
|---|---|
| `fully tested and validated` | Full 9-bullet R10 battery (above) pasted on a fresh `ov update versa -i ecovoyage` rebuild |
| `analysed on a live system` | Live invocation of the runner ran AND output pasted, but some R10 bullets skipped. NEVER use for `--dry-run`-only |
| `syntax check only` | Compile + validators / dry-run / parse passed; live runner did NOT execute. HONEST default; do NOT commit at this tier |
| `theoretical suggestion` | FORBIDDEN as shipped-code tier |

**Any rule violation FORBIDS commit.** A violation of R1, R2, R3,
R4, R5, R6, R7, R8, R9, R10, OR Clean Architecture means: NO commit,
at any tier, with any wording. There is no "downgrade tier and ship
anyway" path. The agent's only authorized responses to a known
violation: (a) fix in the same working tree + re-run verification, or
(b) escalate and STOP. Suggesting any other path is itself a
violation.

---

## Eval testing surface — workspace-owned, github-rebuildable

This workspace ships a single `overthink.yml` that declares the
`versa/ecovoyage` deploy and references the upstream `versa` image at
the highest kind level via the `@github.com/...` schema. Four eval
entry points:

```bash
ov eval image versa                    # baked image-section probes only
ov eval live versa-ecovoyage           # full three-section battery
ov eval kind deploy                    # per-kind R10 sequence dispatcher
ov image build '@github.com/overthinkos/overthink/versa:main'
                                       # local rebuild from upstream
                                       # github source — clones overthink,
                                       # reads upstream image.yml + 18
                                       # layers, builds locally. Result
                                       # tagged ghcr.io/overthinkos/versa:
                                       # <calver> + :latest.
```

The workspace adds **INSTANCE-level `eval:` probes** on top of the
~11 baked image-level probes that flow through automatically via the
upstream image. The new probes (in `overthink.yml`) cover:

- **Host-port HTTP reachability** for all 5 SPA/tile services (martin
  TileJSON, versatiles serve, pmtiles viewer, versatiles frontend,
  maputnik).
- **Airflow REST** unauth `/api/v2/version` probe + authenticated DAG
  inventory listing (JWT dance asserts all 6 self-authored DAGs).
- **Marimo MCP** ping + tool-catalog probe via the `mcp:` verb.
- **Workspace bind-mount integrity** (seeded notebook + dags dir).
- **Per-instance env vars** (`AIRFLOW_ADMIN_PASSWORD`,
  `AIRFLOW_DAGS_DIR`).
- **Container identity** (podman inspect → running).
- **Tailscale serve mappings** registered for this instance.

For browser-driven map render verification (CDP), see the R10 battery
bullet 8 above — that surface is invoked by Claude during R10, not by
`ov eval live`. The YAML probes cover the network/transport layer;
CDP covers what the browser actually renders.

**Why github-schema rebuild matters**: every `ov image build` against
the `@github.com/...` ref produces a deterministic image from
upstream source. Operators with `:main` track HEAD; pinning to
`:v<calver>` or a SHA gives reproducible eval. The workspace
overthink.yml's `data_source:` field independently pins the seed-data
OCI ref so seeded notebooks/DAGs are reproducible across image
rebuilds.

---

**Discover everything else** — don't hardcode the host workspace
path, port allocations, tailnet hostname, admin password, marimo
session ID, or cell IDs:

```bash
ov status versa -i ecovoyage      # container name, ports, volumes, tunnel
# The `bind:` line under Volumes gives:
#   bind: <HOST_PATH> -> /workspace
# That HOST_PATH is the working directory CLAUDE.md sits in; in any
# shell rooted there, `$PWD` is equivalent — prefer `$PWD` over
# typing the path out.
```

If you ever need an absolute host path in a command, derive it:

```bash
HOST_WS=$(ov status versa -i ecovoyage | awk '/bind:/ {print $2}')
```

Inside the container, every workspace file lives under `/workspace`
(e.g. `/workspace/notebooks/osm-monaco-viz.py`,
`/workspace/dags/...`, `/workspace/tiles/...`). The same files appear
on the host under `$HOST_WS/...`.

## Service discovery — DO NOT HARDCODE

URLs, host ports, and the tailnet hostname rotate per deployment.
`port: [auto]` in `deploy.yml` reassigns host ports on every
`ov update`; the tailnet name comes from whichever host Tailscale is
running on; the admin password is generated per instance. Read the
live config every session — don't trust values from CLAUDE.md or
prior transcripts.

**Stable across deployments** (safe to assume):

- Image name `versa`, instance name `ecovoyage` (the deploy identity).
- Container name pattern `ov-<image>-<instance>` → `ov-versa-ecovoyage`.
- Container-internal ports (defined by the image): marimo 2718,
  martin 3000, maputnik/pmtiles-viewer/versatiles-frontend
  8000-8002, airflow 8080, versatiles-serve 8090.
- Cell IDs in `notebooks/osm-monaco-viz.py` (stable per notebook revision).
- The 6 DAG IDs the notebook self-authors
  (`notebook_osm_pipeline`, `notebook_gtfs_pipeline`,
  `notebook_osm_gpqtiles_pipeline`,
  `notebook_osm_duckdb_mvt_pipeline`,
  `notebook_osm_duckdb_freestiler_pipeline`,
  `notebook_osm_shortbread_pipeline`).
- URL patterns within each tile server: martin `/{source}/{z}/{x}/{y}`,
  versatiles serve `/tiles/{source-stem}/{z}/{x}/{y}`.

**Per-deployment — always discover**:

### 1. The notebook's diagnostic cell is the canonical truth

Cell `MJUe` resolves every browser-facing URL from env vars
(`MARTIN_PUBLIC_URL`, `AIRFLOW_PUBLIC_URL`, `VERSATILES_PUBLIC_URL`,
`VERSATILES_STYLE_PUBLIC_URL`, `VERSATILES_ASSETS_PUBLIC_URL`,
`PMTILES_VIEWER_PUBLIC_URL`) and exports the resolved values as cell
variables. Read them via the marimo MCP — those are exactly the URLs
the user's browser is fetching:

```python
# Pseudocode for the marimo MCP call sequence
sid = mcp__marimo__get_active_notebooks()["notebooks"][0]["session_id"]
data = mcp__marimo__get_cell_runtime_data(session_id=sid, cell_ids=["MJUe"])
# → data[0]["variables"]["martin"]["value"]            == tile-server URL
# → data[0]["variables"]["airflow_public"]["value"]    == airflow UI/REST URL
# → data[0]["variables"]["versatiles_public"]["value"] == versatiles serve URL
# → data[0]["variables"]["versatiles_assets"]["value"] == /style /fonts /styler base
# → data[0]["variables"]["pmtiles_viewer"]["value"]    == PMTiles viewer URL
# → data[0]["variables"]["airflow_api_internal"]["value"] == kernel-side airflow URL
```

If the cell ID `MJUe` has changed (notebook re-seeded), find the
current one by searching `get_lightweight_cell_map` for the
"Diagnostic cell — displays the URLs" preview line.

### 2. Host-port + container-name discovery from the deploy

```bash
ov status versa -i ecovoyage        # one-shot: ports, status, tunnel, volumes
ov status versa                     # default instance (no -i)
```

`ov status` is the canonical surface — it lists container name,
container→host port mappings, tunnel mode (tailscale all-ports vs
none), volumes, and tool-probe results in one call. Prefer it over
poking the container engine directly.

### 3. Tailnet hostname + which ports are tailnet-exposed

```bash
tailscale status --json | python3 -c \
  'import sys,json;print(json.load(sys.stdin)["Self"]["DNSName"].rstrip("."))'
# → e.g. ac.armadillo-quail.ts.net  (THIS host only)

tailscale serve status --json | python3 -c \
  'import sys,json,re
d=json.load(sys.stdin)
for h,c in (d.get("Web") or {}).items():
    for p,t in (c.get("Handlers") or {}).items():
        print(h, "->", t.get("Proxy",""))'
```

The tailnet listener is **HTTPS-only** — a plain `http://` request to
the tailnet IP/hostname returns "Client sent an HTTP request to an
HTTPS server". The local-loopback listener (`http://127.0.0.1:<port>`)
is plain HTTP. The notebook resolves URLs to whichever the
`*_PUBLIC_URL` env vars are set to in the container.

### 4. Airflow admin password (per-instance secret)

```bash
ov cmd versa -i ecovoyage "echo \$AIRFLOW_ADMIN_PASSWORD"
```

(Username is always `admin`; password is regenerated when the deploy
is re-provisioned. The escaped `\$` ensures the host shell hands the
literal `$` to the container shell for expansion.)

## MCP servers (host-port form, reachable from this directory)

This workspace's `.mcp.json` registers two HTTP MCP servers:

- `marimo` — `http://localhost:32718/mcp/server` — **10 read-only**
  inspection tools (`get_active_notebooks`, `get_lightweight_cell_map`,
  `get_cell_runtime_data`, `get_cell_outputs`, `get_cell_dependency_graph`,
  `get_tables_and_variables`, `get_database_tables`,
  `get_notebook_errors`, `lint_notebook`, `get_marimo_rules`).
  `run_stale_cells` / `edit_notebook` exist only inside marimo's
  in-editor chat panel (Agent Mode), not on this transport.
- `chrome-devtools-ecovoyage` — `http://localhost:9232/mcp` — full CDP
  surface (navigate / click / type / screenshot / evaluate_script /
  network + console inspection / lighthouse / perf-trace) bound to the
  `ecovoyage-browser` sway-browser-vnc sidecar.

**Airflow has no MCP server** — the upstream REST→MCP wrapper was
removed in 2026-05 (no v2 release). Drive it via REST `/api/v2` on
host port **38080** (see Airflow recipe in the playbook below).

The marimo MCP server name deliberately did NOT change in the 2026-05
image rename — it reflects upstream-software identity, not OUR image
identity. See `/ov-versa:versa` "MCP Name Decoupling".

## Tool playbook — when to use which tool

### Reading / inspecting the running notebook

| Need | Use | Why |
|---|---|---|
| List active notebook sessions + IDs | `mcp__marimo__get_active_notebooks` | Returns `session_id` needed by every other marimo MCP call. |
| Cell overview (IDs, states, has-output, has-errors) | `mcp__marimo__get_lightweight_cell_map` | Cheap structural map; preview lines configurable. |
| Cell source + variables + execution time | `mcp__marimo__get_cell_runtime_data` | Authoritative view of what a cell currently holds in the live kernel. |
| Cell display outputs (visual + console) | `mcp__marimo__get_cell_outputs` | The visible UI output of a cell — better than scraping the DOM. |
| All errors organised by cell | `mcp__marimo__get_notebook_errors` | First stop when debugging — beats hunting through console messages. |
| Lint warnings | `mcp__marimo__lint_notebook` | Marimo's own static check. |

### Editing the notebook

| Need | Use | Why |
|---|---|---|
| Modify cell code | `Edit` / `Write` on `notebooks/osm-monaco-viz.py` | Marimo notebooks are plain Python files. The running kernel hot-reloads on file change. There is no MCP write tool. |
| Author / commit new cells | Same — Edit/Write the `.py` | Marimo's command-palette / "edit cell" surfaces are browser-only. |

### Executing cells

| Need | Use | Why |
|---|---|---|
| Re-run interactively in the live session | Browser tab at `<marimo_url>/?file=notebooks%2Fosm-monaco-viz.py` (per-cell run buttons; no global "Run all" exists — marimo is reactive). Resolve `<marimo_url>` from the marimo MCP `get_active_notebooks` or from `tailscale serve status --json`. | Source of truth for what the user sees. |
| Headless cold-start run (CI-like, produces .ipynb with outputs) | `ov cmd versa -i ecovoyage "/home/user/.pixi/envs/default/bin/marimo export ipynb /workspace/notebooks/osm-monaco-viz.py --include-outputs --sort topological -o /workspace/.run-output.ipynb -f"` (the `/home/user/.pixi/envs/default/bin/marimo` path is image-defined — `marimo` is not on the default PATH). Verified working. Spawns a fresh kernel — does NOT mutate the live session. Output lands inside the workspace bind at `<HOST_WS>/.run-output.ipynb`. |

**`ov cmd` quoting**: the inner command is ONE positional argument
(quoted string). `ov cmd <image> -i <instance> "<command-as-one-string>"`
is the verified shape. A `--` separator before the command is rejected
as `unexpected argument`.

### Driving Airflow (no MCP available)

1. Resolve the URL + password dynamically (don't hardcode):
   ```bash
   AIRFLOW=$(ov cmd versa -i ecovoyage "echo \$AIRFLOW_PUBLIC_URL" | tr -d '\r')
   PW=$(ov cmd versa -i ecovoyage "echo \$AIRFLOW_ADMIN_PASSWORD" | tr -d '\r')
   ```
2. Get a JWT:
   ```bash
   TOKEN=$(curl -sk -X POST -H 'Content-Type: application/json' \
     -d "{\"username\":\"admin\",\"password\":\"$PW\"}" \
     "$AIRFLOW/auth/token" \
     | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
   ```
3. Call REST endpoints with `Authorization: Bearer $TOKEN`:
   ```bash
   curl -sk -H "Authorization: Bearer $TOKEN" "$AIRFLOW/api/v2/dags?limit=30"
   ```
   `/api/v2/version` is the only unauthenticated endpoint; everything
   else returns 401 without a JWT. If you need the kernel-side URL
   (e.g. for a notebook cell making server-side calls), read
   `AIRFLOW_API_INTERNAL_URL` instead — same `ov cmd … "echo \$VAR"`
   pattern.

### Browser-testing maps + tile servers — use CDP, not curl

All browser-facing behaviour (map rendering, tile fetches, sprite/glyph
loading, sources / styles / CORS) must be tested through the
`chrome-devtools-ecovoyage` MCP against the live notebook. Curl
bypasses CORS, sees no JS, and tests a connection the user's browser
never makes — wrong tool for verifying what the user sees.

Tools live under `mcp__chrome-devtools-ecovoyage__*`. The full debug
surface:

| Tool | Use it for |
|---|---|
| `navigate_page` | Open the notebook URL (HTTPS — the tailnet listener is TLS-only; plain `http://` hits the wrong protocol and returns "Client sent an HTTP request to an HTTPS server"). Resolve the URL from `get_active_notebooks` + `tailscale status --json`. |
| `list_pages` / `select_page` | Switch between open tabs. |
| `list_network_requests` | THE primary tile-fetch debugger. Filter `resourceTypes=["fetch","xhr"]` to see every MapLibre tile request, every Airflow REST call, every style/sprite/glyph fetch. 4xx/5xx is the smoking gun. Includes timing. |
| `get_network_request` | Drill into one request: full URL, headers, response body, status, MIME type. Use this to verify tile content is `application/x-protobuf` or `vnd.mapbox-vector-tile` and non-empty. |
| `list_console_messages` (types=`["error","warn"]`) | MapLibre's own diagnostics — broken sprite URLs, font fallback, vector-tile parsing errors, style-load failures, WebGL warnings. |
| `get_console_message` | Drill into a single console entry with full args. |
| `evaluate_script` | Reach into the page DOM. Map iframes live under `<marimo_url>/@file/...` (same-origin), so you can fetch their HTML, walk their `<canvas>` / `.leaflet-tile` / SVG-path counts, probe `window.maplibregl` / `window.VersaTilesStyle`, or trigger a `fetch()` from inside the page to test tile endpoints exactly as the map would. |
| `take_snapshot` | a11y-tree text snapshot — preferred over screenshots when you need to find a button or input by label. |
| `take_screenshot` | Visual confirmation only. The file lands in the sidecar container's `/tmp`, not the host's — not useful for diagnostic detail; use network + console + DOM inspection instead. |
| `wait_for` | Wait for a piece of text to appear after `navigate_page`. |
| `click` / `type_text` / `fill` / `press_key` / `hover` | UI interactions — driving the marimo per-cell run buttons, opening the command palette, etc. |
| `performance_start_trace` / `performance_stop_trace` / `performance_analyze_insight` | Tile-rendering perf / WebGL frame timing. |
| `lighthouse_audit` | A11y / perf / best-practices audit for the notebook page. |
| `list_pages` + `new_page` | Spawn a fresh tab to PMTiles viewer / maputnik / Airflow UI without losing the notebook session. |

**Tile-server health via CDP** (replaces ad-hoc curl):

```javascript
// In evaluate_script — runs in the live page, sees the same network
// stack the map uses (CORS, cookies, TLS chain, redirects all real).
// Resolve URLs from the notebook's diagnostic cell variables.
async (martinUrl, versatilesUrl) => {
  const results = {};
  const cat = await fetch(`${martinUrl}/catalog`).then(r => r.json());
  results.martin_sources = Object.keys(cat.tiles);
  for (const src of results.martin_sources) {
    const meta = await fetch(`${martinUrl}/${src}`).then(r => r.json());
    const [w, s, e, n] = meta.bounds;
    // Hit a tile at meta.minzoom that's actually inside the bbox
    const z = meta.minzoom;
    const lon = (w + e) / 2, lat = (s + n) / 2;
    const x = Math.floor(((lon + 180) / 360) * 2 ** z);
    const y = Math.floor(((1 - Math.asinh(Math.tan(lat * Math.PI / 180)) / Math.PI) / 2) * 2 ** z);
    const r = await fetch(`${martinUrl}/${src}/${z}/${x}/${y}`);
    results[src] = { z, x, y, status: r.status, ct: r.headers.get('content-type'), bytes: (await r.blob()).size };
  }
  return results;
}
```

Pass `martinUrl` etc. as `args` to `evaluate_script` after resolving
them from the diagnostic cell — never embed literal URLs in the
script body. Martin returns HTTP 204 (empty body) for tiles outside
the source's bbox; the snippet above samples the center of `bounds`
at `minzoom` to avoid that false-negative.

**Per-cell iframe-index → cell-id mapping** is stable per notebook
revision but NOT across notebook re-seeds. Don't hardcode the list —
derive it from `get_lightweight_cell_map` (cells with map-rendering
code preview) and the iframe DOM order.

### Force-seed semantics

`ov update --force-seed versa -i ecovoyage` OVERWRITES
`<HOST_WS>/notebooks/osm-monaco-viz.py` (and other seeded files)
via a `cp -a` seeder container. Git history is the recovery surface
— commit local edits before force-seeding.

## Notebook map cells

The notebook produces 5 MapLibre maps + 1 folium map. The pipeline /
backend mapping is notebook content (stable per revision); the actual
host names/ports each fetches come from the diagnostic-cell variables
(`martin`, `versatiles_public`, `versatiles_assets`) at runtime — never
hardcode them.

| Pipeline | Backend (variable from diagnostic cell) | Tile source name |
|---|---|---|
| Streets via tippecanoe + MapLibre + 3D terrain | `martin` + external `tiles.mapterhorn.com` | `monaco` |
| gpq-tiles direct GeoParquet → PMTiles | `martin` | `monaco-gpqtiles` |
| DuckDB ST_AsMVT + pmtiles.Writer | `martin` | `monaco-duckdb-mvt` |
| DuckDB → freestiler (Rust tiling engine) | `martin` | `monaco-duckdb-freestiler` |
| Shortbread schema via Tilemaker | `versatiles_public` (tiles) + `versatiles_assets/fonts/` (glyphs) | `monaco-shortbread` |
| GTFS transit (bus stops) | folium / Leaflet on default OSM raster (external) | — |

To find which marimo cell renders which map, walk
`get_lightweight_cell_map` and match preview text — the cell IDs are
4-char tokens in the .py file and rotate when the notebook is
regenerated.

The 6 self-authored Airflow DAGs are: `notebook_osm_pipeline`,
`notebook_gtfs_pipeline`, `notebook_osm_gpqtiles_pipeline`,
`notebook_osm_duckdb_mvt_pipeline`,
`notebook_osm_duckdb_freestiler_pipeline`,
`notebook_osm_shortbread_pipeline`. List + status via the JWT recipe
above; never assume they're present — re-seeded notebooks can change
the set.

## Stale references in this file

`.mcp.json` registers MCP servers at literal ports
(currently 32718 for marimo, 9232 for chrome-devtools). If `port:
[auto]` in `deploy.yml` rotates those, the entries in `.mcp.json`
need to be edited to match — they are NOT auto-discovered. After any
`ov update`, run `ov status versa -i ecovoyage` and reconcile.

## Shell gotcha

The `Bash` tool sometimes re-execs nested `for` loops with
empty-string / multi-form path arguments through a non-bash
interpreter, producing `command not found: curl`. Force bash for
non-trivial scripts:

```bash
/bin/bash -c '... for path in "/" "/tiles" ...; do curl ...; done ...'
```
