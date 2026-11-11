# Legacy transport publication overview

The opt-in template draws the legacy CANOE transport model, including its embedded
fuel supply. The editable SVG uses the original **1280 x 720** canvas. Its journal
starting size is **180 x 101.25 mm**, with minimum type 7.175 pt and 600-dpi PNG
exports. The ordinary v2 backend does not depend on this experiment or its renderer.

## Rebuild

From the repository root:

```powershell
uv run --offline --no-sync python scripts/experiments/legacy_transport_diagram/build.py

uv run --offline --no-sync python scripts/experiments/legacy_transport_diagram/build.py --render --inkscape 'C:\Program Files\Inkscape\bin\inkscape.exe'
```

The builder reuses existing PyYAML and pypdf and an installed Inkscape. It downloads
or installs nothing. `figure.yaml` owns presentation dimensions, colors, original
counts, source/mode selectors, carrier notation and node-type parameter fields.
`config/paths.yaml` owns the opt-in legacy artifact route. Core dependency files,
ETL entrypoints and the production workflow are unaffected.

## Deliverables

Canonical files are in `legacy_backend/diagrams/`:

- `canoe-legacy-transport.svg`: native vector source, with named layers and model IDs.
- `canoe-legacy-transport.pdf`: one-page vector export, embedded fonts and selectable text.
- `canoe-legacy-transport.png`: 4252 x 2392 pixels at 600 dpi.
- `captions.txt`: a short overview caption draft.
- `topology.json`: complete model membership, source hashes and positive pathway witnesses.
- `validation.json`: topology, type, contrast, actual text bounds and PDF checks.

The first generated bundle remains in `initial_attempt/`. The supplied original
SVG and the read-only compiled legacy database are unchanged. Rendering uses Arial
with embedded Verdana fallback for scientific glyphs. No SVG/PDF raster images,
external resources, web fonts or HTML labels are used.

## Overview and aggregation

Imported chemical fuels have prominent full-name input nodes. Familiar terms such
as Ren., CNG, LNG and H2 are retained. The orange area locates supply-chain and
direct fuel-use accounting. Fuel accounting and blends are collapsed into the
supply links rather than drawn as preparation nodes. Transport rows repeat their
eligible delivered-carrier abbreviations, derived from the actual legacy paths.
No abbreviation key is printed in the artwork; the author may define it in the
publication caption.

One spacious endogenous H2 enclosure shows steam reforming and electrolysis, with
a green model-produced H2 output. Gas and electricity enter across the boundary.
Electrolyser types, CCS branches, conditioning and delivery details are omitted
from the drawing but preserved in the topology and SVG ID audit. The separate
process-emission membership remains in the audit; the overview does not print its
own nested process envelope.

The four original family counts remain 3/54/10/57. They are presentation counts
from the supplied diagram, while the inspected compiled membership is reconciled
in `topology.json`. Class/mode rows each have their own end-use demand. Transit,
school and coach buses are separate; air and rail have distinct passenger and
freight rows. All eight passenger, six freight and one off-road demand commodities
are preserved individually. Repeated passenger-km/tonne-km labels never pool demand
across classes or modes. Borderless P/F labels are typographic flow annotations.

Other off-road demand sits at the top of the demand layer. It receives a direct
diesel path in PJ. `T_OFF` is recorded on that path as the legacy energy pass-through,
without a plotted vehicle/service transformation.

The only dashed link is the **Hourly charger load** annotation inside the light-duty
family, targeting its car/truck BEV subset. It uses the legacy `T_LDV_BEV_CHRG`
profile. The same charger also serves motorcycles in the database; that fact stays
in the audit and the visual hourly annotation is scoped to LDV car/truck BEVs as
requested. All other charging and mixed-fuel support is collapsed into electricity
supply, without a separate PHEV treatment in the figure.

The bottom-left node-type legend uses small technology/fuel glyphs, compact fields
and colored indexing dimensions. It retains the original technology/fuel parameter
capabilities. `EmissionEmbodied` is empty in the inspected build, so the vehicle-cycle
EF entry is a parameter capability rather than a claim of populated legacy rows.

## Validation boundary

Every run resolves all selected vehicle inputs and demands, enforces one mode
assignment for 115 vehicle/off-road technologies, and retains all 166 technology IDs
(162 drawn/aggregated and four folded annual bridges). Source hashes must remain
unchanged. Native SVG checks cover IDs, external/raster content, text contrast and
minimum physical type. Rendering measures every label and checks the one-page PDF,
embedded fonts, extractable text and zero image objects.

The final PDF-to-PNG preview is inspected under `tmp/legacy_diagram/`. One-time
revision acceptance also checked exact SVG/topology replay, 15 distinct demand nodes,
hourly BEV scope and orthogonal routes against unrelated node interiors. This
verifies the figure; it does not establish solver feasibility or numerical parity,
rebuild a database, or adapt the v2 model.
