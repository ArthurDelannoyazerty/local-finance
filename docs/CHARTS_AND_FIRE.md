# Charts, FIRE and trade entry

## Shared chart behavior

`components/Chart.tsx` is used by Dashboard, Portfolio, Allocation, Flows and FIRE. Legends use swatches matching the actual series: lines have no artificial point, and dashed/dotted styles keep their color. Pie legends represent slices.

- Click a legend item to toggle it.
- Double-click to isolate it; double-click it again to restore all entries.
- Shift+Enter provides keyboard isolation.

Chart animations remain disabled. Monetary axis labels preserve fractional thousands, for example `2 k` and `2,5 k`.

## Zoom: Portfolio only

Zoom is disabled by default. Only Portfolio's wealth-evolution chart passes `zoomable` to the shared chart component. Drag a rectangle to zoom both axes, then use Reset or Escape to return to the full range. Selection mode can be toggled off.

All other pages have no zoom toolbar, rectangle-selection handler or wheel/pinch zoom. Allocation also disables native treemap roaming, click-to-zoom and breadcrumbs. Legend interactions and hover details remain available.

## Appearance

Dashboard's expense donut uses zero slice padding, zero border width and no rounded ends. Small categories retain their proportional area instead of being hidden by decorative gaps.

Allocation account nodes have explicit stable IDs and colored fills, including the account-only view. Asset performance colors remain red/green independently of the account fill.

## Transfer flow bug

A period containing A -> B and B -> A previously sent a cycle to the ECharts Sankey layout. ECharts requires an acyclic graph and throws instead of drawing it. The investment flow has the same problem when a purchase and a sale appear in one period.

`flow-chart.ts` separates sending and receiving node identities into two columns for transfers and investments. All directed gross amounts are preserved; opposite flows are not netted or removed. Original account/asset labels appear in labels and escaped tooltips. Budget flows retain their existing acyclic structure.

Tests reproduce the original cycle failure using the real ECharts SVG renderer and verify successful rendering of reciprocal and three-node flows. Synthetic backend tests verify multi-month aggregation and date filtering. No live personal database is used.

## FIRE

Every valid hypothesis edit immediately requests a deterministic projection. Monte Carlo runs automatically after a 150 ms quiet period. Superseded waits and fetches are cancelled. Query keys contain the complete parameters, preventing a late response from replacing a newer result.

The previous result remains visible with an updating indicator. Each result is paired with its own parameters. Invalid inputs block requests and show an error. Defaults cannot overwrite an edited form or loaded scenario.

Monthly savings and expenses are normalized to cents for defaults, scenario loading and manual edits. Inputs use a 0.01 step. FIRE uses a numeric age axis and correctly handles negative Monte Carlo bands without exposing the invisible baseline in the legend.

## Navigation, months and trade duplication

The global Importer button and popup entry point are removed from all screens. Existing stored imports, importer services and the Excel workflow in Data are not deleted.

Flows retains month checkboxes grouped by year from the first financial entry, with shortcuts and a deliberate empty selection. Portfolio retains a 24 px separation before trade history.

Each trade-history row has a Duplicate button. It opens a new draft with the same ticker, name, account, action, quantity, price, fees and comment, defaulting to today's local date. All fields remain editable. Nothing is written until Save; Cancel changes nothing. Saving uses POST, not the original trade's PUT endpoint, and never copies IDs, revisions or importer metadata. Existing inventory validation still applies, including duplicated sales.

## Verification

Run Ruff, Pytest, Prettier, Vitest, TypeScript and Vite checks. Added regression tests cover rendering, zoom opt-in, navigation and duplication in `frontend/src/*test.*` and transfer aggregation in `tests/test_transfer_flows.py`.

Browser acceptance should additionally cover Portfolio rectangle selection/reset, legend interactions, editing a duplicate then cancelling/saving, checking the account-only allocation colors, and selecting months containing transfers in both directions.
