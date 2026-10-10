# Chart interactions and automatic FIRE calculations

## Shared charts

`components/Chart.tsx` centralizes chart behavior for Dashboard, Portfolio, Allocation, Flows and FIRE. `chart-options.ts` derives legend swatches from actual series styles: lines have no artificial point, and dashed/dotted styles retain their color. Pie legends represent slices.

- Click a legend item to toggle it.
- Double-click to isolate it; double-click the isolated item again to restore all entries.
- Shift+Enter provides keyboard isolation.
- Drag a rectangle to zoom. Disable selection mode to use ordinary chart interactions. Reset with the button or Escape.

Cartesian charts use data-coordinate zoom on both axes. Pie, Sankey and treemap charts have no Cartesian axes, so they use a cropped/enlarged viewport instead, capped at 16x. Selection starts only after a movement threshold, preserving ordinary clicks such as treemap navigation. Financial axis labels preserve fractional thousands (for example `2 k` and `2,5 k`). Chart animations are disabled.

## FIRE

There is no calculate button. Every valid hypothesis edit immediately requests a deterministic projection. Monte Carlo runs automatically after a 150 ms quiet period to avoid sending hundreds of expensive simulations during a slider drag. Superseded waits and fetches are cancelled. Query keys include the entire parameter set, so a late response cannot replace a newer parameter set's result.

The previous result stays visible while updating, with a status message. Each result is paired with its own parameters; axes and threshold lines do not mix old results with new inputs. Invalid inputs block requests and show an error. Defaults cannot overwrite an already edited form or loaded scenario.

Monthly savings and expenses are normalized to cents for defaults, scenario loading and manual edits. Inputs use a 0.01 step. FIRE charts use a numeric age axis, including numeric stop-work/tipping markers. The Monte Carlo percentile band handles negative values without adding its invisible baseline to the legend.

## Month selection and layout

Flows offers calendar-month checkboxes grouped by year, from the first financial entry through the current/latest recorded month. Shortcuts select the last three available months, the current year, all months or none. A deliberate empty selection is not reset to the default. Portfolio has a 24 px separation between its wealth chart and trade history.

## Checks

Run the repository's standard Ruff, Pytest, Prettier, Vitest, TypeScript and Vite checks. New regression tests are in `tests/test_broker_imports.py`, `frontend/src/interactions.test.ts`, `frontend/src/imports.test.ts` and `frontend/src/api-errors.test.ts`.

Browser acceptance checks should cover legend single/double-click, rectangle selection on every chart type, Escape/reset, month selection and clearing, rapid FIRE slider edits, loading a saved scenario, and confirming/re-importing a synthetic CSV. Never use a live personal financial history as a public test fixture.
