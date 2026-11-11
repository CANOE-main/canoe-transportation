"""Read-only view of annual charging-profile normalization."""

import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    from validation.charging_profile_diagnostic import load_diagnostic, profile_chart
    return load_diagnostic, mo, profile_chart


@app.cell
def _(load_diagnostic):
    diagnostic = load_diagnostic()
    return (diagnostic,)


@app.cell
def _(diagnostic, mo, profile_chart):
    mo.vstack([
        mo.md("# How does annual utilization change the charging shape?\n\n"
              "The registered simulations are transformed with the production hourly normalization and scaling functions. "
              "The multiplier uses all 8,760 hours. The selected week only illustrates the unchanged timing. "
              "The dashed line marks the annual target, not a requirement that each week average 20%."),
        mo.ui.altair_chart(profile_chart(diagnostic), chart_selection=False, legend_selection=False),
        mo.ui.table(diagnostic.annual, selection=None, label="Full-year and illustrative-week checks"),
        mo.accordion({"Registered source identities and production selection": mo.json(diagnostic.metadata)}),
    ])
    return


if __name__ == "__main__":
    app.run()
