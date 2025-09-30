# [PackageName]: Normalisation, Decomposition, and Counterfactual Modelling for Environmental Time-series

[![CRAN status](https://www.r-pkg.org/badges/version/normet)](https://CRAN.R-project.org/package=normet)
[![R-CMD-check](https://github.com/normet-dev/normet-r/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/normet-dev/normet-r/actions/workflows/R-CMD-check.yaml)

`[PackageName]` is an R package for **environmental time-series analysis**. It provides a suite of tools for:

-   **Meteorological Normalisation** (de-weathering) of pollutant concentrations.
-   **Counterfactual Modelling** using the H2O AutoML backend.
-   **Synthetic Control Methods** (classic SCM and ML-based SCM).
-   **Uncertainty Quantification** via bootstrapping, jackknife, and placebo tests.
-   **Evaluation Metrics** tailored for environmental data.

The package is designed for **air-quality research**, **causal inference**, and **policy evaluation**.

---

## Features

-   High-level pipelines (`nm_do_all`, `nm_run_syn`) for normalisation and synthetic control.
-   Rolling weather normalisation (`nm_rolling`) for short-term trend analysis.
-   Time-series decomposition (`nm_decompose`) to separate emissions-driven and meteorology-driven variability.
-   A powerful, scalable backend using **H2O AutoML**.
-   Placebo-in-space (`nm_placebo_in_space`) and placebo-in-time (`nm_placebo_in_time`) analyses for robustness checks.
-   Bootstrap and jackknife-based uncertainty bands (`nm_uncertainty_bands`).
-   Rich evaluation metrics (`nm_modStats`): RMSE, FAC2, IOA, r, R², etc.
-   Parallel execution (`nm_scm_all`) for large panel datasets.

---

## Installation

You can install the released version of `normet` from CRAN with:

```r
install.packages("normet")
```

Or, install the development version from GitHub with:

```r
# install.packages("devtools")
devtools::install_github("[normet-dev/normet-r")
```

### Backend Setup

This package relies on the **H2O** backend, which must be installed separately. H2O requires a Java environment.

```r
# Install the h2o package from CRAN
install.packages("h2o")
```

---

## Quick Start

A quick example using the `nm_do_all` pipeline for meteorological normalisation:

```r
library([PackageName])

# Enable default logging to see progress
nm_enable_default_logging()

# Example dataset (must contain a date column, target, and predictors)
set.seed(123)
df <- data.frame(
  date = seq.POSIXt(as.POSIXct("2021-01-01"), by = "hour", length.out = 1000),
  pm25 = abs(rnorm(1000, 20, 10) + sin(1:1000 / 100) * 10),
  temp = rnorm(1000, 15, 5),
  wind = abs(rnorm(1000, 3, 2)),
  humidity = rnorm(1000, 60, 10)
)

# Run the full pipeline
results <- nm_do_all(
  df = df,
  value = "pm25",
  backend = "h2o",
  feature_names = c("temp", "wind", "humidity"),
  n_samples = 100 # Use fewer samples for a quick example
)

# The results are a list containing:
# 1. The normalised (deweathered) time-series
print(head(results$out))

# 2. The prepared dataset with splits & time-based features
print(head(results$df_prep))

# 3. The trained H2O AutoML model object
print(results$model)

# Evaluate model performance manually
stats <- nm_modStats(results$df_prep, results$model)
print(stats)
```

The `nm_do_all` pipeline performs three main steps:
1.  **Data preparation**: Parses the datetime column, validates features, imputes missing values, adds date-based covariates (hour, weekday, etc.), and splits data into training/testing sets.
2.  **Model training**: Trains a model using H2O AutoML.
3.  **Normalisation**: Resamples weather covariates and predicts the counterfactual ("deweathered") series.

---

## Advanced Examples

### Time-Series Decomposition

Decompose the time series into emissions-driven and meteorology-driven components.

```r
# Decompose to get emissions-driven components
emi <- nm_decompose(
  method = "emission",
  df = df,
  value = "pm25",
  feature_names = c("temp", "wind", "humidity"),
  n_samples = 100
)
print(head(emi))

# Decompose to get meteorology-driven components
met <- nm_decompose(
  method = "meteorology",
  df = df,
  value = "pm25",
  feature_names = c("temp", "wind", "humidity"),
  n_samples = 100
)
print(head(met))
```

### Causal Inference with Synthetic Control

Estimate the effect of an intervention on a single unit.

```r
# Create example panel data
set.seed(456)
df_panel <- data.frame(
  date = rep(seq.Date(as.Date("2020-01-01"), by = "day", length.out = 100), 4),
  city = rep(c("A", "B", "C", "D"), each = 100),
  pm25 = c(
    abs(rnorm(100, 20, 5) + c(rep(0, 70), rep(10, 30))), # City A is treated after day 70
    abs(rnorm(100, 22, 5)), # Donors
    abs(rnorm(100, 18, 5)),
    abs(rnorm(100, 25, 5))
  )
)

# Run Synthetic Control (SCM)
syn <- nm_run_syn(
  df = df_panel,
  date_col = "date",
  unit_col = "city",
  outcome_col = "pm25",
  treated_unit = "A",
  cutoff_date = "2020-03-11", # Day 71
  donors = c("B", "C", "D"),
  scm_backend = "scm" # 'scm' or 'mlscm'
)
print(tail(syn)) # Shows observed, synthetic, and effect columns

# Run a Placebo-in-Space test for significance
placebo <- nm_placebo_in_space(
  df = df_panel,
  date_col = "date",
  unit_col = "city",
  outcome_col = "pm25",
  treated_unit = "A",
  cutoff_date = "2020-03-11"
)
print(paste("Placebo P-Value:", round(placebo$p_value, 3)))
```

---

## Uncertainty Quantification for Causal Effects

Construct confidence bands for the treatment effect using bootstrap or jackknife methods.

```r
# Bootstrap version
boot <- nm_uncertainty_bands(
  df = df_panel,
  date_col = "date",
  unit_col = "city",
  outcome_col = "pm25",
  treated_unit = "A",
  cutoff_date = "2020-03-11",
  scm_backend = "scm",
  method = "bootstrap",
  B = 50 # Use a small number of replications for example
)
# This returns a list with the main effect ('treated') and bounds ('low', 'high')

# Plot the results
nm_plot_uncertainty_bands(boot, cutoff_date = "2020-03-11")

# Jackknife version (leave-one-donor-out)
jack <- nm_uncertainty_bands(
  df = df_panel,
  date_col = "date",
  unit_col = "city",
  outcome_col = "pm25",
  treated_unit = "A",
  cutoff_date = "2020-03-11",
  scm_backend = "scm",
  method = "jackknife",
  ci_level = 0.95
)
nm_plot_uncertainty_bands(jack, cutoff_date = "2020-03-11")

```

---

## Dependencies

-   R >= 4.0
-   **Main Dependencies**: `h2o`, `dplyr`, `data.table`, `lubridate`, `foreach`, `doSNOW`, `glmnet`, `quadprog`
-   **Suggested**: `lgr`, `progress`

---

## Citation

If you use `normet` in your research, please cite:

```
Song, C. Other (2025).
normet: Normalisation, Decomposition, and Counterfactual Modelling for Environmental Time-series.
University of Manchester. R package version [X.Y.Z].
[https://github.com/](https://github.com/)normet-dev/normet-r
```

---

## License

This project is licensed under the MIT License.

---

## Contributing

Contributions are welcome! Please note that this project is released with a Contributor Code of Conduct. By participating, you agree to abide by its terms.

Please submit bug reports and feature requests via the [GitHub issue tracker](https://github.com/normet-dev/normet-r/issues).
