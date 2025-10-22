# normet: Normalisation, Decomposition, and Counterfactual Modelling for Environmental Time-series

[![CRAN status](https://www.r-pkg.org/badges/version/normet)](https://CRAN.R-project.org/package=normet)
[![R-CMD-check](https://github.com/normet-dev/normet-r/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/normet-dev/normet-r/actions/workflows/R-CMD-check.yaml)

`normet` is an R package designed for **environmental time-series analysis**. It provides a powerful and user-friendly suite of tools for air quality research, causal inference, and policy evaluation.

---

## ✨ Core Strengths

* **Automated & Intelligent**: Powered by an **H2O AutoML** backend, it automatically finds the optimal model, eliminating tedious manual tuning.
* **All-in-One Solution**: Offers high-level functions that cover the entire workflow, from data preprocessing and model training to weather normalisation and counterfactual modelling.
* **Robust Causal Inference**: Integrates both classic and machine-learning-based Synthetic Control Methods (SCM) and provides multiple uncertainty quantification tools (Bootstrap, Jackknife, Placebo Tests) to ensure reliable conclusions.
* **Designed for Environmental Science**: Its features are built to address core challenges in air quality research, such as isolating meteorological impacts and evaluating policy effectiveness.

---

## 🚀 Workflow

The core workflow of `normet` is designed to simplify complex analytical steps:

1.  **Data Preparation (`nm_prepare_data`)**: Automatically processes time-series data, including imputation, feature engineering (e.g., time-based variables), and dataset splitting.
2.  **Model Training (`nm_train_model`)**: Trains high-performance machine learning models using H2O AutoML.
3.  **Analysis & Application**:
    * **Weather Normalisation (`nm_normalise`)**: Removes the influence of meteorological conditions on pollutant concentrations.
    * **Time-Series Decomposition (`nm_decompose`)**: Decomposes the series into meteorology-driven and emission-driven components.
    * **Counterfactual Modelling (`nm_run_scm`)**: Estimates the causal effect of an intervention (e.g., a new policy).

---

## 🔧 Installation

You can install the stable version of `normet` from CRAN:

Install the latest development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("normet-dev/normet-r")
```

### Backend Setup

`normet` relies on the **H2O** machine learning platform as its core backend. Please ensure you have Java installed in your environment and then install the `h2o` package:

```r
# Install the h2o package from CRAN
install.packages("h2o")
```

-----

## 💡 Quick Start: One-Shot Weather Normalisation

With the `nm_do_all` function, you can perform a complete weather normalisation workflow in just a few lines of code.

```r
library(normet)
library(dplyr) # For data manipulation

# Use the built-in example dataset
data("MY1")

# Define the feature variables for the model
predictors <- c(
  "u10", "v10", "d2m", "t2m", "blh", "sp", "ssrd", "tcc", "tp", "rh2m","date_unix", "day_julian", "weekday", "hour"
)

features_to_use <- c(
  "u10", "v10", "d2m", "t2m", "blh", "sp", "ssrd", "tcc", "tp", "rh2m"
)

# Run the end-to-end pipeline
# nm_do_all automatically handles data prep, model training, and normalisation
results <- nm_do_all(
  df = my1,
  value = "PM2.5",
  feature_names = predictors,
  variables_resample = features_to_use, # Specify met variables to resample to remove their effect
  n_samples = 100 # Use a small sample size for a quick demo
)

# View the normalised (deweathered) time-series results
cat("Normalised (deweathered) time-series:\n")
print(head(results$out))

# Inspect the trained AutoML model object
cat("\nTrained H2O AutoML Model:\n")
print(results$model)

# Evaluate the model's performance
stats <- nm_modStats(results$df_prep, results$model)
cat("\nModel Performance Metrics:\n")
print(stats)
```

The `nm_do_all` function returns a list containing three key elements:

1.  `out`: A data frame with the normalised (deweathered) time-series.
2.  `df_prep`: The preprocessed data, including training/testing splits.
3.  `model`: The trained H2O AutoML model object.


### Step-by-Step Execution

For more control over the process, you can execute each step manually.

#### 1\. Prepare the Data (`nm_prepare_data`)

This function handles missing value imputation, adds time-based features, and splits the data into training and testing sets.

```r
df_prep <- nm_prepare_data(
  df = my1,
  value = 'PM2.5',
  feature_names = features_to_use,
  split_method = 'random',
  fraction = 0.75
)
```

#### 2\. Train the Model (`nm_train_model`)

Train a machine learning model using H2O AutoML. The configuration allows you to control the training process.

```r
# Define all predictor variables
target <- 'value'

# Configure H2O AutoML
h2o_config <- list(
  max_models = 10,
  sort_metric = "RMSE",
  max_mem_size = "8G"
)

# Train the model
model <- nm_train_model(
  df = df_prep,
  value = target,
  backend = "h2o",
  variables = predictors,
  model_config = h2o_config
)

# Evaluate model performance
nm_modStats(df_prep, model)
```

#### 3\. Perform Normalisation (`nm_normalise`)

Use the trained model to generate the weather-normalised time-series.

```r
df_normalised <- nm_normalise(
  df = df_prep,
  model = model,
  feature_names = predictors,
  variables_resample = features_to_use,
  n_samples = 100
)

print(head(df_normalised))
```

#### 4\. Using a Custom Weather Dataset

You can also provide a specific weather dataset via the `weather_df` argument. This is useful for answering questions like, "What would concentrations have been under the average weather conditions of a different year?"

```r
# For demonstration, create a custom weather dataset using the first 100 rows
custom_weather <- df_prep %>%
  slice(1:100) %>%
  select(all_of(features_to_use))

# Perform normalisation using the custom weather conditions
df_norm_custom <- nm_normalise(
  df = df_prep,
  model = model,
  weather_df = custom_weather,
  feature_names = predictors,
  variables_resample = features_to_use,
  n_samples = 100 # n_samples will now sample from `custom_weather`
)

print(head(df_norm_custom))
```

-----

## 📊 Core Features Showcase

In addition to the high-level pipeline, `normet` offers flexible, modular functions for custom, step-by-step analyses.

### 1\. Weather Normalisation & Time-Series Decomposition

#### Rolling Weather Normalisation (`nm_rolling`)

Ideal for short-term trend analysis, this function performs normalisation within a moving time window to capture dynamic changes.

```r
# Assuming you have `results$df_prep` and `results$model` from the quick start
df_dew_rolling <- nm_rolling(
  df = df_prep,
  value = 'value',
  model = model,
  feature_names = predictors,
  variables_resample = features_to_use,
  n_samples = 100,
  window_days = 14,    # Window size in days
  rolling_every = 7      # Step size in days
)
print(head(df_dew_rolling))
```

#### Time-Series Decomposition (`nm_decompose`)

Decomposes the original time series into its **emission-driven** and **meteorology-driven** components.

```r
# Decompose to get the emission-driven component
df_emi <- nm_decompose(
  method = "emission",
  df = df_prep,
  value = "value",
  model = model,
  feature_names = predictors,
  n_samples = 100
)
print(head(df_emi))

# Decompose to get the meteorology-driven component
df_met <- nm_decompose(
  method = "meteorology",
  df = df_prep,
  value = "value",
  model = model,
  feature_names = predictors,
  n_samples = 100
)
print(head(df_met))
```

### 2\. Counterfactual Modelling & Causal Inference

`normet` includes a powerful toolkit for Synthetic Control Methods (SCM) to evaluate the causal impact of policies or events.

#### Data Preparation

```r
# Load the SCM example data
data("SCM")
df <- scm%>%
  filter(date >= as.Date('2015-05-01') & date < as.Date('2016-04-30'))

# Define the treated unit, donor pool, and intervention date
treated_unit <- "2+26 cities"
donor_pool <- c(
  "Dongguan", "Zhongshan", "Foshan", "Beihai", "Nanning", "Nanchang", "Xiamen",
  "Taizhou", "Ningbo", "Guangzhou", "Huizhou", "Hangzhou", "Liuzhou",
  "Shantou", "Jiangmen", "Heyuan", "Quanzhou", "Haikou", "Shenzhen",
  "Wenzhou", "Huzhou", "Zhuhai", "Fuzhou", "Shaoxing", "Zhaoqing",
  "Zhoushan", "Quzhou", "Jinhua", "Shaoguan", "Sanya", "Jieyang",
  "Meizhou", "Shanwei", "Zhanjiang", "Chaozhou", "Maoming", "Yangjiang"
)
cutoff_date <- as.Date("2015-10-23") # Define the intervention start date
```

#### Running a Synthetic Control Analysis (`nm_run_scm`)

```r
# Run classic SCM or the machine learning-based MLSCM
scm_result <- nm_run_scm(
  df = df,
  date_col = "date",
  outcome_col = "SO2wn",
  unit_col = "ID",
  treated_unit = treated_unit,
  donors = donor_pool,
  cutoff_date = cutoff_date,
  scm_backend = "scm" # Options: 'scm' or 'mlscm'
)
print(tail(scm_result))
```

#### Placebo Tests (`nm_placebo_in_space`)

Check the significance of the main effect by iteratively treating each control unit as the "treated" unit and running a "fake" intervention.

```r
placebo_results <- nm_placebo_in_space(
  df = df,
  date_col = "date",
  outcome_col = "SO2wn",
  unit_col = "ID",
  treated_unit = treated_unit,
  donors = donor_pool,
  cutoff_date = cutoff_date,
  scm_backend = "scm", Options: 'scm' or 'mlscm'
  verbose = FALSE
)

# Calculate confidence bands from the placebo effects
bands <- nm_effect_bands_space(placebo_results, level = 0.95, method = "quantile")

# Plot the main effect with the placebo bands
nm_plot_effect_with_bands(bands, cutoff_date = cutoff_date, title = "SCM Effect (95% placebo bands)")
```

#### Uncertainty Quantification (`nm_uncertainty_bands`)

Generate confidence intervals for the causal effect using **Bootstrap** or **Jackknife** methods.

```r
# Bootstrap method
boot_bands <- nm_uncertainty_bands(
  df = df,
  date_col = "date",
  outcome_col = "SO2wn",
  unit_col = "ID",
  treated_unit = treated_unit,
  donors = donor_pool,
  cutoff_date = cutoff_date,
  scm_backend = "scm", Options: 'scm' or 'mlscm'
  method = "bootstrap",
  B = 50 # Use a small number of replications for a quick demo
)
nm_plot_uncertainty_bands(boot_bands, cutoff_date = cutoff_date)

# Jackknife (leave-one-out) method
jack_bands <- nm_uncertainty_bands(
  df = df,
  date_col = "date",
  outcome_col = "SO2wn",
  unit_col = "ID",
  treated_unit = treated_unit,
  donors = donor_pool,
  cutoff_date = cutoff_date,
  scm_backend = "scm",
  method = "jackknife"
)
nm_plot_uncertainty_bands(jack_bands, cutoff_date = cutoff_date)
```

-----

## 📦 Dependencies

  * R (\>= 4.0)
  * **Core Dependencies**: `h2o`, `dplyr`, `data.table`, `lubridate`, `foreach`, `doSNOW`, `glmnet`, `quadprog`
  * **Suggested**: `lgr`, `progress`

-----

## 📜 How to Cite

If you use `normet` in your research, please cite it as follows:

```bibtex
@Manual{normet-pkg,
  title = {normet: Normalisation, Decomposition, and Counterfactual Modelling for Environmental Time-series},
  author = {Chao Song and Other Contributors},
  year = {2025},
  note = {R package version 0.0.1},
  organization = {University of Manchester},
  url = {[https://github.com/normet-dev/normet-r](https://github.com/normet-dev/normet-r)},
}
```

-----

## 📄 License

This project is licensed under the **GNU GENERAL PUBLIC LICENSE**.

-----

## 🤝 How to Contribute

Contributions are welcome\! This project is released with a Contributor Code of Conduct. By participating, you agree to abide by its terms.

Please submit bug reports and feature requests via the [GitHub Issues](https://github.com/normet-dev/normet-r/issues).
