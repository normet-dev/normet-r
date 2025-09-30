#' Create a Configuration for a Single Normalisation Run
#'
#' @param value The target variable name.
#' @param backend The modeling backend (e.g., 'h2o').
#' @param feature_names Character vector of feature names.
#' @param variables_resample Character vector of variables to resample.
#' @param split_method Method for splitting data.
#' @param fraction Proportion of data for training.
#' @param model_config List of model configuration parameters.
#' @param n_samples Number of samples for normalisation.
#' @param aggregate Logical flag to return aggregated results.
#' @param seed Random seed for reproducibility.
#' @param n_cores Number of CPU cores.
#' @param memory_save Logical flag for memory-efficient normalisation.
#' @param verbose Should the function print progress messages?
#'
#' @return A named list of configuration options for `nm_do_all`.
#' @export
nm_config_single <- function(value, backend = 'h2o', feature_names = NULL,
                             variables_resample = NULL, split_method = "random",
                             fraction = 0.75, model_config = NULL, n_samples = 300,
                             aggregate = TRUE, seed = 7654321, n_cores = NULL,
                             memory_save = FALSE, verbose = TRUE) {

  list(
    value = value, backend = backend, feature_names = feature_names,
    variables_resample = variables_resample, split_method = split_method,
    fraction = fraction, model_config = model_config, n_samples = n_samples,
    aggregate = aggregate, seed = seed, n_cores = n_cores,
    memory_save = memory_save, verbose = verbose
  )
}

#' Create a Configuration for an Uncertainty Normalisation Run
#'
#' @param ... Parameters passed to `nm_config_single`.
#' @param n_models Number of models to train for uncertainty estimation.
#' @param confidence_level The confidence level for uncertainty estimation.
#' @return A named list of configuration options for `nm_do_all_unc`.
#' @export
nm_config_unc <- function(..., n_models = 10, confidence_level = 0.95) {
  # Get the base configuration from the single run config
  base_config <- nm_config_single(...)

  # Add or overwrite with uncertainty-specific parameters
  unc_config <- c(base_config, list(
    n_models = n_models,
    confidence_level = confidence_level
  ))

  unc_config$aggregate <- NULL

  return(unc_config)
}


#' Create a Configuration for a Rolling Window Normalisation Run
#'
#' @description
#' This function creates a structured list of configuration options, specifically
#' for running a rolling window normalisation with `nm_rolling` or the `nm_run_workflow`
#' function with `mode = "rolling"`.
#'
#' @param value The target variable name as a string.
#' @param backend The modeling backend to use if a model needs to be trained (e.g., 'h2o').
#' @param feature_names The names of the features used for training and normalisation.
#' @param variables_resample The names of variables to be resampled during normalisation.
#' @param split_method Method for splitting data for model training (e.g., 'random').
#' @param fraction Proportion of data for training if a model is trained.
#' @param model_config A list of configuration parameters for model training.
#' @param n_samples Number of times to sample the data for normalisation.
#' @param window_days The size of the rolling window in days.
#' @param rolling_every The interval at which the rolling window is applied in days.
#' @param seed A random seed for reproducibility.
#' @param n_cores Number of CPU cores for parallel processing.
#' @param memory_save Logical flag for memory-efficient normalisation.
#' @param verbose Should the function print progress messages and logs?
#'
#' @return A named list of configuration options for `nm_rolling`.
#' @export
nm_config_rolling <- function(value, backend = "h2o", feature_names = NULL,
                              variables_resample = NULL, split_method = "random",
                              fraction = 0.75, model_config = NULL, n_samples = 300,
                              window_days = 14, rolling_every = 7, seed = 7654321,
                              n_cores = NULL, memory_save = FALSE, verbose = TRUE) {
  list(
    value = value, backend = backend, feature_names = feature_names,
    variables_resample = variables_resample, split_method = split_method,
    fraction = fraction, model_config = model_config, n_samples = n_samples,
    window_days = window_days, rolling_every = rolling_every, seed = seed,
    n_cores = n_cores, memory_save = memory_save, verbose = verbose
  ) #
}


# --- Workflow Registry ---
# This named list maps a "mode" string to the actual R function to be executed.
.WORKFLOW_REGISTRY <- list(
  single = nm_do_all,
  unc = nm_do_all_unc,
  rolling = nm_rolling
)


# --- Main Entry Point ---

#' Run a Meteorological Normalisation Workflow
#'
#' This is the main entry point for running any of the package's normalisation workflows.
#' You specify the workflow `mode` and provide a corresponding `config` object.
#'
#' @param df The input data frame.
#' @param mode The workflow to run. One of "single", "unc", or "rolling".
#' @param config A configuration list created by one of the `nm_config_*` functions
#'   (e.g., `nm_config_single()`).
#'
#' @return The results from the chosen workflow function.
#'
#' @examples
#' \dontrun{
#' # --- Example 1: Run a single normalisation ---
#' my_config_single <- nm_config_single(
#'   value = "pollutant",
#'   feature_names = c("temp", "humidity"),
#'   n_samples = 50 # Use a small number for a quick example
#' )
#' single_results <- nm_run_workflow(my_data, mode = "single", config = my_config_single)
#'
#' # --- Example 2: Run an uncertainty analysis ---
#' my_config_unc <- nm_config_unc(
#'   value = "pollutant",
#'   feature_names = c("temp", "humidity"),
#'   n_samples = 50,
#'   n_models = 5 # Use a small number for a quick example
#' )
#' unc_results <- nm_run_workflow(my_data, mode = "unc", config = my_config_unc)
#' }
#' @export
nm_run_workflow <- function(df, mode, config) {

  # Look up the runner function from the registry
  if (!mode %in% names(.WORKFLOW_REGISTRY)) {
    stop("Unknown mode '", mode, "'. Available modes are: ", paste(names(.WORKFLOW_REGISTRY), collapse = ", "))
  }
  runner <- .WORKFLOW_REGISTRY[[mode]]

  # Combine the data frame and the config list into a single list of arguments
  args_list <- c(list(df = df), config)

  # Use do.call to execute the function with the prepared arguments
  do.call(runner, args_list)
}
