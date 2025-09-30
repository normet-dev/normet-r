#' Apply rolling window meteorological normalisation
#'
#' \code{nm_rolling} performs meteorological normalisation on data using a rolling window approach.
#' A single model is trained on the entire dataset, and then normalisation is applied to
#' sequential windows of that data.
#'
#' @param df Data frame containing the input data.
#' @param model Pre-trained model for normalisation. If not provided, a model will be trained.
#' @param value The target variable name as a string.
#' @param feature_names The names of the features used for normalisation.
#' @param variables_resample The names of variables to be resampled during normalisation.
#' @param split_method The method for splitting data into training and testing sets.
#' @param fraction The proportion of the data to be used for training.
#' @param backend The modeling backend to use if a model needs to be trained. Default is 'h2o'.
#' @param model_config A list containing configuration parameters for model training.
#' @param n_samples Number of times to sample the data for normalisation.
#' @param window_days The size of the rolling window in days.
#' @param rolling_every The interval at which the rolling window is applied in days.
#' @param seed A random seed for reproducibility.
#' @param n_cores Number of CPU cores to use for parallel processing.
#' @param memory_save Logical for memory-efficient normalisation.
#' @param verbose Should the function print progress messages and logs?
#'
#' @return A data frame containing the combined results from all rolling windows.
#'
#' @export
nm_rolling <- function(df = NULL, model = NULL, value = 'value', feature_names = NULL,
                       variables_resample = NULL, split_method = 'random', fraction = 0.75,
                       backend = 'h2o', model_config = NULL, n_samples = 300,
                       window_days = 14, rolling_every = 7, seed = 7654321,
                       n_cores = NULL, memory_save = FALSE, verbose = TRUE) {

  log <- nm_get_logger("analysis.rolling")

  set.seed(seed)
  n_cores <- ifelse(is.null(n_cores), parallel::detectCores() - 1, n_cores)

  # If using the H2O backend, ensure it is initialized
  if (backend == 'h2o') {
    nm_init_h2o(n_cores, verbose = verbose)
  }

  # Train a model if one is not provided
  if (is.null(model)) {
    if (verbose) log$info("No model provided, training a new one for the rolling analysis...")
    build_results <- nm_build_model(
      df = df,
      value = value,
      backend = backend,
      feature_names = feature_names,
      split_method = split_method,
      fraction = fraction,
      model_config = model_config,
      seed = seed,
      n_cores = n_cores,
      verbose = verbose
    )
    df <- build_results$df_prep
    model <- build_results$model
  }

  if (verbose) log$info("Extracting model features once for all rolling windows...")
  model_features <- nm_extract_features(model)

  # --- Rolling Window Setup ---
  df <- df %>% dplyr::mutate(date_d = as.Date(date))
  date_max <- max(df$date_d, na.rm = TRUE) - lubridate::days(window_days - 1)
  all_dates <- unique(df$date_d[df$date_d <= date_max])
  rolling_dates <- all_dates[seq(1, length(all_dates), by = rolling_every)]

  if (verbose) {
    pb <- progress::progress_bar$new(
      format = "  Rolling window [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = length(rolling_dates), clear = FALSE, width = 80
    )
  }

  rolling_results <- list()

  # --- Apply the rolling window approach ---
  for (i in seq_along(rolling_dates)) {
    ds <- rolling_dates[i]

    # Filter data for the current window
    dfa <- df %>%
      dplyr::filter(date_d >= ds & date_d < (ds + lubridate::days(window_days)))

    if (nrow(dfa) > 0) {
      tryCatch({
        # Normalize the data within the window, passing pre-fetched features
        dfar <- nm_normalise(
            dfa, model, feature_names, variables_resample, n_samples,
            replace = TRUE, aggregate = TRUE, seed = seed, n_cores = n_cores,
            memory_save = memory_save, verbose = FALSE, model_features = model_features
        )

        # Rename the 'normalised' column to be unique for this window
        dfar <- dfar %>%
          dplyr::select(date, normalised) %>%
          dplyr::rename(!!paste0('rolling_', i) := normalised)

        rolling_results[[i]] <- dfar

      }, error = function(e) {
        log$warn("Error during normalisation for window %d (%s): %s", i, ds, e$message)
        rolling_results[[i]] <- NULL
      })
    }

    if (verbose) pb$tick()
  }

  # --- Aggregate results ---
  rolling_results <- Filter(Negate(is.null), rolling_results)

  if (length(rolling_results) == 0) {
    log$warn("No rolling windows were successfully processed.")
    return(data.frame())
  }

  # Merge all results by date
  combined_results <- rolling_results %>%
    purrr::reduce(dplyr::full_join, by = "date")

  return(combined_results)
}
