#' Decompose Time Series Influences
#'
#' @description
#' `nm_decompose` is a high-level wrapper that performs time series decomposition,
#' separating a target variable (e.g., pollutant concentration) into components
#' driven by emissions/trends and meteorology.
#'
#' @details
#' This function supports two decomposition methods:
#' \itemize{
#'   \item \strong{`emission`}: This method isolates the influence of time-based features
#'     (long-term trend, seasonality, day-of-week, and hour-of-day patterns).
#'     The result is a breakdown of the "emissions-driven" or "human activity" signal.
#'   \item \strong{`meteorology`}: This method isolates the influence of individual
#'     meteorological features (e.g., temperature, wind speed) based on their
#'     model-derived importance. The result is a breakdown of the
#'     "meteorology-driven" signal.
#' }
#' If a pre-trained `model` is not provided, the function will first train one using
#' the provided `df`, `value`, `feature_names`, and other training-related arguments.
#'
#' @param method The decomposition method to use. One of `"emission"` or `"meteorology"`.
#' @param df Data frame containing the input data.
#' @param model Optional pre-trained model for decomposition. If `NULL`, a model will be trained.
#' @param value The target variable name as a string.
#' @param backend The modeling backend to use if a model needs to be trained (e.g., 'h2o').
#' @param feature_names The names of the features used for training and decomposition.
#' @param split_method Method for splitting data for model training (e.g., 'random').
#' @param fraction Proportion of data for training if a model is trained.
#' @param model_config A list of configuration parameters for model training.
#' @param n_samples Number of samples for the normalisation process underlying the decomposition.
#' @param seed A random seed for reproducibility.
#' @param importance_ascending Logical. If `TRUE`, sorts meteorological features by
#'   ascending importance. (Used only when `method = "meteorology"`).
#' @param n_cores Number of CPU cores for parallel processing.
#' @param memory_save Logical flag for memory-efficient normalisation.
#' @param verbose Should the function print progress messages and logs?
#'
#' @return A data frame with the decomposed components.
#'
#' @examples
#' \dontrun{
#' # Assuming 'my_data' is a dataframe with 'date', 'pollutant', 'temp', 'wind'
#' features <- c("temp", "wind", "date_unix", "day_julian", "weekday", "hour")
#'
#' # Run emission decomposition
#' df_emi <- nm_decompose(
#'   method = "emission",
#'   df = my_data,
#'   value = "pollutant",
#'   feature_names = features,
#'   n_samples = 100 # Use fewer samples for a quick example
#' )
#' head(df_emi)
#'
#' # Run meteorology decomposition
#' df_met <- nm_decompose(
#'   method = "meteorology",
#'   df = my_data,
#'   value = "pollutant",
#'   feature_names = features,
#'   n_samples = 100
#' )
#' head(df_met)
#' }
#' @export
nm_decompose <- function(method = "emission",
                         df = NULL,
                         model = NULL,
                         value = 'value',
                         feature_names = NULL,
                         backend = NULL,
                         split_method = 'random',
                         fraction = 0.75,
                         model_config = NULL,
                         n_samples = 300,
                         seed = 7654321,
                         importance_ascending = FALSE,
                         n_cores = NULL,
                         memory_save = FALSE,
                         verbose = TRUE) {

  # --- 1. Validate Common Inputs ---
  if (is.null(df) || is.null(value)) stop("`df` and `value` must be provided.")
  if (is.null(model) && is.null(feature_names)) stop("Either `model` or `feature_names` must be provided.")
  if (is.null(model) && is.null(backend)) stop("When training a model, `backend` must be specified.")

  # --- 2. Dispatch Based on Method ---
  if (method == "emission") {
    return(nm_decom_emi(
      df = df, model = model, value = value, feature_names = feature_names,
      backend = backend, split_method = split_method, fraction = fraction,
      model_config = model_config, n_samples = n_samples, seed = seed,
      n_cores = n_cores, memory_save = memory_save, verbose = verbose
    ))
  }

  if (method == "meteorology") {
    return(nm_decom_met(
      df = df, model = model, value = value, feature_names = feature_names,
      backend = backend, split_method = split_method, fraction = fraction,
      model_config = model_config, n_samples = n_samples, seed = seed,
      importance_ascending = importance_ascending,
      n_cores = n_cores, memory_save = memory_save, verbose = verbose
    ))
  }

  # --- 3. Unsupported Method ---
  stop(sprintf("Unsupported decomposition method: '%s'. Use 'emission' or 'meteorology'.", method))
}




#' Decompose Emissions Influences (Implementation)
#'
#' \code{nm_decom_emi} performs decomposition of emissions influences on a time series.
#' This version is updated to match the more robust Python implementation logic.
#'
#' @param df Data frame containing the input data.
#' @param model Pre-trained model for decomposition. If not provided, a model will be trained.
#' @param value The target variable name as a string.
#' @param backend The modeling backend to use if a model needs to be trained.
#' @param feature_names The names of the features used for training and decomposition.
#' @param split_method Method for splitting data. Default is 'random'.
#' @param fraction Proportion of data for training. Default is 0.75.
#' @param model_config A list of configuration parameters for model training.
#' @param n_samples Number of samples for normalisation. Default is 300.
#' @param seed A random seed for reproducibility. Default is 7654321.
#' @param n_cores Number of CPU cores for parallel processing.
#' @param memory_save Logical for memory-efficient normalisation. Default is FALSE.
#' @param verbose Should the function print progress messages and logs? Default is TRUE.
#'
#' @return The decomposed data frame with columns including `observed`, individual time-based
#'   contributions, `emi_total`, `emi_noise`, and `emi_base`.
#'
nm_decom_emi <- function(df = NULL, model = NULL, value = 'value',
                         feature_names = NULL, backend = NULL,
                         split_method = 'random', fraction = 0.75,
                         model_config = NULL, n_samples = 300, seed = 7654321,
                         n_cores = NULL, memory_save = FALSE, verbose = TRUE) {

  log <- nm_get_logger("analysis.decompose.emissions")
  h2o::h2o.no_progress()

  # --- 1. Validate Inputs ---
  if (is.null(df) || is.null(value)) stop("`df` and `value` must be provided.")
  if (is.null(model) && is.null(feature_names)) stop("Either `model` or `feature_names` must be provided.")
  if (is.null(model) && is.null(backend)) stop("When training a model, `backend` must be specified.")

  # --- 2. Prepare Data ---
  df_work <- df %>%
    nm_process_date() %>%
    dplyr::filter(!is.na(date) & !is.na(.data[[value]])) %>%
    dplyr::arrange(date)

  observed_series <- df_work[[value]]
  if (value != "value") {
    df_work <- df_work %>% dplyr::rename(value = all_of(value))
  }

  # --- 3. Train Model if Needed ---
  if (is.null(model)) {
    if (verbose) log$info("Training model via backend='%s'...", backend)
    build_results <- nm_build_model(
      df = df_work, value = "value", backend = backend, feature_names = feature_names,
      split_method = split_method, fraction = fraction, model_config = model_config,
      seed = seed, n_cores = n_cores, verbose = verbose
    )
    df_work <- build_results$df_prep
    model <- build_results$model
  }

  # --- 4. Extract Model Features ---
  model_feats <- tryCatch(nm_extract_features(model), error = function(e) feature_names)
  model_feats <- intersect(model_feats, colnames(df_work))
  if (length(model_feats) == 0) stop("No valid model features found in the provided `df`.")

  result <- data.frame(date = df_work$date, observed = df_work$value)

  # --- 5. Time-Based Decomposition ---
  time_vars_order <- c("date_unix", "day_julian", "weekday", "hour")
  decomp_vars <- c("base", intersect(time_vars_order, model_feats))

  if (verbose) {
    pb <- progress::progress_bar$new(
      format = "  Decomposing [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = length(decomp_vars), clear = FALSE, width = 80
    )
  }

  tmp_results <- list()
  current_features_to_resample <- model_feats

  for (var_to_freeze in decomp_vars) {
    if (verbose) pb$tick()
    if (var_to_freeze != "base") {
      current_features_to_resample <- setdiff(current_features_to_resample, var_to_freeze)
    }

    df_norm <- nm_normalise(
      df = df_work, model = model, feature_names = model_feats,
      variables_resample = current_features_to_resample, n_samples = n_samples,
      seed = seed, n_cores = n_cores, memory_save = memory_save, verbose = FALSE
    )
    tmp_results[[var_to_freeze]] <- dplyr::right_join(df_norm, result["date"], by = "date")$normalised
  }

  result <- cbind(result, as.data.frame(tmp_results))

  # --- 6. Recompose Components ---
  result$emi_total <- result[[decomp_vars[length(decomp_vars)]]] %||% result$observed

  recomp_pairs <- list(
    c("hour", "weekday"), c("weekday", "day_julian"),
    c("day_julian", "date_unix"), c("date_unix", "base")
  )
  for (pair in recomp_pairs) {
    a <- pair[1]; b <- pair[2]
    if (a %in% colnames(result) && b %in% colnames(result)) {
      result[[a]] <- result[[a]] - result[[b]]
    }
  }

  base_mean <- mean(result$base, na.rm = TRUE)
  result$emi_noise <- result$base - base_mean
  result$emi_base <- base_mean
  result$base <- NULL

  return(result)
}


#' Decompose Meteorological Influences (Implementation)
#'
#' \code{nm_decom_met} performs decomposition of meteorological influences on a time series.
#' This version is updated to match the more robust Python implementation logic.
#'
#' @param df Data frame containing the input data.
#' @param model Pre-trained model for decomposition. If not provided, a model will be trained.
#' @param value The target variable name as a string.
#' @param backend The modeling backend to use if a model needs to be trained.
#' @param feature_names The names of the features used for training and decomposition.
#' @param split_method Method for splitting data. Default is 'random'.
#' @param fraction Proportion of data for training. Default is 0.75.
#' @param model_config A list of configuration parameters for model training.
#' @param n_samples Number of samples for normalisation. Default is 300.
#' @param seed A random seed for reproducibility. Default is 7654321.
#' @param importance_ascending Logical, sort feature importances in ascending order. Default is FALSE.
#' @param n_cores Number of CPU cores for parallel processing.
#' @param memory_save Logical for memory-efficient normalisation. Default is FALSE.
#' @param verbose Should the function print progress messages and logs? Default is TRUE.
#'
#' @return A data frame with decomposed components including `observed`, `emi_total`, individual
#'   meteorological contributions, `met_total`, `met_base`, and `met_noise`.
#'
nm_decom_met <- function(df = NULL, model = NULL, value = 'value',
                         feature_names = NULL, backend = NULL,
                         split_method = 'random', fraction = 0.75,
                         model_config = NULL, n_samples = 300, seed = 7654321,
                         importance_ascending = FALSE, n_cores = NULL,
                         memory_save = FALSE, verbose = TRUE) {

  log <- nm_get_logger("analysis.decompose.met")
  h2o::h2o.no_progress()

  # --- 1. Validate Inputs ---
  if (is.null(df) || is.null(value)) stop("`df` and `value` must be provided.")
  if (is.null(model) && is.null(feature_names)) stop("Either `model` or `feature_names` must be provided.")
  if (is.null(model) && is.null(backend)) stop("When training a model, `backend` must be specified.")

  # --- 2. Prepare Data ---
  df_work <- df %>%
    nm_process_date() %>%
    dplyr::filter(!is.na(date) & !is.na(.data[[value]])) %>%
    dplyr::arrange(date)

  observed_series <- df_work[[value]]
  if (value != "value") {
    df_work <- df_work %>% dplyr::rename(value = all_of(value))
  }

  # --- 3. Train Model if Needed ---
  if (is.null(model)) {
    if (verbose) log$info("Training model via backend='%s'...", backend)
    build_results <- nm_build_model(
      df = df_work, value = "value", backend = backend, feature_names = feature_names,
      split_method = split_method, fraction = fraction, model_config = model_config,
      seed = seed, n_cores = n_cores, verbose = verbose
    )
    df_work <- build_results$df_prep
    model <- build_results$model
  }

  # --- 4. Extract and Filter Features ---
  feat_sorted <- tryCatch(
    nm_extract_features(model, importance_ascending = importance_ascending),
    error = function(e) feature_names
  )
  feat_sorted <- intersect(feat_sorted, colnames(df_work))
  if (length(feat_sorted) == 0) stop("No valid model features found in `df`.")

  time_vars <- c("hour", "weekday", "day_julian", "date_unix")
  contrib_candidates <- feat_sorted[!feat_sorted %in% time_vars]

  result <- data.frame(date = df_work$date, observed = df_work$value)

  # --- 5. Iterative Decomposition ---
  decomp_order <- c("emi_total", contrib_candidates)
  resample_vars <- contrib_candidates
  tmp_results <- list()

  if (verbose) {
    pb <- progress::progress_bar$new(
      format = "  Decomposing [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = length(decomp_order), clear = FALSE, width = 80
    )
  }

  for (var_to_freeze in decomp_order) {
    if (verbose) pb$tick()

    df_norm <- nm_normalise(
      df = df_work, model = model, feature_names = feat_sorted,
      variables_resample = resample_vars, n_samples = n_samples,
      seed = seed, n_cores = n_cores, memory_save = memory_save, verbose = FALSE
    )

    tmp_results[[var_to_freeze]] <- dplyr::right_join(df_norm, result["date"], by = "date")$normalised

    if (var_to_freeze != "emi_total") {
      resample_vars <- setdiff(resample_vars, var_to_freeze)
    }
  }

  # --- 6. Recompose Meteorological Components ---
  result$emi_total <- tmp_results[["emi_total"]]

  prev_key <- "emi_total"
  for (feat in contrib_candidates) {
    result[[feat]] <- tmp_results[[feat]] - tmp_results[[prev_key]]
    prev_key <- feat
  }

  result$met_total <- result$observed - result$emi_total
  result$met_base <- mean(result$met_total, na.rm = TRUE)

  contrib_sum <- if (length(contrib_candidates) > 0) {
    rowSums(result[, contrib_candidates, drop = FALSE], na.rm = TRUE)
  } else {
    0.0
  }
  result$met_noise <- result$met_total - (result$met_base + contrib_sum)

  return(result)
}
