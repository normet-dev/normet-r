#' Generate a Resampled Data Frame (data.table version)
#'
#' @keywords internal
nm_generate_resampled <- function(df, variables_resample, replace, seed, weather_df) {
  # This helper is called by parallel workers and receives data.tables.
  missing_cols <- setdiff(variables_resample, colnames(weather_df)) #
  if (length(missing_cols) > 0) {
    stop("`weather_df` is missing required columns: ", paste(missing_cols, collapse = ", ")) #
  }
  set.seed(seed) #
  sample_indices <- sample(nrow(weather_df), size = nrow(df), replace = replace) #

  # Create a copy to avoid modifying the original data.table in place by reference
  out <- data.table::copy(df) #

  # First, select the data to be assigned using the correct `..` prefix
  sampled_data <- weather_df[sample_indices, ..variables_resample] #

  # Then, assign the new data to the columns specified in the `variables_resample` vector
  # using the `:=` operator. The parentheses around `variables_resample` are crucial.
  out[, (variables_resample) := sampled_data] #

  out[, seed := seed] #
  return(out)
}


#' Normalise a Time Series Using a Trained Model
#'
#' \code{nm_normalise} is a high-level wrapper that deweathers a time series. It
#' dispatches to a specific implementation based on the model's backend attribute.
#'
#' @param df The input data frame.
#' @param model The trained model object, which should have a 'backend' attribute.
#' @param ... Additional arguments passed to the implementation function.
#'
#' @return A data frame containing the normalised results.
#' @export
nm_normalise <- function(df, model, ...) {
  # This dispatcher function is correct and requires no changes.
  log <- nm_get_logger("analysis.normalise")
  model_backend <- attr(model, "backend")
  if (!is.null(model_backend) && startsWith(model_backend, "h2o")) {
    if (("verbose" %in% names(list(...)) && list(...)$verbose) || is.null(list(...)$verbose)) {
       log$info("Dispatching to H2O backend for normalisation.")
    }
    return(nm_normalise_h2o(df = df, model = model, ...))
  } else {
    err_msg <- paste("Unsupported model backend for normalisation:", model_backend %||% "NULL")
    log$error(err_msg)
    stop(err_msg)
  }
}


#' Normalise Data using an H2O Model (Implementation)
#'
#' @description
#' `nm_normalise_h2o` is the specific implementation for normalising data using
#' a trained H2O model.
#'
#' @param df The input data frame or data.table.
#' @param model The trained model object, which should have a 'backend' attribute.
#' @param feature_names The names of the features used for training and decomposition.
#' @param variables_resample The names of variables to be resampled during normalisation.
#' @param n_samples Number of times to sample the data for normalisation.
#' @param replace Logical flag for resampling with or without replacement.
#' @param aggregate Logical flag to return aggregated results or individual samples.
#' @param seed A random seed for reproducibility.
#' @param n_cores Number of CPU cores to use for parallel processing.
#' @param weather_df A data frame to sample weather conditions from. If NULL, `df` is used.
#' @param memory_save Logical for memory-efficient normalisation.
#' @param verbose Should the function print progress messages and logs?
#' @param n_retries Number of times to retry a failed prediction batch.
#' @param model_features Optional character vector of features used by the model.
#' @param cl (Optional) An existing parallel cluster object. If provided, the function
#'   will use it instead of creating and destroying its own.
#'
#' @return A data frame containing the normalised results.
#' @keywords internal
#' Normalise Data using an H2O Model (Implementation)
#'
#' @keywords internal
nm_normalise_h2o <- function(df, model, feature_names, variables_resample = NULL, n_samples = 300,
                             replace = TRUE, aggregate = TRUE, seed = 7654321, n_cores = NULL,
                             weather_df = NULL, memory_save = FALSE, verbose = TRUE,
                             n_retries = 3, model_features = NULL, cl = NULL) {

  log <- nm_get_logger("analysis.normalise.h2o") #
  nm_require("data.table", hint = "install.packages('data.table')") #

  data.table::setDT(df) #
  if (!is.null(weather_df)) data.table::setDT(weather_df) #

  # --- Step 1: Preparation and Feature Extraction ---
  if (verbose) log$info("Step 1/4: Preparing data and validating features...") #

  df <- nm_process_date(df) #
  df <- nm_check_data(df, feature_names, 'value') #

  if (is.null(weather_df)) weather_df <- df #

  time_vars <- c("date_unix", "day_julian", "weekday", "hour") #
  if (is.null(variables_resample)) {
    variables_resample <- setdiff(feature_names, time_vars) #
  }
  if (is.null(model_features)) {
    model_features <- nm_extract_features(model) #
  }

  manage_cluster_locally <- is.null(cl) #

  if (manage_cluster_locally) {
    n_cores <- n_cores %||% (parallel::detectCores() - 1) #
    cl <- parallel::makeCluster(n_cores) #
    on.exit(parallel::stopCluster(cl), add = TRUE) #
  }

  # --- Step 2: Parallel Resampling and Prediction ---
  set.seed(seed) #
  random_seeds <- sample(1:1000000, n_samples, replace = FALSE) #

  if (verbose) {
    log$info("Step 2/4: Generating %d resamples in parallel...", n_samples) #
    pb <- progress::progress_bar$new(
      format = "  Generating samples [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = n_samples, clear = FALSE, width = 80
    ) #
  }

  doSNOW::registerDoSNOW(cl) #
  progress_opts <- list(progress = function(n) if(verbose) pb$tick()) #

  resampled_list <- foreach::foreach(
    s = random_seeds, .packages = c("data.table"),
    .export = "nm_generate_resampled", .options.snow = progress_opts
  ) %dopar% {
    nm_generate_resampled(df, variables_resample, replace, s, weather_df) #
  } #

  df_all <- data.table::rbindlist(resampled_list) #

  # --- Step 3: Prediction ---
  if (verbose) log$info("Step 3/4: Making single batch prediction on all resamples...") #
  preds <- nm_predict(model, df_all, verbose = verbose, feature_names = model_features) #

  df_result <- data.table::data.table(date = df_all$date, observed = df_all$value, normalised = preds, seed = df_all$seed) #

  # --- Step 4: Aggregate Results ---
  if (nrow(df_result) == 0) {
    log$error("All normalisation samples failed to produce results.") #
    stop("All normalisation samples failed.") #
  }

  if (aggregate) {
    if (verbose) log$info("Step 4/4: Aggregating %d predictions...", nrow(df_result)) #
    df_out <- df_result[, .(observed = mean(observed, na.rm = TRUE), normalised = mean(normalised, na.rm = TRUE)), by = date] #
  } else {
    if (verbose) log$info("Step 4/4: Formatting results into wide format...") #
    observed <- unique(df_result[, .(date, observed)]) #
    wide <- data.table::dcast(df_result, date ~ seed, value.var = "normalised") #
    df_out <- merge(observed, wide, by = "date", all = TRUE) #
  }

  if (verbose) log$info("Normalisation complete.") #

  return(as.data.frame(df_out)) #
}
