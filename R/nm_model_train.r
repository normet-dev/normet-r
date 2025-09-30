#' Train a model using a specified backend
#'
#' \code{nm_train_model} is a high-level wrapper function that dispatches the training
#' task to a specific backend implementation based on the `backend` parameter.
#' The default backend is 'h2o'.
#'
#' @param df Input data frame containing the data to be used for training.
#' @param value The target variable name as a string. Default is "value".
#' @param variables Independent/explanatory variables used for training the model.
#' @param model_config A list containing configuration parameters for model training.
#' @param backend The modeling framework to use. Default is 'h2o'. Others can be added in the future.
#' @param seed A random seed for reproducibility. Default is 7654321.
#' @param n_cores Number of CPU cores to use for the model training. Default is system's total minus one.
#' @param verbose Should the function print progress messages? Default is TRUE.
#'
#' @return The trained model object from the specified backend.
#'
#' @export
nm_train_model <- function(df, value = 'value', variables = NULL, model_config = NULL, backend = 'h2o',
                           seed = 7654321, n_cores = NULL, verbose = TRUE) {

  log <- nm_get_logger("model.train")

  if (verbose) log$info("Dispatching to backend for training: %s", backend)

  if (backend == 'h2o') {
    # If the backend is 'h2o', call the h2o-specific training function
    model <- nm_train_h2o(
      df = df,
      value = value,
      variables = variables,
      model_config = model_config,
      seed = seed,
      n_cores = n_cores,
      verbose = verbose
    )
    return(model)

  } else {
    err_msg <- paste("Unsupported backend:", backend)
    log$error(err_msg)
    stop(err_msg)
  }
}


#' Prepare Data and Train a Model
#'
#' \code{nm_build_model} prepares the input data and trains a model using a selected backend.
#' It serves as a wrapper around the `nm_prepare_data` and `nm_train_model` functions.
#'
#' @param df The raw input data frame.
#' @param value A string indicating the target column in `df`.
#' @param backend A string for the modeling backend. Defaults to 'h2o'.
#' @param feature_names A character vector of predictor columns to use. Must not be empty.
#' @param split_method A string for the data splitting strategy. Default is 'random'.
#' @param fraction A numeric value for the training fraction of the split. Default is 0.75.
#' @param model_config A list of backend-specific configurations to be passed to the trainer.
#' @param seed An integer for the random seed. Default is 7654321.
#' @param n_cores An integer for the number of CPU cores to use (primarily for H2O).
#' @param verbose A logical value to enable logging messages. Default is TRUE.
#' @param drop_time_features A logical value. If TRUE, helper time features
#'   (like "date_unix", "day_julian") are dropped. Default is FALSE.
#'
#' @return A list containing two elements:
#'   \item{df_prep}{The prepared data frame with training/testing sets.}
#'   \item{model}{The trained model object.}
#'
#' @export
nm_build_model <- function(df,
                           value,
                           backend = 'h2o',
                           feature_names = NULL,
                           split_method = "random",
                           fraction = 0.75,
                           model_config = NULL,
                           seed = 7654321,
                           n_cores = NULL,
                           verbose = TRUE,
                           drop_time_features = FALSE) {

  # Initialize logger
  log <- nm_get_logger("model.build")

  # --- 1. Validate Inputs ---
  if (is.null(feature_names) || length(feature_names) == 0) {
    err_msg <- "`feature_names` must be provided and non-empty."
    log$error(err_msg)
    stop(err_msg)
  }

  # --- 2. Select Features ---
  variables <- feature_names
  if (drop_time_features) {
    time_features_to_drop <- c("date_unix", "day_julian", "weekday", "hour")
    variables <- setdiff(variables, time_features_to_drop)
  }

  # --- 3. Prepare Data ---
  if (verbose) log$info("Preparing data for model training...")
  df_prep <- nm_prepare_data(
    df = df,
    value = value,
    feature_names = variables,
    split_method = split_method,
    fraction = fraction,
    seed = seed
  )

  # --- 4. Align Features ---
  variables <- intersect(variables, colnames(df_prep))
  if (length(variables) == 0) {
    err_msg <- "None of the requested features remain after data preparation. Check `feature_names` and your input columns."
    log$error(err_msg)
    stop(err_msg)
  }

  # --- 5. Resolve Target Column ---
  if ("value" %in% colnames(df_prep)) {
    target_col <- "value"
  } else if (value %in% colnames(df_prep)) {
    target_col <- "value"
    df_prep[["value"]] <- df_prep[[value]]
  } else {
    err_msg <- paste0("Target column not found after data preparation; tried 'value' and '", value, "'.")
    log$error(err_msg)
    stop(err_msg)
  }

  # --- 6. Ensure model_config is a valid list ---
  if (is.null(model_config)) {
    model_config <- list()
  } else if (!is.list(model_config)) {
    stop("`model_config` must be a list.")
  }

  # --- 7. Train Model ---
  model <- nm_train_model(
    df = df_prep,
    value = target_col,
    backend = backend,
    variables = variables,
    model_config = model_config,
    seed = seed,
    n_cores = n_cores,
    verbose = verbose
  )

  if (verbose) {
    log$info("Model trained with backend = %s", backend)
  }

  # --- 8. Return Results ---
  return(list(df_prep = df_prep, model = model))
}
