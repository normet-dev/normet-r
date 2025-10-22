#' Perform All Steps for Meteorological Normalisation
#'
#' @description
#' `nm_do_all` is a high-level convenience pipeline that prepares data, trains a model,
#' and runs the normalisation process.
#'
#' @param df The raw input data frame.
#' @param value The target variable name as a string.
#' @param backend The modeling backend to use for training. Default is 'h2o'.
#' @param feature_names The names of the features used for training and normalisation.
#' @param variables_resample Character vector of variables to resample during normalisation.
#' @param split_method Method for splitting data for model training (e.g., 'random').
#' @param fraction Proportion of data for training if a model is trained.
#' @param model_config A list of configuration parameters for model training.
#' @param n_samples Number of times to sample the data for normalisation.
#' @param seed A random seed for reproducibility.
#' @param n_cores Number of CPU cores for parallel processing.
#' @param memory_save Logical flag for memory-efficient normalisation.
#' @param verbose Should the function print progress messages?
#' @param aggregate Logical flag to return aggregated results or individual samples.
#' @param init_h2o Logical. If TRUE (default), ensures the H2O cluster is initialized.
#'   Set to FALSE when called from a workflow that manages initialization.
#' @param ... Additional arguments passed to underlying functions (e.g., `cl` for parallel clusters).
#'
#' @return A list containing the following elements:
#' \describe{
#'   \item{out}{The normalised data frame.}
#'   \item{model}{The trained model object.}
#'   \item{df_prep}{The prepared data frame used for training.}
#' }
#'
#' @export
nm_do_all <- function(df = NULL, value = 'value', backend = 'h2o', feature_names = NULL,
                      variables_resample = NULL, split_method = 'random', fraction = 0.75,
                      model_config = NULL, n_samples = 300, seed = 7654321, n_cores = NULL,
                      memory_save = FALSE, verbose = TRUE, aggregate = TRUE, init_h2o = TRUE, ...){

  # Get a namespaced logger and record the start time
  log <- nm_get_logger("workflow.do_all") #
  start_time <- Sys.time() #

  if (verbose) {
    log$info("Starting do_all | backend=%s | value=%s | n_samples=%d", backend, value, n_samples) #
  }

  # Only initialize H2O if requested. This avoids redundant checks when called from a parent workflow.
  if (backend == 'h2o' && init_h2o) {
    nm_init_h2o(n_cores = n_cores, verbose = verbose) #
  }

  # 1) & 2) Prepare data and Train model
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
  ) #

  df_prep <- build_results$df_prep #
  model <- build_results$model #

  # 3) Normalise
  # Pass the ellipsis '...' down to nm_normalise to handle potential 'cl' argument from parent
  out <- nm_normalise(
    df = df_prep,
    model = model,
    feature_names = feature_names,
    variables_resample = variables_resample,
    n_samples = n_samples,
    aggregate = aggregate,
    seed = seed,
    n_cores = n_cores,
    memory_save = memory_save,
    verbose = verbose,
    ... # Pass extra arguments like 'cl' down
  ) #

  if (verbose) {
    log$info("do_all finished.") #
  }

  # 4) Return results
  return(list(out = out, model = model, df_prep = df_prep)) #
}



#' Perform Normalisation with Uncertainty Estimation
#'
#' @description
#' Trains multiple models, aggregates their normalised predictions,
#' and builds uncertainty bands with optional performance-based weighting.
#'
#' @param df The raw input data frame.
#' @param value The target variable name as a string.
#' @param backend The modeling backend to use for training. Default is 'h2o'.
#' @param feature_names The names of the features used for training and normalisation.
#' @param variables_resample The names of variables to be resampled during normalisation.
#' @param split_method The method for splitting data into training and testing sets.
#' @param fraction The proportion of the data to be used for training.
#' @param model_config A list containing configuration parameters for model training.
#' @param n_samples Number of times to sample the data for normalisation.
#' @param n_models Number of models to train for uncertainty estimation.
#' @param confidence_level The confidence level for uncertainty estimation.
#' @param seed A random seed for reproducibility.
#' @param n_cores Number of CPU cores to use for parallel processing.
#' @param memory_save Logical for memory-efficient normalisation.
#' @param verbose Should the function print progress messages and logs?
#' @param weighted_method Method for weighting models ("r2" or "rmse").
#' @param cleanup_every Integer; how often to clear H2O memory (only relevant if backend = "h2o").
#'
#' @return A list containing the normalised data with uncertainty and model statistics.
#'
#' @export
nm_do_all_unc <- function(df = NULL, value = 'value', backend = 'h2o', feature_names = NULL,
                               variables_resample = NULL, split_method = 'random', fraction = 0.75,
                               model_config = NULL, n_samples = 300, n_models = 10,
                               confidence_level = 0.95, seed = 7654321, n_cores = NULL,
                               memory_save = FALSE, verbose = TRUE, weighted_method = "r2",
                               cleanup_every = 5) {

  log <- nm_get_logger("workflow.do_all_unc")
  h2o::h2o.no_progress()

  # 1. Validate weighting method
  if (!weighted_method %in% c("r2", "rmse")) {
    log$error("`weighted_method` must be 'r2' or 'rmse'.")
    stop("`weighted_method` must be 'r2' or 'rmse'.")
  }

  # 2. Generate reproducible seeds
  set.seed(seed)
  seeds <- sample(1:1000000, n_models, replace = FALSE)

  # 3. Initialize H2O with health check
  if (backend == 'h2o') {
    if (!requireNamespace("h2o", quietly = TRUE)) stop("Package 'h2o' is required.")
    if (!h2o::h2o.clusterIsUp()) {
      try(h2o::h2o.shutdown(prompt = FALSE), silent = TRUE)
      Sys.sleep(2)
      tryCatch({
        h2o::h2o.init(nthreads = -1, max_mem_size = "6G")
      }, error = function(e) {
        stop("Failed to initialize H2O: ", e$message)
      })
    }
  }

  # 4. Setup parallel cluster
  n_cores_for_norm <- n_cores %||% (parallel::detectCores() - 1)
  n_cores_for_norm <- max(1, n_cores_for_norm)
  cl <- parallel::makeCluster(n_cores_for_norm)
  on.exit(parallel::stopCluster(cl), add = TRUE)

  series_list <- list()
  stats_list <- list()
  observed_ref <- NULL

  # 5. Progress bar
  if (verbose) {
    log$info("Starting uncertainty run with %d models using %d cores.", n_models, n_cores_for_norm)
    pb <- progress::progress_bar$new(
      format = "  Training Models [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = n_models, clear = FALSE, width = 80
    )
  }

  # 6. Main loop over seeds
  for (i in seq_along(seeds)) {
    current_seed <- seeds[i]

    # Check H2O health before each model
    if (backend == 'h2o' && !h2o::h2o.clusterIsUp()) {
      log$warn("H2O cluster down before model %d. Restarting...", i)
      try(h2o::h2o.shutdown(prompt = FALSE), silent = TRUE)
      Sys.sleep(2)
      h2o::h2o.init(nthreads = -1, max_mem_size = "6G")
    }

    # Train model
    res_i <- nm_do_all(
      df = df, value = value, backend = backend, feature_names = feature_names,
      variables_resample = variables_resample, split_method = split_method, fraction = fraction,
      model_config = model_config, n_samples = n_samples, seed = current_seed, n_cores = n_cores,
      memory_save = memory_save, verbose = FALSE, aggregate = TRUE,
      init_h2o = FALSE, cl = cl
    )

    out_i <- res_i$out
    model_i <- res_i$model
    df_prep_i <- res_i$df_prep

    # Store observed reference once
    if (is.null(observed_ref) && "observed" %in% colnames(out_i)) {
      observed_ref <- out_i[, "observed", drop = FALSE]
    }

    # Collect normalized series
    series_list[[i]] <- out_i[, "normalised", drop = FALSE] %>%
      dplyr::rename_with(~ paste0("normalised_", current_seed), .cols = "normalised")

    # Collect model stats
    tryCatch({
      stats_i <- nm_modStats(df = df_prep_i, model = model_i)
      stats_i$seed <- current_seed
      stats_list[[i]] <- stats_i
    }, error = function(e) {
      log$warn("Failed to compute metrics for seed %d: %s", current_seed, e$message)
    })

    if (verbose) pb$tick()

    # Periodic H2O cleanup
    if (backend == 'h2o' && i %% cleanup_every == 0) {
      h2o::h2o.removeAll()
      gc(verbose = FALSE)
    }
  }

  # 7. Final cleanup
  if (backend == 'h2o') {
    h2o::h2o.removeAll()
    gc(verbose = FALSE)
  }

  if (is.null(observed_ref)) {
    log$error("do_all_unc produced no outputs - verify inputs and seeds.")
    stop("do_all_unc produced no outputs - verify inputs and seeds.")
  }

  # 8. Aggregate predictions
  out <- cbind(observed_ref, dplyr::bind_cols(series_list))
  mod_stats <- dplyr::bind_rows(stats_list)

  pred_cols <- grep("^normalised_", colnames(out), value = TRUE)
  P <- out[, pred_cols, drop = FALSE]

  out$mean   <- rowMeans(P, na.rm = TRUE)
  out$std    <- apply(P, 1, sd, na.rm = TRUE)
  out$median <- apply(P, 1, median, na.rm = TRUE)

  alpha <- (1.0 - confidence_level) / 2.0
  out$lower_bound <- apply(P, 1, quantile, probs = alpha, na.rm = TRUE)
  out$upper_bound <- apply(P, 1, quantile, probs = 1.0 - alpha, na.rm = TRUE)

  # 9. Compute performance-based weights
  testing_stats <- mod_stats %>% dplyr::filter(set == 'testing')
  scores <- rep(0.0, length(seeds))

  if (nrow(testing_stats) > 0) {
    if (weighted_method == "r2") {
      scores <- sapply(seeds, function(s) {
        r2 <- testing_stats$R2[testing_stats$seed == s]
        if (length(r2) > 0 && !is.na(r2)) max(as.numeric(r2), 0.0) else 0.0
      })
    } else {
      eps <- 1e-9
      scores <- sapply(seeds, function(s) {
        rmse <- testing_stats$RMSE[testing_stats$seed == s]
        if (length(rmse) > 0 && !is.na(rmse)) 1.0 / (as.numeric(rmse) + eps) else 0.0
      })
    }
  }

  score_sum <- sum(scores, na.rm = TRUE)
  w <- if (is.na(score_sum) || score_sum <= 0) {
    rep(1.0 / length(seeds), length(seeds))
  } else {
    scores / score_sum
  }

  # Weighted blended prediction
  out$weighted <- if (ncol(P) > 0 && length(w) == ncol(P)) {
    as.numeric(as.matrix(P) %*% w)
  } else {
    NA
  }

  # Attach weights to model stats
  if (nrow(mod_stats) > 0) {
    w_df <- data.frame(seed = seeds, weight = w)
    mod_stats <- dplyr::left_join(mod_stats, w_df, by = "seed")
  }

  return(list(out = out, mod_stats = mod_stats))
}
