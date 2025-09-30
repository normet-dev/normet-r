#' Create a value grid for Partial Dependence Plots (Internal Helper)
#'
#' @keywords internal
.create_pdp_grid <- function(series, quantile_range = c(0.01, 0.99), grid_points = 50) {
  # This helper function is correct and requires no changes.
  s <- as.numeric(series)
  s <- s[is.finite(s)]
  if (length(s) == 0) return(NULL)
  lo_hi <- stats::quantile(s, probs = quantile_range, na.rm = TRUE)
  if (any(!is.finite(lo_hi)) || lo_hi[1] == lo_hi[2]) return(NULL)
  return(seq(lo_hi[1], lo_hi[2], length.out = max(2, grid_points)))
}


#' Compute Partial Dependence Plots (PDP)
#'
#' @param df Data frame containing the input data.
#' @param model The trained model object, which should have a 'backend' attribute.
#' @param var_list A character vector of variables to compute PDP for. If NULL, PDPs
#'   will be computed for all features used by the model.
#' @param verbose Logical. If TRUE, shows progress bars and logs.
#' @param ... Additional arguments passed to the implementation function.
#'
#' @return A data frame with PDP results.
#' @export
nm_pdp <- function(df, model, var_list = NULL, verbose = TRUE, ...) {
  log <- nm_get_logger("analysis.pdp") #
  model_backend <- attr(model, "backend") #

  if (!is.null(model_backend) && startsWith(model_backend, "h2o")) {
    if (verbose) log$info("Dispatching to H2O backend for PDP calculation.") #
    # Pass verbose and other arguments down
    return(nm_pdp_h2o(df = df, model = model, var_list = var_list, verbose = verbose, ...)) #
  } else {
    if (verbose) log$info("Dispatching to generic backend for PDP calculation.") #
    # Pass verbose and other arguments down
    return(nm_pdp_generic(df = df, model = model, var_list = var_list, verbose = verbose, ...)) #
  }
}


#' Compute PDP for H2O Models
#' @keywords internal
nm_pdp_h2o <- function(df, model, var_list = NULL, training_only = TRUE, verbose = TRUE) {

  log <- nm_get_logger("analysis.pdp.h2o") #
  nm_require("h2o", hint = "install.packages('h2o')") #

  feature_names <- nm_extract_features(model) #
  vars_for_pdp <- if (is.null(var_list)) feature_names else intersect(var_list, feature_names) #

  X_df <- if ("set" %in% colnames(df) && training_only) df[df$set == "training", ] else df #
  cols_to_use <- c(feature_names, "value") #
  df_h2o <- h2o::as.h2o(X_df[, intersect(cols_to_use, colnames(X_df))]) #

  if (verbose) {
    if (!requireNamespace("progress", quietly = TRUE)) {
      stop("Package 'progress' is required for progress bars. Please install it.")
    }
    pb <- progress::progress_bar$new(
      format = "  Calculating PDP [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = length(vars_for_pdp), clear = FALSE, width = 80
    )
  }

  pieces <- list()
  for (var in vars_for_pdp) {
    tryCatch({
      #if (verbose) log$info("Calculating PDP for '%s'...", var)

      fr <- h2o::h2o.partialPlot(
        object = model,
        newdata = df_h2o,
        cols = var,
        plot = FALSE
      ) #

      out_df <- data.frame(
        variable = var,
        value = as.character(fr[[var]]),
        pdp_mean = fr$mean_response,
        pdp_std = fr$stddev_response
      ) #

      pieces[[var]] <- out_df

    }, error = function(e) {
      log$warn("PDP failed for '%s' (H2O): %s", var, e$message) #
    })

    if (verbose) pb$tick()
  }

  return(dplyr::bind_rows(pieces)) #
}



#' Compute PDP for Generic Models
#' @keywords internal
nm_pdp_generic <- function(df, model, var_list = NULL, training_only = TRUE, n_cores = NULL,
                           grid_points = 50, quantile_range = c(0.01, 0.99), verbose = TRUE) {

  log <- nm_get_logger("analysis.pdp.generic") #
  feature_names <- nm_extract_features(model) #
  vars_for_pdp <- if (is.null(var_list)) feature_names else intersect(var_list, feature_names) #

  X_df <- if ("set" %in% colnames(df) && training_only) df[df$set == "training", feature_names] else df[, feature_names] #

  n_cores <- n_cores %||% (parallel::detectCores() - 1) #
  cl <- parallel::makeCluster(n_cores) #
  doSNOW::registerDoSNOW(cl)
  on.exit(parallel::stopCluster(cl), add = TRUE)

  if (verbose) {
    log$info("Calculating PDP in parallel for %d variables on %d cores...", length(vars_for_pdp), n_cores) #
    if (!requireNamespace("progress", quietly = TRUE)) {
      stop("Package 'progress' is required for progress bars. Please install it.")
    }
    pb <- progress::progress_bar$new(
      format = "  Calculating PDP [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = length(vars_for_pdp), clear = FALSE, width = 80
    )
    opts <- list(progress = function(n) pb$tick())
  } else {
    opts <- list()
  }

  pieces <- foreach::foreach(
    var = vars_for_pdp,
    # Updated export list for robustness
    .export = c(".LOGGER_NAME", "nm_get_logger", "nm_require", "%||%", "nm_predict", "nm_predict_h2o",
                "nm_auto_target_mb", "nm_extract_features", "nm_extract_features_h2o",
                ".create_pdp_grid"),
    .packages = c("data.table"),
    .options.snow = opts
  ) %dopar% {
    grid <- .create_pdp_grid(X_df[[var]], quantile_range, grid_points) #
    if (is.null(grid)) return(NULL)

    X_work <- X_df

    pdp_results <- sapply(grid, function(g) {
      X_work[[var]] <- g #
      yhat <- nm_predict(model, X_work, verbose = FALSE) #
      c(mean = mean(yhat, na.rm = TRUE), sd = sd(yhat, na.rm = TRUE)) #
    })

    data.frame(variable = var, value = grid, pdp_mean = pdp_results["mean", ], pdp_std = pdp_results["sd", ]) #
  }

  return(dplyr::bind_rows(Filter(Negate(is.null), pieces))) #
}
