#' Augmented Synthetic Control Method (SCM)
#'
#' \code{nm_scm} implements the Augmented Synthetic Control Method for a single treated unit.
#'
#' @param df Long data frame with columns for date, unit, and outcome.
#' @param date_col Name of the date column (coercible to Date).
#' @param unit_col Name of the unit identifier column.
#' @param outcome_col Name of the outcome column.
#' @param treated_unit The treated unit identifier.
#' @param cutoff_date The intervention cutoff date (character or Date).
#' @param donors Optional character vector of donor unit identifiers. If NULL, uses all non-treated units.
#' @param pre_covariates Optional character vector of pre-period covariate columns to augment features.
#' @param alphas Optional numeric vector of ridge lambdas. If NULL, uses seq(0.1, 10, by = 0.1).
#' @param allow_negative_weights Logical; if TRUE, donor weights may be negative. Default FALSE.
#' @param verbose Logical; if TRUE, prints log messages and shows a progress bar. Default TRUE.
#'
#' @return A list containing:
#'   \item{synthetic}{A data frame with observed, synthetic, and effect time series.}
#'   \item{weights}{A named vector of donor weights.}
#'   \item{alpha_map}{A list mapping each timestamp to the chosen Ridge lambda.}
#'
nm_scm <- function(df, date_col = "date", unit_col = "code", outcome_col = "poll",
                   treated_unit = NULL, cutoff_date = NULL, donors = NULL,
                   pre_covariates = NULL, alphas = NULL, allow_negative_weights = FALSE,
                   verbose = TRUE) {

  # --- 0. Setup and basic validation ---
  if (is.null(treated_unit) || is.null(cutoff_date)) {
    stop("Both `treated_unit` and `cutoff_date` must be provided.")
  }
  df[[date_col]] <- as.Date(df[[date_col]])
  cutoff_ts <- as.Date(cutoff_date)
  if (!treated_unit %in% df[[unit_col]]) {
    stop("Treated unit not found in data.")
  }

  # --- 1. Pivot to wide panel (date x units) ---
  panel <- df %>%
    tidyr::pivot_wider(id_cols = dplyr::all_of(date_col),
                       names_from = dplyr::all_of(unit_col),
                       values_from = dplyr::all_of(outcome_col)) %>%
    dplyr::arrange(.data[[date_col]])
  if (!treated_unit %in% colnames(panel)) stop("Treated unit not found in panel.")
  if (is.null(donors)) donors <- setdiff(colnames(panel), c(date_col, treated_unit))
  donors <- intersect(donors, colnames(panel))
  if (length(donors) == 0) stop("No valid donors after filtering.")

  # --- 2. Pre/post split ---
  pre_idx <- panel[[date_col]] < cutoff_ts
  dates_pre <- panel[[date_col]][pre_idx]
  if (length(dates_pre) < 3 && verbose) {
    warning("Very short pre-period; results may be unstable.")
  }

  # --- 3. Build ridge feature matrices from pre-period (rows = units, cols = features) ---
  Y_pre <- panel[pre_idx, c(donors, treated_unit)] %>% tidyr::drop_na()
  if (nrow(Y_pre) < 3) stop("Not enough complete pre-treatment rows.")
  # Observations are units; features are pre-period times (and optionally covariates)
  X_donors <- t(as.matrix(Y_pre[, donors]))           # donors x T_pre
  X_treated <- t(as.matrix(Y_pre[[treated_unit]]))    # 1 x T_pre

  # --- 4. Optional covariate augmentation (unit-level means over pre-period) ---
  if (!is.null(pre_covariates)) {
    cov_df <- df[df[[date_col]] < cutoff_ts, c(unit_col, pre_covariates), drop = FALSE] %>%
      dplyr::group_by(.data[[unit_col]]) %>%
      dplyr::summarise(dplyr::across(dplyr::all_of(pre_covariates), mean, na.rm = TRUE), .groups = "drop") %>%
      tidyr::drop_na()
    valid_units <- cov_df[[unit_col]]
    donors <- intersect(donors, valid_units)
    if (!(treated_unit %in% valid_units) || length(donors) == 0) {
      stop("Covariate filtering removed all valid units.")
    }
    # Rebuild pre matrices to match filtered units
    Y_pre <- panel[pre_idx, c(donors, treated_unit)] %>% tidyr::drop_na()
    X_donors <- t(as.matrix(Y_pre[, donors]))
    X_treated <- t(as.matrix(Y_pre[[treated_unit]]))

    cov_mat <- as.matrix(cov_df[, pre_covariates, drop = FALSE])
    rownames(cov_mat) <- cov_df[[unit_col]]
    # Augment features by column-binding covariates (kept in unit order)
    X_donors <- cbind(X_donors, cov_mat[donors, , drop = FALSE])
    X_treated <- cbind(X_treated, cov_mat[treated_unit, , drop = FALSE])
  }

  # --- 5. RidgeCV predictions over time points (solve donors->treated mapping per t) ---
  if (is.null(alphas)) alphas <- seq(0.1, 10, by = 0.1)
  lambda_grid <- sort(as.numeric(alphas), decreasing = TRUE)  # glmnet prefers decreasing lambda
  m_treated <- rep(NA_real_, nrow(panel))
  m_donors <- matrix(NA_real_, nrow = nrow(panel), ncol = length(donors)); colnames(m_donors) <- donors
  alpha_map <- list()

  pb <- NULL
  if (verbose) pb <- utils::txtProgressBar(min = 0, max = nrow(panel), style = 3)
  for (t in seq_len(nrow(panel))) {
    if (verbose) utils::setTxtProgressBar(pb, t)
    y_t <- as.numeric(panel[t, donors, drop = TRUE])  # donor outcomes at time t (vector length = J)
    mask <- is.finite(y_t)
    if (sum(mask) >= 3) {
      # Fit ridge using masked donors as observations, pre features as predictors
      cv_fit <- glmnet::cv.glmnet(
        x = X_donors[mask, , drop = FALSE],
        y = y_t[mask],
        alpha = 0,                      # ridge
        lambda = lambda_grid,
        family = "gaussian",
        standardize = TRUE
      )
      chosen <- cv_fit$lambda.min
      alpha_map[[as.character(panel[[date_col]][t])]] <- chosen
      # Predict treated baseline (1 x features)
      m_treated[t] <- as.numeric(stats::predict(cv_fit, newx = X_treated, s = "lambda.min"))
      # Predict donors baselines (donors x features -> length(mask) predictions)
      preds_don <- as.numeric(stats::predict(cv_fit, newx = X_donors, s = "lambda.min"))
      # Place predictions only for donors used in the fit; keep NA for others
      m_donors[t, mask] <- preds_don[mask]
    }
  }
  if (verbose && !is.null(pb)) close(pb)

  # --- 6. Residual construction (observed - baseline) ---
  R_don <- as.matrix(panel[, donors, drop = FALSE]) - m_donors
  r_treat <- panel[[treated_unit]] - m_treated
  pre_residuals <- cbind(treat = r_treat[pre_idx], R_don[pre_idx, , drop = FALSE]) %>%
    as.data.frame() %>% tidyr::drop_na()
  if (nrow(pre_residuals) < 3) stop("Insufficient aligned residuals.")
  r_pre <- as.numeric(pre_residuals[, "treat"])
  R_pre <- as.matrix(pre_residuals[, donors, drop = FALSE])

  # --- 7. Quadratic program: minimize || r_pre - R_pre w ||^2 subject to constraints ---
  J <- length(donors)
  Dmat <- 2 * t(R_pre) %*% R_pre + 1e-6 * diag(J)          # ensure positive definite
  dvec <- 2 * t(R_pre) %*% r_pre
  A_eq <- matrix(1, nrow = 1, ncol = J); b_eq <- 1

  # Construct constraint matrix Amat per quadprog's column-convention:
  # Columns = constraints; First 'meq' are equalities
  if (allow_negative_weights) {
    Amat <- t(A_eq)                 # only the simplex-sum constraint
    bvec <- b_eq
    meq <- 1
  } else {
    A_ineq <- diag(J)               # non-negativity: w_j >= 0
    b_ineq <- rep(0, J)
    Amat <- t(rbind(A_eq, A_ineq))  # combine equality and inequality
    bvec <- c(b_eq, b_ineq)
    meq <- 1
  }

  w <- tryCatch({
    sol <- quadprog::solve.QP(Dmat, dvec, Amat, bvec, meq = meq)
    sol$solution
  }, error = function(e) {
    if (verbose) warning(sprintf("Quadratic program failed: %s. Using fallback weights.", e$message))
    rep(1 / J, J)                   # fallback: uniform weights
  })

  # Normalize and enforce non-negativity if requested
  if (!allow_negative_weights) w <- pmax(w, 0)
  sw <- sum(w)
  if (sw <= 0 || !is.finite(sw)) {
    if (verbose) warning("Weight sum is non-positive or non-finite; using uniform weights.")
    w <- rep(1 / J, J)
  } else {
    w <- w / sw
  }
  weights <- stats::setNames(w, donors)

  # --- 8. Synthetic path and effect ---
  synth <- as.numeric(m_treated) + as.numeric(R_don %*% weights)
  out <- data.frame(
    date = panel[[date_col]],
    observed = panel[[treated_unit]],
    synthetic = synth
  )
  out$effect <- out$observed - out$synthetic

  if (verbose) {
    nz <- sum(weights > 0)
    msg <- sprintf("SCM completed: %d donors used; non-zero weights: %d; pre-period length: %d.",
                   length(donors), nz, nrow(Y_pre))
    message(msg)
  }

  return(list(synthetic = out, weights = weights, alpha_map = alpha_map))
}




#' Machine Learning Synthetic Control (ML-SCM)
#'
#' \code{nm_mlscm} estimates counterfactual outcomes using a machine learning backend.
#' It wraps around `nm_build_model()` and `nm_predict()` to train and apply a model
#' using pre-treatment data and donor units.
#'
#' @param df Long-format panel data.
#' @param date_col Name of the date column.
#' @param unit_col Name of the unit identifier column.
#' @param outcome_col Name of the outcome variable.
#' @param treated_unit Name of the treated unit.
#' @param cutoff_date Date string marking the intervention point.
#' @param donors Character vector of donor units.
#' @param backend ML backend to use (e.g. "h2o", "flaml").
#' @param model_config List of backend-specific model configuration parameters.
#' @param split_method Data splitting method ("random", "time", etc.).
#' @param fraction Fraction of pre-treatment data used for training.
#' @param seed Random seed for reproducibility.
#' @param verbose Whether to print progress messages.
#'
#' @return A data frame with columns: date, observed, synthetic, effect.
nm_mlscm <- function(df,
                     date_col,
                     unit_col,
                     outcome_col,
                     treated_unit,
                     cutoff_date,
                     donors,
                     backend = "h2o",
                     model_config = NULL,
                     split_method = "random",
                     fraction = 1.0,
                     seed = 7654321,
                     verbose = TRUE) {

  log <- nm_get_logger("causal.scm.ml")

  # --- 1. Validate and normalize inputs ---
  if (missing(df) || missing(date_col) || missing(outcome_col) || missing(unit_col) ||
      missing(treated_unit) || missing(donors) || missing(cutoff_date)) {
    stop("Missing required arguments.")
  }

  df[[date_col]] <- as.Date(df[[date_col]])
  cutoff_ts <- as.Date(cutoff_date)

  # --- 2. Filter and reshape panel ---
  union_units <- unique(c(donors, treated_unit))
  panel <- df %>%
    dplyr::filter(.data[[unit_col]] %in% union_units) %>%
    tidyr::pivot_wider(id_cols = all_of(date_col), names_from = all_of(unit_col), values_from = all_of(outcome_col)) %>%
    dplyr::arrange(.data[[date_col]])

  if (!treated_unit %in% colnames(panel)) {
    log$error("Treated unit '%s' not found in panel.", treated_unit)
    stop("Treated unit not found.")
  }

  valid_donors <- intersect(donors, setdiff(colnames(panel), c(date_col, treated_unit)))
  if (length(valid_donors) == 0) stop("No valid donors available.")

  # --- 3. Safe column mapping ---
  safe_name <- function(name) {
    s <- gsub("[^A-Za-z0-9_]", "_", as.character(name))
    s <- gsub("_+", "_", s)
    s <- gsub("^_|_$", "", s)
    if (nchar(s) == 0) s <- "_"
    if (grepl("^[0-9]", s)) s <- paste0("_", s)
    return(s)
  }

  build_safe_map <- function(cols) {
    out <- character(length(cols))
    used <- character()
    for (i in seq_along(cols)) {
      base <- safe_name(cols[i])
      cand <- base
      k <- 1
      while (cand %in% used) {
        k <- k + 1
        cand <- paste0(base, "_", k)
      }
      used <- c(used, cand)
      out[i] <- cand
    }
    names(out) <- cols
    return(out)
  }

  col_map <- build_safe_map(colnames(panel))
  panel_safe <- panel
  colnames(panel_safe) <- col_map[colnames(panel)]
  treated_safe <- col_map[treated_unit]
  donors_safe <- col_map[valid_donors]
  date_safe <- col_map[date_col]

  # --- 4. Extract pre-treatment data ---
  pre_panel_safe <- panel_safe %>% dplyr::filter(.data[[date_safe]] < cutoff_ts)
  if (nrow(pre_panel_safe) < 3) stop("Too few pre-treatment time points.")

  # --- 5. Ensure model_config is a list ---
  if (is.null(model_config)) {
    model_config <- list()
  } else if (!is.list(model_config)) {
    stop("`model_config` must be a list.")
  }

  if (verbose) {
    log$info("Training ML-SCM model | backend=%s | donors=%d | cutoff=%s", backend, length(donors_safe), cutoff_date)
    log$info("Model config keys: %s", paste(names(model_config), collapse = ", "))
  }

  # --- 6. Train model ---
  build_results <- nm_build_model(
    df = pre_panel_safe,
    value = treated_safe,
    backend = backend,
    feature_names = donors_safe,
    split_method = split_method,
    fraction = fraction,
    model_config = model_config,
    seed = seed,
    verbose = verbose,
    drop_time_features = TRUE
  )
  model <- build_results$model

  # --- 7. Predict full period ---
  synth_all <- nm_predict(model, panel_safe[, donors_safe, drop = FALSE])

  # --- 8. Assemble output ---
  out <- data.frame(
    date = panel[[date_col]],
    observed = panel[[treated_unit]],
    synthetic = synth_all
  )
  out$effect <- out$observed - out$synthetic

  if (verbose) log$info("ML-SCM completed: %d timestamps (%s to %s)", nrow(out), min(out$date), max(out$date))
  return(out)
}



#' Unified Synthetic Control Dispatcher
#'
#' @description
#' `nm_run_scm` is an internal helper that validates inputs and calls the
#' appropriate SCM backend ('scm' or 'mlscm'). It is not exported.
#'
#' @param df Long-format panel data.
#' @param date_col Name of date column.
#' @param unit_col Name of unit identifier column.
#' @param outcome_col Name of outcome variable.
#' @param treated_unit Name of treated unit.
#' @param cutoff_date Intervention date.
#' @param donors Optional vector of donor units.
#' @param scm_backend Either "scm" or "mlscm".
#' @param verbose Whether to print INFO log messages. Default TRUE.
#' @param ... Additional arguments passed to the backend function.
#'
#' @return A data frame with columns: date, observed, synthetic, effect.
#' @keywords internal
nm_run_scm <- function(df,
                       date_col,
                       unit_col,
                       outcome_col,
                       treated_unit,
                       cutoff_date,
                       donors = NULL,
                       scm_backend = "scm",
                       verbose = TRUE,
                       ...) {

  log <- nm_get_logger("causal.core")

  # --- 1. Validate required columns ---
  required_cols <- c(date_col, unit_col, outcome_col)
  missing_cols <- setdiff(required_cols, colnames(df))
  if (length(missing_cols) > 0) stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
  if (is.null(treated_unit) || treated_unit == "") stop("`treated_unit` must be a non-empty string.")

  # --- 2. Normalize backend ---
  scm_backend <- tolower(scm_backend)

  # --- 3. Resolve donor pool ---
  all_units <- unique(df[[unit_col]])
  if (!(treated_unit %in% all_units)) stop("Treated unit not found in data.")
  base_pool <- if (!is.null(donors)) donors else all_units
  donor_pool <- setdiff(unique(base_pool), treated_unit)
  if (length(donor_pool) == 0) stop("No donors available after excluding treated unit.")

  # --- 4. Parse cutoff date ---
  cutoff_ts <- as.Date(cutoff_date)
  cutoff_str <- format(cutoff_ts, "%Y-%m-%d")

  # --- 5. Extract additional arguments ---
  args <- list(...)

  # --- 6. Dispatch to backend ---
  if (scm_backend == "scm") {
    if (verbose) {
      log$info("Running SCM | treated=%s | donors=%d | cutoff=%s",
               treated_unit, length(donor_pool), cutoff_str)
    }
    out <- do.call(nm_scm, c(list(
      df = df, date_col = date_col, unit_col = unit_col, outcome_col = outcome_col,
      treated_unit = treated_unit, cutoff_date = cutoff_str, donors = donor_pool,
      verbose = verbose), args)
    )
    return(out$synthetic)

  } else if (scm_backend == "mlscm") {
    ml_backend <- tolower(args$backend %||% "h2o")
    if (verbose) {
      log$info("Running ML-SCM | backend=%s | treated=%s | donors=%d | cutoff=%s",
               ml_backend, treated_unit, length(donor_pool), cutoff_str)
    }
    return(do.call(nm_mlscm, c(list(
      df = df, date_col = date_col, unit_col = unit_col, outcome_col = outcome_col,
      treated_unit = treated_unit, cutoff_date = cutoff_str, donors = donor_pool,
      backend = ml_backend, verbose = verbose), args))
    )
  }

  stop("Unsupported scm_backend: ", scm_backend)
}




#' Run Synthetic Control for Many Treated Units
#'
#' @description
#' Applies a synthetic control method to every unit in a dataset,
#' treating each one as the target unit in turn. Uses parallel execution
#' for the classic SCM backend, and serial execution for ML-SCM to avoid
#' conflicts with H2O clusters.
#'
#' @param df A long-format panel data frame.
#' @param date_col The name of the date column.
#' @param outcome_col The name of the outcome variable column.
#' @param unit_col The name of the column containing unit identifiers.
#' @param donors A character vector specifying the donor pool. If NULL,
#'   all other units are used as donors.
#' @param cutoff_date The treatment cutoff date in "YYYY-MM-DD" format.
#' @param scm_backend The synthetic control method to use: "scm" or "mlscm".
#' @param n_cores The number of parallel workers. Defaults to all available
#'   cores minus one.
#' @param verbose Logical; whether to print INFO/WARN log messages.
#'   Default is TRUE. Progress bar is always shown regardless of this flag.
#' @param ... Additional arguments passed to the backend functions.
#'
#' @return A single, long-format data frame containing the combined results
#'   for all treated units.
#' @export
nm_scm_all <- function(df,
                       date_col,
                       outcome_col,
                       unit_col,
                       donors = NULL,
                       cutoff_date,
                       scm_backend = "scm",
                       n_cores = NULL,
                       verbose = TRUE,
                       ...) {

  log <- nm_get_logger("causal.scm.all") #

  # --- 1. Setup ---
  units <- sort(unique(df[[unit_col]])) #
  n_cores <- n_cores %||% (parallel::detectCores() - 1) #
  n_cores <- max(1, n_cores) #
  extra_args <- list(...) #
  ml_backend_for_log <- if (scm_backend == "mlscm") extra_args$backend %||% "h2o" else NA #

  if (verbose) {
    log$info(
      "scm_all: scm_backend=%s | ml_backend=%s | cutoff=%s | units=%d | n_cores=%d",
      scm_backend, ml_backend_for_log, cutoff_date, length(units), n_cores
    ) #
  }

  # Initialize H2O once at the beginning if using the mlscm backend
  if (tolower(scm_backend) == "mlscm" && (extra_args$backend %||% "h2o") == "h2o") {
      nm_init_h2o(verbose = verbose) #
  }

  # --- 2. Progress bar (always shown) ---
  if (!requireNamespace("progress", quietly = TRUE)) {
    stop("Package 'progress' is required for progress bar. Please install it.") #
  }
  pb <- progress::progress_bar$new(
    format = "  SCM-all [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
    total = length(units), clear = FALSE, width = 80
  ) #

  results <- list() #

  # --- 3. Execution mode ---
  if (tolower(scm_backend) == "scm") {
    cl <- parallel::makeCluster(n_cores) #
    doSNOW::registerDoSNOW(cl) #
    on.exit(parallel::stopCluster(cl), add = TRUE)
    opts <- list(progress = function(n) pb$tick()) #

    pieces <- foreach::foreach(
      code = units,
      .packages = c("dplyr", "tidyr", "glmnet", "quadprog", "stringr"),
      .export = c(".LOGGER_NAME", "nm_run_scm", "nm_scm", "nm_mlscm", "nm_build_model",
                  "nm_train_model", "nm_train_h2o", "nm_predict", "nm_get_logger",
                  "nm_require", "%||%"),
      .options.snow = opts
    ) %dopar% {
      tryCatch({
        run_args <- c(
          list(
            df = df, date_col = date_col, outcome_col = outcome_col,
            unit_col = unit_col, treated_unit = code,
            cutoff_date = cutoff_date, donors = donors,
            scm_backend = scm_backend, verbose = FALSE
          ),
          extra_args
        ) #

        syn <- do.call(nm_run_scm, run_args) #

        syn[[unit_col]] <- code #
        syn <- syn %>% dplyr::rename(!!date_col := date) #
        return(syn) #

      }, error = function(e) {
        warning(sprintf("SCM run failed for unit %s: %s", code, e$message))
        return(NULL) #
      })
    }
    results <- pieces #

  } else if (tolower(scm_backend) == "mlscm") {
    # Serial execution for ML-SCM
    for (code in units) {
      pb$tick() #
      tryCatch({
        syn <- nm_run_scm(
          df = df, date_col = date_col, outcome_col = outcome_col, unit_col = unit_col,
          treated_unit = code, cutoff_date = cutoff_date, donors = donors,
          scm_backend = scm_backend, verbose = FALSE, ...
        ) #
        syn[[unit_col]] <- code #
        syn <- syn %>% dplyr::rename(!!date_col := date) #
        results[[length(results) + 1]] <- syn #
      }, error = function(e) {
        if (verbose) warning(sprintf("ML-SCM run failed for unit %s: %s", code, e$message)) #
      })

      if ((extra_args$backend %||% "h2o") == "h2o") {
        h2o.removeAll() #
        gc() #
      }
    }
  } else {
    stop("Unsupported scm_backend: ", scm_backend) #
  }

  # --- 4. Aggregate Results ---
  results <- Filter(Negate(is.null), results) #
  if (length(results) == 0) {
    log$error("All synthetic control runs failed.") #
    stop("All synthetic control runs failed.") #
  }

  return(dplyr::bind_rows(results)) #
}
