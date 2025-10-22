#' Placebo-in-Space Test for Synthetic Control
#'
#' @description
#' Runs placebo-in-space tests by iteratively treating each donor unit as if it
#' were the treated unit. This helps assess the significance of the observed
#' treatment effect by comparing it against a distribution of placebo effects.
#'
#' @param df A long-format panel data frame.
#' @param date_col Name of the date column.
#' @param unit_col Name of the unit identifier column.
#' @param outcome_col Name of the outcome variable column.
#' @param treated_unit The treated unit identifier.
#' @param cutoff_date The intervention cutoff date.
#' @param donors Optional vector of donor units. If NULL, all units except the treated are used.
#' @param scm_backend Which SCM backend to use: "scm" or "mlscm".
#' @param post_agg Aggregation method for post-treatment effect: "mean" or "sum".
#' @param verbose Logical; whether to print log messages.
#' @param cleanup_every Integer; how often to clear H2O memory (only relevant if scm_backend = "mlscm").
#' @param ... Additional arguments passed to `nm_run_scm`.
#'
#' @return A list with:
#' \describe{
#'   \item{treated}{Data frame with the treated unit's effect path.}
#'   \item{placebos}{List of placebo effect paths for each donor unit.}
#'   \item{p_value}{Randomization inference p-value.}
#'   \item{ref_band}{Data frame with reference bands (quantiles, mean, sd).}
#' }
#'
nm_placebo_in_space <- function(df, date_col, unit_col, outcome_col,
                                treated_unit, cutoff_date, donors = NULL,
                                scm_backend = "scm", post_agg = "mean",
                                verbose = TRUE, cleanup_every = 10, ...) {
  log <- nm_get_logger("causal.placebo.space")

  # Validate aggregation method
  post_agg <- tolower(post_agg)
  if (!post_agg %in% c("mean", "sum")) {
    log$warn("Invalid post_agg='%s'; falling back to 'mean'.", post_agg)
    post_agg <- "mean"
  }

  # Parse cutoff date
  cutoff_ts <- as.Date(cutoff_date)

  # Extract additional arguments
  extra_args <- list(...)
  ml_backend <- if (tolower(scm_backend) == "mlscm") extra_args$backend else NULL

  # Safe H2O initialization
  safe_h2o_init <- function(ip = "localhost", port = 54321, max_mem = "4G", nthreads = -1, retries = 3) {
    for (i in seq_len(retries)) {
      tryCatch({
        h2o::h2o.init(ip = ip, port = port, max_mem_size = max_mem, nthreads = nthreads)
        if (h2o::h2o.clusterIsUp()) return(TRUE)
      }, error = function(e) {
        message(sprintf("H2O init attempt %d failed: %s", i, e$message))
        Sys.sleep(2)
      })
    }
    stop("H2O cluster failed to initialize after retries.")
  }

  # Retry wrapper for nm_run_scm
  retry_nm_run_scm <- function(..., max_attempts = 3) {
    for (i in seq_len(max_attempts)) {
      tryCatch({
        return(nm_run_scm(...))
      }, error = function(e) {
        message(sprintf("nm_run_scm attempt %d failed: %s", i, e$message))
        Sys.sleep(2)
      })
    }
    stop("nm_run_scm failed after retries.")
  }

  # Auto-initialize H2O if needed
  if (tolower(scm_backend) == "mlscm" && (ml_backend %||% "h2o") == "h2o") {
    if (!requireNamespace("h2o", quietly = TRUE)) stop("Package 'h2o' is required.")
    if (!h2o::h2o.clusterIsUp()) {
      try(h2o::h2o.shutdown(prompt = FALSE), silent = TRUE)
      Sys.sleep(2)
      safe_h2o_init(ip = extra_args$h2o_ip %||% "localhost",
                    port = extra_args$h2o_port %||% 54321)
    }
  }

  # Validate treated unit
  all_units <- sort(unique(df[[unit_col]]))
  if (!(treated_unit %in% all_units)) {
    stop("treated_unit must be a valid unit identifier in df[[unit_col]].")
  }

  # Run SCM for the treated unit
  df_true <- retry_nm_run_scm(
    df = df,
    date_col = date_col,
    unit_col = unit_col,
    outcome_col = outcome_col,
    treated_unit = treated_unit,
    cutoff_date = cutoff_date,
    donors = donors,
    scm_backend = scm_backend,
    ...,
    verbose = if (scm_backend == "mlscm") FALSE else verbose
  )

  # Build donor pool
  donor_pool <- if (is.null(donors)) {
    setdiff(all_units, treated_unit)
  } else {
    intersect(sort(unique(donors)), setdiff(all_units, treated_unit))
  }

  if (length(donor_pool) == 0) {
    log$warn("No donor units available for placebo-in-space.")
    empty_band <- data.frame(
      date = df_true$date, p10 = NA, p90 = NA, p2_5 = NA, p97_5 = NA,
      mean = NA, std = NA, band_low_1sd = NA, band_high_1sd = NA
    )
    return(list(treated = df_true, placebos = list(),
                p_value = NA_real_, ref_band = empty_band))
  }

  # Initialize progress bar
  if (!requireNamespace("progress", quietly = TRUE)) {
    stop("Package 'progress' is required for progress bar.")
  }
  pb <- progress::progress_bar$new(
    format = "  Placebos [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
    total = length(donor_pool), clear = FALSE, width = 60
  )

  placebo_effects <- list()
  counter <- 0

  # Main loop over donor units
  for (u in donor_pool) {
    tryCatch({
      if (tolower(scm_backend) == "mlscm" && (ml_backend %||% "h2o") == "h2o") {
        if (!h2o::h2o.clusterIsUp()) stop("H2O cluster is not reachable.")
      }

      syn_u <- retry_nm_run_scm(
        df = df,
        date_col = date_col,
        unit_col = unit_col,
        outcome_col = outcome_col,
        treated_unit = u,
        cutoff_date = cutoff_date,
        donors = setdiff(all_units, u),
        scm_backend = scm_backend,
        ...,
        verbose = if (scm_backend == "mlscm") FALSE else verbose
      )

      placebo_effects[[u]] <- syn_u[, c("date", "effect")] %>%
        dplyr::rename(!!u := effect)
    }, error = function(e) {
      if (verbose) log$warn("Placebo failed for unit %s: %s", u, e$message)
    })

    counter <- counter + 1

    # Periodic H2O memory cleanup
    if (tolower(scm_backend) == "mlscm" && (ml_backend %||% "h2o") == "h2o" &&
        counter %% cleanup_every == 0) {
      if (h2o::h2o.clusterIsUp()) {
        log$info("Cleaning H2O memory at iteration %d", counter)
        h2o::h2o.removeAll()
        gc(verbose = FALSE)
      } else {
        log$warn("H2O cluster not reachable during cleanup.")
      }
    }

    pb$tick()
  }

  # Final cleanup
  if (tolower(scm_backend) == "mlscm" && (ml_backend %||% "h2o") == "h2o") {
    if (h2o::h2o.clusterIsUp()) {
      h2o::h2o.removeAll()
      gc(verbose = FALSE)
    }
  }

  if (length(placebo_effects) == 0) {
    log$warn("All placebo runs failed.")
    empty_band <- data.frame(
      date = df_true$date, p10 = NA, p90 = NA, p2_5 = NA, p97_5 = NA,
      mean = NA, std = NA, band_low_1sd = NA, band_high_1sd = NA
    )
    return(list(treated = df_true, placebos = list(),
                p_value = NA_real_, ref_band = empty_band))
  }

  # Combine placebo effects into matrix
  placebo_mat <- purrr::reduce(placebo_effects, dplyr::full_join, by = "date") %>%
    dplyr::arrange(date) %>%
    tibble::column_to_rownames("date")

  # Compute reference bands
  ref_band <- data.frame(
    p10 = apply(placebo_mat, 1, quantile, 0.10, na.rm = TRUE),
    p90 = apply(placebo_mat, 1, quantile, 0.90, na.rm = TRUE),
    p2_5 = apply(placebo_mat, 1, quantile, 0.025, na.rm = TRUE),
    p97_5 = apply(placebo_mat, 1, quantile, 0.975, na.rm = TRUE),
    mean = rowMeans(placebo_mat, na.rm = TRUE),
    std = apply(placebo_mat, 1, sd, na.rm = TRUE)
  )
  ref_band$band_low_1sd <- ref_band$mean - ref_band$std
  ref_band$band_high_1sd <- ref_band$mean + ref_band$std
  ref_band$date <- rownames(ref_band)

  # Compute post-treatment p-value
  post_mask <- df_true$date >= cutoff_ts
  agg_fun <- if (post_agg == "sum") sum else mean
  obs_stat <- agg_fun(df_true$effect[post_mask], na.rm = TRUE)
  plc_stats <- apply(placebo_mat[post_mask, , drop = FALSE], 2, agg_fun, na.rm = TRUE)
  p_value <- (sum(abs(plc_stats) >= abs(obs_stat), na.rm = TRUE) + 1) / (length(plc_stats) + 1)

  return(list(
    treated = df_true,
    placebos = placebo_effects,
    p_value = p_value,
    ref_band = ref_band
  ))
}
