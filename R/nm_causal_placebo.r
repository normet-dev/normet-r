#' Perform a Placebo-in-Space Analysis
#'
#' @description
#' `nm_placebo_in_space` tests the significance of a treatment effect by iteratively
#' applying the synthetic control method to each donor unit.
#'
#' @param df A long-format panel data frame.
#' @param date_col Name of the date column.
#' @param unit_col Name of the unit identifier column.
#' @param outcome_col Name of the outcome column.
#' @param treated_unit The treated unit identifier.
#' @param cutoff_date The intervention cutoff date.
#' @param donors Optional vector of donor units.
#' @param scm_backend SCM backend ("scm" or "mlscm").
#' @param post_agg Aggregation method for post-treatment effect ("mean" or "sum").
#' @param verbose Whether to print log messages (INFO/WARN). Default TRUE.
#' @param ... Arguments forwarded to `nm_run_scm()`.
#'
#' @return A list with:
#'   - treated: data.frame with observed/synthetic/effect for true treated unit
#'   - placebos: list of placebo effect series
#'   - p_value: placebo-based significance estimate
#'   - ref_band: reference band (p10, p90, mean, std) across placebo effects
#' @export
nm_placebo_in_space <- function(df, date_col, unit_col, outcome_col,
                                treated_unit, cutoff_date, donors = NULL,
                                scm_backend = "scm", post_agg = "mean",
                                verbose = TRUE, ...) {

  log <- nm_get_logger("causal.placebo.space") #

  # --- 1. Validate aggregation method ---
  post_agg <- tolower(post_agg) #
  if (!post_agg %in% c("mean", "sum")) {
    log$warn("Invalid post_agg='%s'; falling back to 'mean'.", post_agg) #
    post_agg <- "mean" #
  }

  # --- 2. Parse cutoff date ---
  cutoff_ts <- as.Date(cutoff_date) #

  # --- 3. Log setup ---
  extra_args <- list(...) #
  ml_backend <- if (tolower(scm_backend) == "mlscm") extra_args$backend else NULL #
  if (verbose) {
    log$info("Placebo-in-space: scm_backend=%s | backend=%s | treated=%s | cutoff=%s",
             scm_backend, ml_backend %||% "none", treated_unit, cutoff_date) #
  }

  # Initialize H2O once at the beginning if using the mlscm backend
  if (tolower(scm_backend) == "mlscm" && (ml_backend %||% "h2o") == "h2o") {
    nm_init_h2o(verbose = verbose) #
  }

  # --- 4. Run true treated unit analysis ---
  df_true <- nm_run_scm(
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
  ) #

  # --- 5. Build donor pool ---
  all_units <- sort(unique(df[[unit_col]])) #
  donor_pool <- if (is.null(donors)) {
    setdiff(all_units, treated_unit) #
  } else {
    intersect(sort(unique(donors)), setdiff(all_units, treated_unit)) #
  }

  if (length(donor_pool) == 0) {
    log$warn("No donor units available for placebo-in-space.") #
    empty_band <- data.frame(
      date = df_true$date, p10 = NA, p90 = NA, p2_5 = NA, p97_5 = NA,
      mean = NA, std = NA, band_low_1sd = NA, band_high_1sd = NA
    ) #
    return(list(treated = df_true, placebos = list(),
                p_value = NA_real_, ref_band = empty_band)) #
  }

  # --- 6. Progress bar ---
  if (!requireNamespace("progress", quietly = TRUE)) {
    stop("Package 'progress' is required for progress bar. Please install it.") #
  }
  pb <- progress::progress_bar$new(
    format = "  Placebos [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
    total = length(donor_pool), clear = FALSE, width = 60
  ) #

  placebo_effects <- list() #
  for (u in donor_pool) {
    tryCatch({
      syn_u <- nm_run_scm(
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
      ) #
      placebo_effects[[u]] <- syn_u[, c("date", "effect")] %>%
        dplyr::rename(!!u := effect) #
    }, error = function(e) {
      if (verbose) log$warn("Placebo failed for unit %s: %s", u, e$message) #
    })

    if (tolower(scm_backend) == "mlscm" && (ml_backend %||% "h2o") == "h2o") {
      h2o.removeAll() #
      gc(verbose = FALSE) #
    }

    pb$tick() #
  }

  if (length(placebo_effects) == 0) {
    log$warn("All placebo runs failed.") #
    return(list(treated = df_true, placebos = list(),
                p_value = NA_real_, ref_band = empty_band)) #
  }

  # --- 7. Combine placebo effects into matrix ---
  placebo_mat <- purrr::reduce(placebo_effects, dplyr::full_join, by = "date") %>%
    dplyr::arrange(date) %>%
    tibble::column_to_rownames("date") #

  # --- 8. Reference band calculation ---
  ref_band <- data.frame(
    p10 = apply(placebo_mat, 1, quantile, 0.10, na.rm = TRUE),
    p90 = apply(placebo_mat, 1, quantile, 0.90, na.rm = TRUE),
    p2_5 = apply(placebo_mat, 1, quantile, 0.025, na.rm = TRUE),
    p97_5 = apply(placebo_mat, 1, quantile, 0.975, na.rm = TRUE),
    mean = rowMeans(placebo_mat, na.rm = TRUE),
    std = apply(placebo_mat, 1, sd, na.rm = TRUE)
  ) #
  ref_band$band_low_1sd <- ref_band$mean - ref_band$std #
  ref_band$band_high_1sd <- ref_band$mean + ref_band$std #
  ref_band$date <- rownames(ref_band) #

  # --- 9. P-value calculation ---
  post_mask <- df_true$date >= cutoff_ts #
  agg_fun <- if (post_agg == "sum") sum else mean #
  obs_stat <- agg_fun(df_true$effect[post_mask], na.rm = TRUE) #
  plc_stats <- apply(placebo_mat[post_mask, , drop = FALSE], 2, agg_fun, na.rm = TRUE) #
  p_value <- (sum(abs(plc_stats) >= abs(obs_stat), na.rm = TRUE) + 1) / (length(plc_stats) + 1) #

  return(list(treated = df_true, placebos = placebo_effects,
              p_value = p_value, ref_band = ref_band)) #
}


#' Perform a Placebo-in-Time Analysis
#'
#' @description
#' `nm_placebo_in_time` tests significance by running the SCM analysis multiple times
#' with fake "treatment" dates that occur before the actual treatment date. This
#' version is updated to run the 'mlscm' backend serially to prevent H2O conflicts
#' and includes memory management.
#'
#' @param df A long-format panel data frame.
#' @param min_pre_period The minimum number of pre-period data points required for a
#'   placebo run.
#' @param placebo_every The interval (in number of time points) at which to select
#'   placebo dates (e.g., every 7 days).
#' @param verbose Should the function print log messages and a progress bar?
#'   Default is `TRUE`.
#' @param ... Arguments passed to the SCM dispatcher (`nm_run_syn`), such as `unit_col`,
#'  `outcome_col`, `treated_unit`, `cutoff_date`, `donors`, and `scm_backend`.
#'
#' @return A list containing the following elements:
#' \describe{
#'   \item{treated}{The data frame of results for the true treated unit and cutoff date.}
#'   \item{placebos}{A list where each element is a numeric vector representing the effect path from a single placebo run.}
#'   \item{p_value}{A p-value estimated from comparing the true effect to the distribution of placebo effects.}
#'   \item{ref_band_event_time}{A data frame containing quantiles and mean of placebo effects, aligned to "event time" (time since intervention).}
#'   \item{placebo_stats}{A numeric vector of the aggregated post-period statistics for each placebo run.}
#' }
#' @export
nm_placebo_in_time <- function(df, min_pre_period = 30, placebo_every = 7, verbose = TRUE, ...) {

  log <- nm_get_logger("causal.placebo.time") #
  args <- list(...) #
  cutoff_dt <- as.Date(args$cutoff_date) #
  scm_backend <- tolower(args$scm_backend %||% "scm") #

  if (verbose) log$info("Starting placebo-in-time analysis (backend: %s)...", scm_backend) #

  # Initialize H2O once at the beginning if using the mlscm backend
  if (scm_backend == "mlscm" && (args$backend %||% "h2o") == "h2o") {
    nm_init_h2o(verbose = verbose) #
  }

  # --- 1. Run analysis for the true treated unit ---
  df_true <- nm_run_scm(df = df, verbose = verbose, ...) #
  post_len <- sum(df_true$date >= cutoff_dt) #
  if (post_len == 0) stop("No post-period observations at/after the true cutoff date.") #

  # --- 2. Identify Placebo Cutoff Dates ---
  all_dates <- sort(unique(df$date[df[[args$unit_col]] == args$treated_unit])) #
  placebo_candidates <- c() #

  # --- 3. Run placebo tests (conditionally parallel or serial) ---
  if (!requireNamespace("progress", quietly = TRUE)) {
    stop("Package 'progress' is required for progress bar. Please install it.") #
  }
  pb <- progress::progress_bar$new(
    format = "  Placebos-in-time [:bar] :percent | Elapsed: :elapsed | ETA: :eta",
    total = length(placebo_candidates), clear = FALSE, width = 80
  ) #

  jobs <- list() # Initialize jobs list

  if (scm_backend == "scm") {
    n_cores <- args$n_cores %||% (parallel::detectCores() - 1) #
    cl <- parallel::makeCluster(n_cores) #
    doSNOW::registerDoSNOW(cl) #
    on.exit(parallel::stopCluster(cl), add = TRUE)
    opts <- list(progress = function(n) pb$tick()) #

    jobs <- foreach::foreach(
      pc = placebo_candidates,
      .packages = c("dplyr"),
      # Export a more robust list of dependencies
      .export = c(".LOGGER_NAME", "nm_run_scm", "nm_scm", "nm_mlscm", "nm_get_logger", "nm_require", "%||%"),
      .options.snow = opts
    ) %dopar% {
      #
    } #

  } else { # This block is for "mlscm"
    if (verbose) log$info("Running 'mlscm' placebo tests serially to ensure H2O stability...")

    for (pc in placebo_candidates) {
      pb$tick() #
      result <- tryCatch({
        placebo_args <- args #
        placebo_args$cutoff_date <- as.character(pc) #
        syn_pc <- do.call(nm_run_scm, c(list(df = df, verbose = FALSE), placebo_args)) #
        post_period_mask <- syn_pc$date >= pc #
        seg <- syn_pc$effect[post_period_mask][1:post_len] #
        agg_fun <- if ((args$post_agg %||% "mean") == "sum") sum else mean #
        stat <- agg_fun(seg, na.rm = TRUE) #
        list(date = pc, segment = seg, stat = stat) #
      }, error = function(e) {
        if(verbose) log$warn("Placebo run failed for date %s: %s", as.character(pc), e$message)
        NULL
      })

      if ((args$backend %||% "h2o") == "h2o") {
        h2o.removeAll() #
        gc(verbose = FALSE) #
      }

      jobs[[length(jobs) + 1]] <- result
    }
  }

  # --- 4. Aggregate Results & Calculate P-value ---
  jobs <- Filter(Negate(is.null), jobs) #
  if (length(jobs) == 0) {
    log$warn("All placebo-in-time runs failed.") #
    return(list(treated = df_true, placebos = list(), p_value = NA_real_, ref_band_event_time = NULL)) #
  }

  return(list(
    treated = df_true,
    placebos = setNames(lapply(jobs, function(j) j$segment),
                        sapply(jobs, function(j) as.character(j$date))),
    p_value = p_value,
    ref_band_event_time = ref_band_event_time,
    placebo_stats = placebo_stats
  )) #
}
