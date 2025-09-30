#' Build Uncertainty Bands from Placebo-in-Space Results
#'
#' \code{nm_effect_bands_space} builds uncertainty bands for the treated effect
#' from the output of `nm_placebo_in_space`.
#'
#' @param placebo_space_out The list returned by `nm_placebo_in_space`.
#' @param level The confidence level for the bands (e.g., 0.95).
#' @param method Method for band construction: "quantile" or "std".
#' @param verbose Should the function print log messages? Default is TRUE.
#'
#' @return A data frame with columns for the effect and its bands.
#'
#' @export
nm_effect_bands_space <- function(placebo_space_out, level = 0.95, method = "quantile", verbose = TRUE) {

  log <- nm_get_logger("analysis.bands.space")

  if (verbose) log$info("Building effect bands from placebo-in-space results (method: %s)...", method)

  df_true <- placebo_space_out$treated
  if (is.null(df_true) || !"effect" %in% colnames(df_true)) {
    log$error("`placebo_space_out$treated` must be a data frame with an 'effect' column.")
    stop("`placebo_space_out$treated` must be a data frame with an 'effect' column.")
  }
  effect <- df_true$effect

  plc_list <- placebo_space_out$placebos
  if (length(plc_list) == 0) {
    log$warn("No placebo series available; returning effect with NA bands.")
    return(data.frame(date = df_true$date, effect = effect, lower = NA, upper = NA))
  }

  plc_mat <- purrr::reduce(plc_list, dplyr::full_join, by = "date") %>%
    dplyr::right_join(df_true["date"], by = "date") %>%
    dplyr::arrange(date) %>%
    tibble::column_to_rownames("date")

  out <- data.frame(date = df_true$date, effect = effect) %>% tibble::column_to_rownames("date")

  if (method == "quantile") {
    alpha <- (1.0 - level) / 2.0
    q_low <- apply(plc_mat, 1, quantile, alpha, na.rm = TRUE)
    q_high <- apply(plc_mat, 1, quantile, 1.0 - alpha, na.rm = TRUE)
    out$lower <- effect + q_low
    out$upper <- effect + q_high
    out$plc_q_low <- q_low
    out$plc_q_high <- q_high

  } else if (method == "std") {
    z <- stats::qnorm(0.5 + level / 2.0)
    mu <- rowMeans(plc_mat, na.rm = TRUE)
    sd <- apply(plc_mat, 1, sd, na.rm = TRUE)
    out$lower <- effect + (mu - z * sd)
    out$upper <- effect + (mu + z * sd)
    out$plc_mean <- mu
    out$plc_std <- sd

  } else {
    log$error("`method` must be 'quantile' or 'std'.")
    stop("`method` must be 'quantile' or 'std'.")
  }

  return(tibble::rownames_to_column(out, "date"))
}


#' Build Uncertainty Bands from Bootstrap or Jackknife Results
#'
#' Construct confidence bands for the treatment effect using either
#' nonparametric bootstrap or leave-one-donor-out jackknife methods.
#'
#' @param df A long-format panel data frame.
#' @param date_col Name of the date column.
#' @param unit_col Name of the unit identifier column.
#' @param outcome_col Name of the outcome variable column.
#' @param treated_unit The treated unit identifier.
#' @param cutoff_date The intervention cutoff date.
#' @param donors Optional character vector of donor units.
#' @param scm_backend The synthetic control method to use: \code{"scm"} or \code{"mlscm"}.
#' @param method Uncertainty estimation method: \code{"bootstrap"} or \code{"jackknife"}.
#' @param B Number of bootstrap replications (used if method is \code{"bootstrap"}).
#' @param seed Random seed for reproducibility (used if method is \code{"bootstrap"}).
#' @param donor_frac Fraction of donors to sample in each bootstrap replication.
#' @param time_block_days Size of time blocks for bootstrap resampling.
#' @param ci_level Confidence interval level (e.g., 0.95 for 95\% bands).
#' @param verbose Logical; should the function print log messages (INFO/WARN)? Default is \code{TRUE}.
#' @param ... Additional arguments passed to the SCM dispatcher \code{nm_run_scm}.
#'
#' @return A list containing:
#' \describe{
#'   \item{treated}{A data frame with the original effect path for the treated unit.}
#'   \item{low}{A named numeric vector of the lower uncertainty band.}
#'   \item{high}{A named numeric vector of the upper uncertainty band.}
#'   \item{jackknife_effects}{(Optional, if method is \code{"jackknife"}) A data frame with effect paths from each jackknife run.}
#' }
#'
#' @importFrom stats qnorm quantile setNames
#' @export
nm_uncertainty_bands <- function(df, date_col, unit_col, outcome_col, treated_unit, cutoff_date,
                                 donors = NULL, scm_backend = "scm", method = "jackknife",
                                 B = 200, seed = 7654321, donor_frac = 0.8,
                                 time_block_days = NULL, ci_level = 0.95, verbose = TRUE, ...) {

  # --- 0. Initial Setup ---
  log <- nm_get_logger("causal.uncertainty")

  # Ensure date column is Date type and parse cutoff date
  df[[date_col]] <- as.Date(df[[date_col]])
  cutoff_ts <- as.Date(cutoff_date)

  # --- 1. Build and Validate Donor Pool ---
  all_units <- sort(unique(df[[unit_col]]))
  base_donors <- if (is.null(donors)) {
    setdiff(all_units, treated_unit)
  } else {
    intersect(donors, setdiff(all_units, treated_unit))
  }
  if (length(base_donors) < 3) stop("Need at least 3 donors in the pool.")

  # --- 2. Reference SCM Run (point estimate) ---
  df_true <- nm_run_scm(
    df = df,
    date_col = date_col,
    unit_col = unit_col,
    outcome_col = outcome_col,
    treated_unit = treated_unit,
    cutoff_date = cutoff_date,
    donors = base_donors,
    scm_backend = scm_backend,
    ...,
    verbose = FALSE
  )
  effect_index <- df_true$date
  effect_series <- df_true$effect

  # --- 3. Bootstrap Method ---
  if (tolower(method) == "bootstrap") {
    set.seed(seed)
    eff_paths <- list()

    # Pre-treatment dates for block resampling
    pre_dates <- unique(df[[date_col]][df[[date_col]] < cutoff_ts])
    pre_days <- sort(unique(as.Date(pre_dates)))

    # Progress bar
    if (!requireNamespace("progress", quietly = TRUE)) {
      stop("Package 'progress' is required for progress bar. Please install it.")
    }
    pb <- progress::progress_bar$new(
      format = "[:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = B, clear = FALSE, width = 60
    )

    for (b in seq_len(B)) {
      # (a) Resample donors
      k <- max(3, round(length(base_donors) * donor_frac))
      k <- min(k, length(base_donors))
      sub_donors <- sample(base_donors, size = k, replace = (k > length(base_donors)))

      # (b) Optional block bootstrap on pre-treatment period
      df_b <- df
      if (!is.null(time_block_days) && time_block_days > 0 && length(pre_days) >= time_block_days) {
        n_blocks <- max(1, floor(length(pre_days) / time_block_days))
        starts <- sample(0:(length(pre_days) - time_block_days), size = n_blocks, replace = TRUE)
        boot_days <- unlist(lapply(starts, function(s) pre_days[(s + 1):(s + time_block_days)]))
        boot_days <- as.Date(boot_days)

        is_pre <- df_b[[date_col]] < cutoff_ts
        keep_pre <- as.Date(df_b[[date_col]]) %in% boot_days
        df_b <- rbind(df_b[is_pre & keep_pre, ], df_b[!is_pre, ])
      }

      # (c) Run SCM on resampled data
      tryCatch({
        out_b <- nm_run_scm(
          df = df_b,
          date_col = date_col,
          unit_col = unit_col,
          outcome_col = outcome_col,
          treated_unit = treated_unit,
          cutoff_date = cutoff_date,
          donors = sub_donors,
          scm_backend = scm_backend,
          ...,
          verbose = FALSE
        )
        eff_paths[[length(eff_paths) + 1]] <- out_b$effect
      }, error = function(e) {
        if (verbose) log$warn(sprintf("Bootstrap replicate failed: %s", e$message))
      })

      pb$tick()
    }

    if (length(eff_paths) == 0) {
      return(list(treated = df_true, low = NA, high = NA))
    }

    # (d) Percentile confidence intervals
    M <- do.call(rbind, eff_paths)
    alpha <- (1 - ci_level) / 2
    q_low <- apply(M, 2, stats::quantile, probs = alpha, na.rm = TRUE)
    q_high <- apply(M, 2, stats::quantile, probs = 1 - alpha, na.rm = TRUE)

    return(list(
      treated = df_true,
      low = stats::setNames(q_low, effect_index),
      high = stats::setNames(q_high, effect_index)
    ))
  }

  # --- 4. Jackknife Method ---
  if (tolower(method) == "jackknife") {
    n <- length(base_donors)
    jackknife_paths <- list()

    if (!requireNamespace("progress", quietly = TRUE)) {
      stop("Package 'progress' is required for progress bar. Please install it.")
    }
    pb <- progress::progress_bar$new(
      format = "[:bar] :percent | Elapsed: :elapsed | ETA: :eta",
      total = n, clear = FALSE, width = 60
    )

    for (d in base_donors) {
      donors_jk <- setdiff(base_donors, d)
      tryCatch({
        out_jk <- nm_run_scm(
          df = df,
          date_col = date_col,
          unit_col = unit_col,
          outcome_col = outcome_col,
          treated_unit = treated_unit,
          cutoff_date = cutoff_date,
          donors = donors_jk,
          scm_backend = scm_backend,
          ...,
          verbose = FALSE
        )
        jackknife_paths[[length(jackknife_paths) + 1]] <- out_jk$effect
      }, error = function(e) {
        if (verbose) log$warn(sprintf("Jackknife failed for donor %s: %s", d, e$message))
      })
      pb$tick()
    }

    if (length(jackknife_paths) == 0) {
      return(list(treated = df_true, low = NA, high = NA))
    }

    # Combine jackknife results
    jack_df <- do.call(cbind, jackknife_paths)
    theta_dot <- rowMeans(jack_df, na.rm = TRUE)

    # Jackknife standard error using sweep
    diffs <- sweep(jack_df, 1, theta_dot, FUN = "-")
    se <- sqrt(((n - 1) / n) * rowSums(diffs^2, na.rm = TRUE))

    # Confidence intervals
    z <- stats::qnorm(0.5 + ci_level / 2)
    low <- effect_series - z * se
    high <- effect_series + z * se

    return(list(
      treated = df_true,
      low = stats::setNames(low, effect_index),
      high = stats::setNames(high, effect_index),
      jackknife_effects = as.data.frame(jack_df)
    ))
  }

  stop("`method` must be either 'bootstrap' or 'jackknife'")
}


#' Plot Treatment Effect with Uncertainty Bands
#'
#' @description
#' `nm_plot_effect_with_bands` creates a ggplot visualization of a treatment
#' effect and its corresponding uncertainty bands.
#'
#' @param bands_df A data frame containing the effect and its bands. Must contain
#'   at least the columns `date`, `effect`, `lower`, and `upper`.
#' @param cutoff_date Optional. A string in "YYYY-MM-DD" format to mark the
#'   intervention time with a vertical dashed line.
#' @param title The title for the plot.
#'
#' @return A ggplot object.
#' @export
nm_plot_effect_with_bands <- function(bands_df, cutoff_date = NULL, title = "Effect with Placebo Bands") {

  # Ensure date column is Date type
  bands_df$date <- as.Date(bands_df$date)

  # Basic check
  if (!all(c("effect", "lower", "upper") %in% colnames(bands_df))) {
    stop("bands_df must contain columns: 'effect', 'lower', 'upper'")
  }

  p <- ggplot2::ggplot(bands_df, ggplot2::aes(x = date)) +
    geom_ribbon(ggplot2::aes(ymin = lower, ymax = upper), fill = "grey80", alpha = 0.5) +
    geom_line(ggplot2::aes(y = effect), color = "blue", size = 1) +
    labs(title = title, x = "Date", y = "Effect") +
    theme_minimal()

  # Optional cutoff line
  if (!is.null(cutoff_date)) {
    cutoff_ts <- as.Date(cutoff_date)
    p <- p + geom_vline(xintercept = cutoff_ts, linetype = "dashed", color = "red")
  }

  return(p)
}


#' Plot treatment effect with uncertainty bands
#'
#' @param band_result A list returned by `nm_uncertainty_bands()`, containing:
#'   - treated: data.frame with columns "date" and "effect"
#'   - low: named numeric vector of lower bounds
#'   - high: named numeric vector of upper bounds
#' @param cutoff_date Optional string in "YYYY-MM-DD" format to mark intervention time
#' @param title Plot title
#'
#' @return A ggplot object showing effect curve and confidence bands
#' @export
nm_plot_uncertainty_bands <- function(band_result, cutoff_date = NULL, title = "Treatment Effect with Uncertainty Bands") {

  # --- 1. Validate input structure ---
  if (!all(c("treated", "low", "high") %in% names(band_result))) {
    stop("Input must be a list with 'treated', 'low', and 'high'.")
  }

  df <- band_result$treated
  low <- band_result$low
  high <- band_result$high

  # --- 2. Check required columns in treated data ---
  if (!("date" %in% colnames(df)) || !("effect" %in% colnames(df))) {
    stop("`treated` must contain 'date' and 'effect' columns.")
  }

  # --- 3. Assemble plotting data frame ---
  plot_df <- df[, c("date", "effect")]
  plot_df$date <- as.Date(plot_df$date)  # Ensure date format

  # Match lower and upper bounds by date
  plot_df$lower <- as.numeric(low[as.character(plot_df$date)])
  plot_df$upper <- as.numeric(high[as.character(plot_df$date)])

  # --- 4. Build ggplot ---
  p <- ggplot2::ggplot(plot_df, ggplot2::aes(x = date)) +
    geom_ribbon(ggplot2::aes(ymin = lower, ymax = upper), fill = "grey80", alpha = 0.5) +  # Confidence band
    geom_line(ggplot2::aes(y = effect), color = "blue", size = 1) +                         # Effect curve
    labs(title = title, x = "Date", y = "Effect") +
    theme_minimal()

  # --- 5. Optional: Add cutoff marker ---
  if (!is.null(cutoff_date)) {
    cutoff_ts <- as.Date(cutoff_date)
    p <- p + geom_vline(xintercept = cutoff_ts, linetype = "dashed", color = "red")
  }

  return(p)
}
