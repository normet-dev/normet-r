# Default set of statistics to compute
.DEFAULT_STATS <- c("n", "FAC2", "MB", "MGE", "NMB", "NMGE", "RMSE", "r", "COE", "IOA", "R2")

#' Compute Statistics from DataFrame Columns
#'
#' \code{nm_Stats} computes requested statistics from two columns in a data frame.
#'
#' @param df Input data frame containing columns with predictions and observations.
#' @param mod Column name with model predictions. Default is "value_predict".
#' @param obs Column name with ground-truth observations. Default is "value".
#' @param statistic A character vector of metrics to compute. If NULL, a default set is used.
#'
#' @return A single-row data frame with the requested statistics.
#'
#' @export
nm_Stats <- function(df, mod = "value_predict", obs = "value", statistic = NULL) {
  if (is.null(statistic)) {
    statistic <- .DEFAULT_STATS
  }

  missing_cols <- setdiff(c(mod, obs), colnames(df))
  if (length(missing_cols) > 0) {
    stop("Stats: columns not found in DataFrame: ", paste(missing_cols, collapse = ", "))
  }

  y_pred <- df[[mod]]
  y_true <- df[[obs]]

  return(.stats_from_arrays(y_pred, y_true, statistic))
}


#' Predict with a Model and Compute Statistics
#'
#' \code{nm_modStats} makes predictions with a model on a data frame and computes statistics.
#'
#' @param df The prepared dataset. Must contain the target column "value".
#' @param model A trained model compatible with the `nm_predict` function.
#' @param subset A character string specifying which data split to evaluate ("training", "testing", "all").
#'   If NULL and a "set" column exists, returns one row per split plus an "all" row.
#' @param statistic A character vector of metrics to compute; defaults to a comprehensive set.
#' @param verbose Should the function print log messages? Default is TRUE.
#'
#' @return A tidy data frame of metrics with a "set" column indicating which slice was scored.
#'
#' @export
nm_modStats <- function(df, model, subset = NULL, statistic = NULL, verbose = FALSE) {

  # Get a namespaced logger
  log <- nm_get_logger("analysis.modStats")

  if (is.null(statistic)) {
    statistic <- .DEFAULT_STATS
  }

  # Internal function to calculate stats for one data slice
  .one <- function(df_in, tag) {
    if (verbose) log$info("Predicting and calculating stats for subset: '%s'", tag)
    y_pred <- nm_predict(model, df_in)
    y_true <- df_in$value
    st <- .stats_from_arrays(y_pred, y_true, statistic)
    st$set <- tag
    return(st)
  }

  if (!is.null(subset)) {
    # Evaluate only a specific subset
    if (subset != "all") {
      if (!"set" %in% colnames(df)) {
        stop("DataFrame has no 'set' column but a `subset` was requested.")
      }
      df_use <- df[df$set == subset, ]
    } else {
      df_use <- df
    }
    return(.one(df_use, subset))
  }

  # If subset is NULL, compute for each split and for "all"
  if (!"set" %in% colnames(df)) {
    return(.one(df, "all"))
  }

  pieces <- list()
  for (s in unique(df$set)) {
    pieces <- c(pieces, list(.one(df[df$set == s, ], s)))
  }
  pieces <- c(pieces, list(.one(df, "all")))

  if (verbose) log$info("Finished calculating stats.")
  return(dplyr::bind_rows(pieces))
}


#' @keywords internal
.fac2 <- function(y_pred, y_true) {
  epsilon <- 1e-9
  ratio <- y_pred / (y_true + epsilon)
  finite_mask <- is.finite(ratio)
  if (!any(finite_mask)) return(NA_real_)
  r <- ratio[finite_mask]
  return(mean((r >= 0.5) & (r <= 2.0)))
}

#' @keywords internal
.stats_from_arrays <- function(y_pred, y_true, statistic) {
  mask <- is.finite(y_pred) & is.finite(y_true)
  yhat <- as.numeric(y_pred[mask])
  yobs <- as.numeric(y_true[mask])
  n <- length(yhat)
  if (n == 0) {
    keys <- statistic
    if ("r" %in% statistic) keys <- unique(c(keys, "p_level"))
    out <- stats::setNames(rep(NA_real_, length(keys)), keys)
    if ("n" %in% statistic) out["n"] <- 0
    return(as.data.frame(as.list(out)))
  }
  diff <- yhat - yobs
  adiff <- abs(diff)
  res <- list()
  if ("n" %in% statistic) res$n <- as.integer(n)
  if ("FAC2" %in% statistic) res$FAC2 <- .fac2(yhat, yobs)
  if ("MB" %in% statistic) res$MB <- mean(diff)
  if ("MGE" %in% statistic) res$MGE <- mean(adiff)
  if ("RMSE" %in% statistic) res$RMSE <- sqrt(mean(diff^2))
  sum_obs <- sum(yobs)
  if ("NMB" %in% statistic) res$NMB <- if (sum_obs != 0) sum(diff) / sum_obs else NA_real_
  if ("NMGE" %in% statistic) res$NMGE <- if (sum_obs != 0) sum(adiff) / sum_obs else NA_real_
  denom_abs_obs <- sum(abs(yobs - mean(yobs)))
  if ("COE" %in% statistic) {
    res$COE <- if (denom_abs_obs != 0) 1.0 - (sum(adiff) / denom_abs_obs) else NA_real_
  }
  if ("IOA" %in% statistic) {
    lhs <- sum(adiff)
    rhs <- 2.0 * denom_abs_obs
    if (rhs == 0 && lhs == 0) {
      res$IOA <- 1.0
    } else if (rhs == 0) {
      res$IOA <- NA_real_
    } else {
      res$IOA <- if (lhs <= rhs) 1.0 - lhs / rhs else rhs / lhs - 1.0
    }
  }
  r_val <- NA_real_
  p_val <- NA_real_
  if ("r" %in% statistic || "R2" %in% statistic) {
    if (n > 1) {
      test <- suppressWarnings(stats::cor.test(yhat, yobs, method = "pearson"))
      r_val <- test$estimate
      p_val <- test$p.value
    }
  }
  if ("r" %in% statistic) {
    res$r <- r_val
    if (!is.finite(p_val) || p_val >= 0.1) {
      res$p_level <- ""
    } else if (p_val >= 0.05) {
      res$p_level <- "+"
    } else if (p_val >= 0.01) {
      res$p_level <- "*"
    } else if (p_val >= 0.001) {
      res$p_level <- "**"
    } else {
      res$p_level <- "***"
    }
  }
  if ("R2" %in% statistic) res$R2 <- if (is.finite(r_val)) r_val^2 else NA_real_
  keys_needed <- statistic
  if ("r" %in% statistic) keys_needed <- union(keys_needed, "p_level")
  for(k in keys_needed) {
    if (is.null(res[[k]])) res[[k]] <- NA
  }
  return(as.data.frame(res))
}
