# nm_utils_prepare.r

#' Prepare and Standardize Date Column in Panel Data
#' @description
#' Ensures the input data frame contains a valid datetime column,
#' converts it to POSIXct format if needed, and renames it to "date".
#' but it is the first step in the data.table pipeline.
#'
#' @param df A data frame containing at least one datetime-like column.
#' @param prefer Optional string specifying which column to prioritize as the date column.
#' @param verbose Logical flag to enable progress messages.
#'
#' @return A data frame with a standardized "date" column of POSIXct type.
#' @export
nm_process_date <- function(df, prefer = NULL, verbose = TRUE) {
  if ("date" %in% names(df) && inherits(df$date, c("POSIXct", "Date"))) {
    df$date <- as.POSIXct(df$date)
    return(df)
  }

  if (inherits(row.names(df), "POSIXct")) {
    df$date <- as.POSIXct(row.names(df))
    row.names(df) <- NULL
    if (verbose) message("Extracted date from rownames.")
    return(df)
  }

  time_columns <- names(df)[sapply(df, function(x) inherits(x, c("POSIXct", "Date")))]
  if (length(time_columns) == 0) {
    candidates <- names(df)[sapply(df, function(x) any(grepl("^\\d{4}-\\d{2}-\\d{2}", as.character(x))))]
    for (c in candidates) {
      try_date <- suppressWarnings(as.POSIXct(df[[c]]))
      if (inherits(try_date, "POSIXct")) {
        df$date <- try_date
        if (verbose) message("Converted column '", c, "' to POSIXct and set as 'date'.")
        return(df)
      }
    }
    stop("No datetime column found.")
  }

  chosen <- if (!is.null(prefer) && prefer %in% time_columns) prefer else time_columns[1]
  df$date <- as.POSIXct(df[[chosen]])
  return(df)
} #


#' Check Data Validity
#'
#' @description
#' Validates the input data.table for modeling.
#'
#' @param dt data.table to check
#' @param feature_names Character vector of feature columns
#' @param value Name of target column
#'
#' @return Cleaned and validated data.table
#' @keywords internal
nm_check_data <- function(dt, feature_names, value) {
  log <- nm_get_logger("data.prepare.check") #

  # 1. Check target column exists
  if (!value %in% colnames(dt)) {
    err_msg <- paste("Target variable", value, "not found in the data.table") #
    log$error(err_msg) #
    stop(err_msg)
  }

  # 2. Select relevant columns
  selected_columns <- intersect(c("date", value, feature_names), colnames(dt)) #

  # This line is correct, but failed because 'dt' was not a data.table
  dt <- dt[, selected_columns, with = FALSE] #

  # Rename target column to 'value' by reference using setnames
  data.table::setnames(dt, old = value, new = "value", skip_absent = TRUE) #

  # 3. Check date column
  if (!inherits(dt$date, c("POSIXct", "Date"))) {
    err_msg <- "`date` variable must be a parsed datetime (POSIXct or Date class)." #
    log$error(err_msg) #
    stop(err_msg)
  }
  if (any(is.na(dt$date))) {
    err_msg <- "`date` must not contain missing (NA) values." #
    log$error(err_msg) #
    stop(err_msg)
  }

  # 4. Coerce to POSIXct for consistency (by reference)
  if (!inherits(dt$date, "POSIXct")) {
    dt[, date := as.POSIXct(date)] #
  }

  return(dt)
}


#' Impute Missing Values (data.table version)
#'
#' \code{nm_impute_values} imputes or removes missing values using data.table.
#'
#' @keywords internal
nm_impute_values <- function(dt, na_rm) {
  if (na_rm) {
    # na.omit is a generic function from the 'stats' package.
    # data.table provides a highly efficient method for it.
    dt <- na.omit(dt) #
  } else {
    # Impute numeric columns with median
    numeric_cols <- names(dt)[sapply(dt, is.numeric)] #
    for (col in numeric_cols) {
      median_val <- stats::median(dt[[col]], na.rm = TRUE) #
      dt[is.na(get(col)), (col) := median_val] #
    }
    # Impute character/factor columns with mode
    other_cols <- names(dt)[sapply(dt, function(x) is.character(x) || is.factor(x))] #
    for (col in other_cols) {
      mode_val <- nm_getmode(dt[[col]]) #
      dt[is.na(get(col)), (col) := mode_val] #
    }
  }
  return(dt)
}


#' Add Date Variables
#'
#' \code{nm_add_date_variables} adds date-related features using data.table.
#'
#' @keywords internal
nm_add_date_variables <- function(dt) {
  dt[, `:=`(
    date_unix = as.numeric(as.POSIXct(date)),
    day_julian = lubridate::yday(date),
    weekday = as.factor(lubridate::wday(date, label = TRUE)),
    hour = lubridate::hour(date)
  )] #
  return(dt)
}


#' Convert Ordered Factors to Factors
#'
#' \code{nm_convert_ordered_to_factor} converts ordered factors to regular factors.
#'
#' @keywords internal
nm_convert_ordered_to_factor <- function(dt) {
  ordered_cols <- names(dt)[sapply(dt, is.ordered)] #
  if (length(ordered_cols) > 0) {
    for (col in ordered_cols) {
      dt[, (col) := factor(as.character(get(col)))] #
    }
  }
  return(dt)
}


#' Split Data into Training and Testing Sets
#'
#' \code{nm_split_into_sets} splits the data.table into training and testing sets.
#'
#' @keywords internal
nm_split_into_sets <- function(dt, split_method, fraction = 0.75, seed = 7654321) {
  set.seed(seed) #

  if (split_method == 'random') {
    train_indices <- sample(seq_len(nrow(dt)), size = floor(fraction * nrow(dt))) #

    dt[, set := "testing"] #
    dt[train_indices, set := "training"] #

  } else {
    stop("Unknown split method") #
  }

  data.table::setorder(dt, date) #

  return(dt)
}


#' Prepare Data for Model Training
#'
#' @description
#' `nm_prepare_data` is a high-level wrapper that performs a series of data
#' preparation steps.
#'
#' @param df The raw input data frame.
#' @param value A string indicating the target column in `df`.
#' @param feature_names A character vector of predictor columns to use.
#' @param na_rm If TRUE, rows with NA values are removed. If FALSE, they are imputed.
#' @param split_method A string for the data splitting strategy.
#' @param fraction A numeric value for the training fraction of the split.
#' @param seed An integer for the random seed.
#' @param verbose Should the function print log messages?
#'
#' @return A prepared data frame.
#' @export
nm_prepare_data <- function(df, value, feature_names, na_rm = TRUE,
                            split_method = 'random', fraction = 0.75,
                            seed = 7654321, verbose = FALSE) {

  log <- nm_get_logger("data.prepare") #
  nm_require("data.table", hint = "install.packages('data.table')") # Ensure data.table is available
  nm_require("lubridate", hint = "install.packages('lubridate')") # For date functions

  if (verbose) log$info("Starting data preparation pipeline (using data.table)...") #

  # Process date first using the original function
  df_processed_date <- nm_process_date(df) #

  dt <- data.table::as.data.table(df_processed_date)

  # Run the sequential data.table-based preparation steps
  dt <- nm_check_data(dt, feature_names = feature_names, value = value) #
  dt <- nm_impute_values(dt, na_rm = na_rm) #
  dt <- nm_add_date_variables(dt) #
  dt <- nm_convert_ordered_to_factor(dt) #
  dt <- nm_split_into_sets(dt, split_method = split_method, fraction = fraction, seed = seed) #

  if (verbose) {
    n_train <- dt[set == "training", .N] #
    n_test <- dt[set == "testing", .N] #
    log$info(
      "Data preparation complete. Output: %d rows (%d training, %d testing).",
      nrow(dt), n_train, n_test
    ) #
  }

  # Return a standard data.frame for compatibility with other packages/functions
  return(as.data.frame(dt))
}

#' Helper function to get mode
#' @keywords internal
nm_getmode <- function(v) {
  uniqv <- unique(v[!is.na(v)]) #
  if (length(uniqv) == 0) return(NA)
  uniqv[which.max(tabulate(match(v, uniqv)))] #
}
