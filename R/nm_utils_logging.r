# Define the root logger name for the package
.LOGGER_NAME <- "nm"

#' Get a Namespaced Logger
#'
#' \code{nm_get_logger} returns a namespaced logger for the package. By default,
#' loggers created this way do not produce output until logging is explicitly
#' enabled, preventing interference with the user's settings.
#'
#' @param name A character string for the child logger name (e.g., "analysis.decomposition").
#'   If NULL, returns the package root logger.
#'
#' @return A `lgr` Logger object.
#'
#' @export
nm_get_logger <- function(name = NULL) {
  # Lazily require the 'lgr' package
  nm_require("lgr", hint = "install.packages('lgr')")

  logger_name <- if (!is.null(name)) paste(.LOGGER_NAME, name, sep = ".") else .LOGGER_NAME
  return(lgr::get_logger(logger_name))
}


#' Enable Default Console Logging
#'
#' \code{nm_enable_default_logging} attaches a default console handler (appender in lgr)
#' to the root "normet" logger. This is a convenience function for interactive use.
#'
#' @param level The logging level (e.g., "info", "debug"). If NULL, falls back to the
#'   `NORMET_LOGLEVEL` environment variable or "info" by default.
#' @param fmt The format string for the logger layout.
#' @param propagate A logical value. If TRUE, logs are propagated to the global root logger.
#'
#' @return This function is called for its side effects and returns nothing.
#'
#' @export
nm_enable_default_logging <- function(level = NULL,
                                      fmt = NULL,
                                      propagate = FALSE) {

  # Lazily require the 'lgr' package
  nm_require("lgr", hint = "install.packages('lgr')")

  logger <- nm_get_logger()

  # Avoid stacking multiple console appenders
  if (length(logger$appenders) > 0 && any(sapply(logger$appenders, function(a) inherits(a, "AppenderConsole")))) {
    return(invisible(NULL))
  }

  # --- Resolve Log Level ---
  # Priority: function argument > environment variable > default
  env_level <- toupper(Sys.getenv("NORMET_LOGLEVEL", ""))

  if (!is.null(level)) {
    lvl <- level
  } else if (nzchar(env_level)) { # nzchar checks for non-empty string
    lvl <- env_level
  } else {
    lvl <- "info"
  }

  # --- Choose and Configure Handler (Appender) ---
  # lgr's AppenderConsole automatically uses color if the 'crayon' package is available
  handler <- lgr::AppenderConsole$new()
  handler$set_threshold(lvl)

  # --- Apply Formatter (Layout) ---
  # lgr format tokens: %L=Level, %n=name, %m=message, %t=timestamp
  default_fmt <- "%L [%n] %m"
  log_fmt <- fmt %||% default_fmt

  handler$set_layout(lgr::LayoutFormat$new(pattern = log_fmt))

  # --- Configure Logger ---
  logger$set_threshold(lvl)
  logger$add_appender(handler)
  logger$set_propagate(propagate)

  invisible(NULL)
}

# Helper for the null-coalescing operator `a %||% b`
`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}
