#' Require an R Package or Object with a Helpful Error Message
#'
#' \code{nm_require} checks for the availability of a package and stops with an
#' informative error if it's not installed. It can also be used to retrieve a
#' specific object from a package's namespace.
#'
#' @param module A string specifying the package and, optionally, the object to load.
#'   The R-idiomatic `::` is used as a separator.
#'   Examples:
#'   - `"dplyr"` (checks for the package)
#'   - `"dplyr::select"` (retrieves the `select` function)
#' @param hint A string providing an installation hint (e.g., "install.packages('dplyr')")
#'   to be included in the error message if the package is missing.
#'
#' @return If requesting a package, returns the package namespace. If requesting an
#'   object, returns the object itself.
#'
#' @examples
#' \dontrun{
#' # Check for a package
#' nm_require("dplyr", hint = "install.packages('dplyr')")
#'
#' # Get a specific function
#' select_fun <- nm_require("dplyr::select", hint = "install.packages('dplyr')")
#'
#' # This will fail with a helpful error message if 'notapackage' is not installed
#' nm_require("notapackage", hint = "remotes::install_github('user/notapackage')")
#' }
nm_require <- function(module, hint = NULL) {

  module <- trimws(module)

  # Determine if a specific object or a whole package is being requested
  if (grepl("::", module, fixed = TRUE)) {
    # --- Path 1: Requesting a specific object (e.g., "dplyr::select") ---
    parts <- strsplit(module, "::", fixed = TRUE)[[1]]
    pkg_name <- parts[1]
    obj_name <- parts[2]

    # Check if the package is installed
    if (!requireNamespace(pkg_name, quietly = TRUE)) {
      msg <- paste0("Optional dependency '", pkg_name, "' is required but not installed.")
      if (!is.null(hint)) {
        msg <- paste0(msg, " Install it via: ", hint)
      }
      stop(msg, call. = FALSE)
    }

    # Check if the object exists within the package
    if (!exists(obj_name, envir = asNamespace(pkg_name))) {
        stop("Package '", pkg_name, "' does not provide the requested object '", obj_name, "'.", call. = FALSE)
    }

    # Return the object itself
    return(get(obj_name, envir = asNamespace(pkg_name)))

  } else {
    # --- Path 2: Requesting a package (e.g., "dplyr") ---
    pkg_name <- module

    # Check if the package is installed
    if (!requireNamespace(pkg_name, quietly = TRUE)) {
      msg <- paste0("Optional dependency '", pkg_name, "' is required but not installed.")
      if (!is.null(hint)) {
        msg <- paste0(msg, " Install it via: ", hint)
      }
      stop(msg, call. = FALSE)
    }

    # Return the package namespace as the R equivalent of the "module object"
    return(asNamespace(pkg_name))
  }
}
