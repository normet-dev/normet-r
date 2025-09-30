#' Extract and Sort Feature Names by Importance from a Model
#'
#' \code{nm_extract_features} is a high-level wrapper that extracts feature names from a model.
#' It checks the model's 'backend' attribute to dispatch to the appropriate backend-specific function.
#'
#' @param model The trained model object, which should have a 'backend' attribute.
#' @param verbose Should the function print log messages? Default is FALSE.
#' @param ... Additional arguments to be passed to the backend-specific function,
#'   such as `importance_ascending`.
#'
#' @return A vector of sorted feature names based on their importance.
#'
#' @export
nm_extract_features <- function(model, verbose = FALSE, ...) {

  log <- nm_get_logger("features.extract")
  model_backend <- attr(model, "backend")

  if (!is.null(model_backend) && startsWith(model_backend, "h2o")) {
    if (verbose) log$info("Dispatching to backend: %s", model_backend)
    # Pass verbose and other arguments down to the implementation
    return(nm_extract_features_h2o(model, verbose = verbose, ...))

  } else {
    err_msg <- "Model type not supported or 'backend' attribute is missing."
    log$error(err_msg)
    stop(err_msg)
  }
}


#' Extract and Sort Feature Names by Importance from an H2O Model
#'
#' \code{nm_extract_features_h2o} is the specific implementation for H2O models.
#' It uses H2O's `h2o.varimp` function to extract feature importance.
#'
#' @param model The trained H2O model object.
#' @param importance_ascending A logical value for sorting order. Default is `FALSE`.
#' @param verbose Should the function print log messages? Default is FALSE.
#'
#' @return A vector of sorted feature names based on their importance.
#'
nm_extract_features_h2o <- function(model, importance_ascending = FALSE, verbose = FALSE) {

  # Get a namespaced logger
  log <- nm_get_logger("features.extract.h2o")

  # Ensure the h2o package is available
  nm_require("h2o", hint = "install.packages('h2o')")

  if (verbose) {
    log$info("Extracting feature importance from H2O model...")
  }

  # Extract variable importance using H2O's varimp function
  varimp_df <- as.data.frame(h2o::h2o.varimp(model))

  # Check if variable importance data is available
  if (nrow(varimp_df) == 0) {
    err_msg <- "The H2O model does not have variable importance information."
    log$error(err_msg) # Errors are always logged
    stop(err_msg)
  }

  # Extract feature names and sort by relative importance
  feature_names <- varimp_df$variable
  feature_names <- feature_names[order(varimp_df$relative_importance, decreasing = !importance_ascending)]

  # Return the sorted feature names
  return(feature_names)
}
