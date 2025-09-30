#' Predict using a trained model
#'
#' \code{nm_predict} is a high-level wrapper that generates predictions from a trained model.
#' It checks the model's 'backend' attribute to dispatch to the appropriate backend-specific function.
#'
#' @param model The trained model object, which should have a 'backend' attribute.
#' @param newdata A data frame containing the new data for prediction.
#' @param feature_names (Optional) A character vector of feature names to use.
#' @param verbose Should the function print log messages? Default is TRUE.
#' @param ... Additional arguments to be passed to the backend-specific function.
#'
#' @return A numeric vector of predicted values.
#'
#' @export
nm_predict <- function(model, newdata, feature_names = NULL, verbose = FALSE, ...) {

  log <- nm_get_logger("model.predict")
  model_backend <- attr(model, "backend")

  if (!is.null(model_backend) && startsWith(model_backend, "h2o")) {
    if (verbose) log$info("Dispatching to H2O backend for prediction.")
    # Pass arguments in the correct order to the implementation
    return(nm_predict_h2o(model = model, newdata = newdata, feature_names = feature_names, verbose = verbose, ...))

  } else {
    if (verbose) log$warn("Model 'backend' attribute not found or is unsupported. Using generic stats::predict().")
    # The generic stats::predict() also expects (model, newdata)
    return(as.numeric(stats::predict(model, newdata)))
  }
}
