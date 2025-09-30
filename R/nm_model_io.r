#' Save a trained model
#'
#' \code{nm_save_model} is a high-level wrapper that saves a trained model.
#' It automatically detects the model type from its "backend" attribute and calls
#' the appropriate backend-specific saving function.
#'
#' @param model The trained model object to save. It must have a "backend" attribute.
#' @param path A string specifying the directory path where the model will be saved.
#' @param filename A string specifying the desired filename for the saved model.
#' @param verbose Should the function print log messages? Default is TRUE.
#'
#' @return A string indicating the path of the saved model.
#'
#' @export
nm_save_model <- function(model, path = './', filename = 'model', verbose = TRUE) {

  log <- nm_get_logger("model.save")

  # Automatically detect the backend from the model's attribute
  model_backend <- attr(model, "backend")

  if (verbose) log$info("Dispatching to backend for saving: %s", model_backend)

  # Dispatch to the correct implementation based on the detected backend
  if (!is.null(model_backend) && startsWith(model_backend, "h2o")) {
    return(nm_save_h2o(model = model, path = path, filename = filename, verbose = verbose))

  } else {
    err_msg <- paste("Unsupported model backend for saving:", model_backend %||% "NULL")
    log$error(err_msg)
    stop(err_msg)
  }
}


#' Load a saved model from a specified backend
#'
#' @description
#' `nm_load_model` is a high-level wrapper that dispatches the model loading task.
#'
#' @param path A string specifying the directory path where the model is saved.
#' @param filename A string specifying the filename of the saved model.
#' @param backend A string specifying the model backend (e.g., 'h2o').
#' @param verbose Should the function print log messages? Default is TRUE.
#'
#' @return The loaded model object.
#' @export
nm_load_model <- function(path = './', filename = 'automl', backend = 'h2o', verbose = TRUE) {
  # This dispatcher function requires no changes.
  log <- nm_get_logger("model.load")
  if (verbose) log$info("Dispatching to backend for loading: %s", backend)
  if (backend == 'h2o') {
    return(nm_load_h2o(path = path, filename = filename, verbose = verbose))
  } else {
    err_msg <- paste("Unsupported backend for loading:", backend)
    log$error(err_msg)
    stop(err_msg)
  }
}
