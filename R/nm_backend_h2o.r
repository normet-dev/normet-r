#' Safely Initialize H2O Cluster
#'
#' @description
#' Initializes a local or remote H2O cluster with memory and core settings.
#' Handles connection failures, retries initialization, and logs cluster info robustly.
#'
#' @param n_cores Number of CPU cores to use. If NULL, uses all available cores.
#' @param max_mem_size Maximum memory allocation for H2O JVM (e.g. "8G").
#' @param verbose Whether to print progress and cluster info. Default is TRUE.
#' @param max_retries Number of times to retry initialization if it fails. Default is 2.
#' @param retry_delay Seconds to wait between retries. Default is 5.
#' @param port Port for H2O cluster. Default is 54321.
#'
#' @return H2O connection object (invisible)
#' @export
nm_init_h2o <- function(n_cores = NULL, max_mem_size = "8G",
                        verbose = TRUE, max_retries = 2,
                        retry_delay = 5, port = 54321) {
  log <- nm_get_logger("h2o.init") #
  nm_require("h2o", hint = "install.packages('h2o')") #

  nthreads <- if (!is.null(n_cores) && n_cores > 0) n_cores else -1 #

  # --- 1. If already running, reuse connection ---
  already_running <- tryCatch({
    !is.null(h2o::h2o.getConnection())
  }, error = function(e) FALSE) #

  if (already_running) {
    if (verbose) log$info("H2O is already initialized.") #
    return(invisible(h2o::h2o.getConnection())) #
  }

  # --- 2. Retry loop ---
  attempt <- 0 #
  success <- FALSE #
  while (!success && attempt < max_retries) {
    attempt <- attempt + 1 #
    tryCatch({
      h2o::h2o.init(nthreads = nthreads,
                    max_mem_size = max_mem_size,
                    port = port) #
      # confirm cluster is alive
      h2o::h2o.clusterStatus() #
      success <- TRUE #
    }, error = function(e) {
      log$warn("H2O init attempt %d/%d failed: %s",
               attempt, max_retries, e$message) #
      if (attempt < max_retries) Sys.sleep(retry_delay) #
    })
  }

  if (!success) {
    stop("H2O failed to initialize after ", max_retries, " attempts.") #
  }

  # --- 3. Progress bar control ---
  if (verbose) {
    h2o::h2o.show_progress() #
  } else {
    h2o::h2o.no_progress() #
  }

  # --- 4. Log cluster info ---
  tryCatch({
    cluster_info <- h2o::h2o.clusterInfo() #

    # Ensure cluster_info is a list before trying to access elements with $.
    if (is.list(cluster_info) && "nodes" %in% names(cluster_info)) {
      node_info <- cluster_info$nodes[1, ] #
      mode <- if (h2o::h2o.is_client()) "client" else "local" #
      log$info("H2O cluster initialized | mode=%s | version=%s | cores=%s | mem=%s",
               mode, cluster_info$version, node_info$num_cpus, node_info$max_mem) #
    } else {
      # If the return value is not a list, log its raw content instead of erroring.
      log$info("H2O cluster is running. Status: %s", paste(as.character(cluster_info), collapse = " ")) #
    }

  }, error = function(e) {
    log$warn("Could not retrieve H2O cluster info: %s", e$message) #
  })

  return(invisible(h2o::h2o.getConnection())) #
}


#' H2O Cluster Watchdog (Silent)
#'
#' Checks whether the H2O cluster is alive. If the connection is lost,
#' it restarts the cluster safely. Suppresses H2O's own console output
#' (cluster info, version, etc.), only logs via nm_get_logger.
#'
#' @param verbose Logical flag to enable logging messages (INFO/WARN). Default TRUE.
#'
#' @return Invisibly returns TRUE if the cluster is alive after the check.
nm_h2o_watchdog <- function(verbose = TRUE) {
  log <- nm_get_logger("h2o.watchdog")
  nm_require("h2o", hint = "install.packages('h2o')")

  alive <- FALSE

  # --- 1. Try to query cluster status silently ---
  tryCatch({
    invisible(capture.output({
      suppressMessages({
        h2o::h2o.clusterStatus()
      })
    }))
    alive <- TRUE
    if (verbose) log$info("H2O cluster is alive.")
  }, error = function(e) {
    if (verbose) log$warn("H2O connection lost: %s", e$message)

    # --- 2. Attempt to restart cluster ---
    if (verbose) log$info("Restarting H2O cluster...")
    nm_stop_h2o(quiet = TRUE)
    Sys.sleep(5)  # allow JVM cleanup
    nm_init_h2o(verbose = verbose)

    # --- 3. Verify restart silently ---
    tryCatch({
      invisible(capture.output({
        suppressMessages({
          h2o::h2o.clusterStatus()
        })
      }))
      alive <<- TRUE
      if (verbose) log$info("H2O cluster successfully restarted.")
    }, error = function(e2) {
      alive <<- FALSE
      log$error("Failed to restart H2O cluster: %s", e2$message)
    })
  })

  invisible(alive)
}


#' Save Trained H2O Model
#'
#' @description
#' `nm_save_h2o` saves a trained H2O model. It handles path normalization
#' and file renaming. This is the H2O-specific implementation.
#'
#' @param model The trained H2O model object to save.
#' @param path A string specifying the directory path where the model will be saved.
#' @param filename A string specifying the desired filename for the saved model.
#' @param verbose Should the function print log messages? Default is `TRUE`.
#'
#' @return A string indicating the full path of the saved model.
#' @keywords internal
nm_save_h2o <- function(model, path = './', filename = 'automl', verbose = TRUE) {
  log <- nm_get_logger("model.save.h2o")

  # Convert the directory path to an absolute path before using it.
  # mustWork = FALSE because the directory might not exist yet.
  path <- normalizePath(path, mustWork = FALSE)

  # Ensure the output directory exists
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
  }

  model_path <- h2o::h2o.saveModel(model, path = path, force = TRUE)
  new_model_path <- file.path(path, filename)

  if (verbose) log$info("Saving H2O model to: %s", new_model_path)

  file.rename(model_path, new_model_path)

  if (verbose) log$info("H2O model saved successfully.")

  return(new_model_path)
}



#' Load Saved H2O Model
#'
#' \code{nm_load_h2o} loads a previously saved H2O model from disk
#'
#' @param path A string specifying the directory path where the model is saved. Default is './'.
#' @param filename A string specifying the name of the saved model file. Default is 'automl'.
#' @param verbose Should the function print log messages? Default is TRUE.
#'
#' @return The loaded H2O model object with its "backend" attribute correctly set.
nm_load_h2o <- function(path = './', filename = 'automl', verbose = TRUE) {

  log <- nm_get_logger("model.load.h2o")

  # --- Path normalization from previous step ---
  model_path <- file.path(path, filename)
  tryCatch({
    full_model_path <- normalizePath(model_path, mustWork = TRUE)
  }, warning = function(w) {
    stop("File not found at path: '", model_path, "'", call. = FALSE)
  })

  if (verbose) log$info("Loading H2O model from: %s", full_model_path)

  # Load the H2O model
  model <- h2o::h2o.loadModel(full_model_path)

  attr(model, "backend") <- "h2o"

  if (verbose) log$info("H2O model loaded successfully and 'backend' attribute attached.")

  return(model)
}



#' Train a model using H2O AutoML
#'
#' \code{nm_train_h2o} trains a model using H2O AutoML with retry logic and cluster health checks.
#' It validates input, initializes H2O, and returns the best model from the AutoML run.
#'
#' @param df Prepared data frame with training/testing split.
#' @param value Name of the target column.
#' @param variables Character vector of predictor column names.
#' @param model_config Optional list of AutoML configuration overrides.
#' @param seed Random seed for reproducibility.
#' @param n_cores Number of CPU cores to use. If NULL, auto-detected.
#' @param verbose Whether to print progress and diagnostics.
#'
#' @return Trained H2O model object (leader), with 'backend' attribute.
nm_train_h2o <- function(df, value = "value", variables = NULL, model_config = NULL,
                         seed = 7654321, n_cores = NULL, verbose = TRUE) {

  log <- nm_get_logger("model.train.h2o")

  # --- 1. Validate input ---
  if (length(unique(variables)) != length(variables)) stop("`variables` contains duplicates.")
  if (!all(variables %in% colnames(df))) stop("Some `variables` not found in input data.")

  df_train <- if ("set" %in% colnames(df)) {
    df %>% dplyr::filter(set == "training") %>% dplyr::select(dplyr::all_of(c(value, variables)))
  } else {
    df %>% dplyr::select(dplyr::all_of(c(value, variables)))
  }

  # --- 2. Default model config ---
  default_model_config <- list(
    max_retries = 3,
    max_models = 10,
    nfolds = 5,
    max_mem_size = "16G",
    include_algos = c("GBM"),
    sort_metric = "RMSE",
    save_model = FALSE,
    filename = "automl",
    path = "./",
    seed = seed
  )
  if (!is.null(model_config)) {
    default_model_config <- utils::modifyList(default_model_config, model_config)
  }

  # --- 3. Resolve core count ---
  n_cores <- ifelse(is.null(n_cores), max(1, parallel::detectCores() - 1), n_cores)

  # --- 4. Internal training function ---
  train_model_internal <- function() {
    # Validate training data
    if (nrow(df_train) < 5) stop("Too few rows in training data.")
    if (any(is.na(df_train[[value]]))) stop("Target column contains NA.")
    if (all(apply(df_train[, variables, drop = FALSE], 2, var, na.rm = TRUE) == 0)) {
      stop("All predictors have zero variance.")
    }

    df_h2o <- h2o::as.h2o(df_train)
    response <- value
    predictors <- setdiff(colnames(df_h2o), response)

    if (verbose) log$info("Training with H2O AutoML...")

    auto_ml <- h2o::h2o.automl(
      x = predictors, y = response, training_frame = df_h2o,
      include_algos = default_model_config$include_algos,
      max_models = default_model_config$max_models,
      nfolds = default_model_config$nfolds,
      seed = default_model_config$seed
    )

    if (verbose) log$info("Best model obtained: %s", auto_ml@leader@model_id)
    return(auto_ml@leader)
  }

  # --- 5. Retry loop with watchdog ---
  retry_count <- 0
  model <- NULL
  while (is.null(model) && retry_count < default_model_config$max_retries) {
    retry_count <- retry_count + 1

    #  Ensure H2O cluster is alive before training
    nm_h2o_watchdog(verbose = FALSE)

    tryCatch({
      model <- train_model_internal()
    }, error = function(e) {
      log$error("Training attempt %d failed: %s", retry_count, e$message)
      if (retry_count < default_model_config$max_retries) {
        log$warn("Retrying... (%d of %d)", retry_count, default_model_config$max_retries)
        nm_stop_h2o(quiet = TRUE)
        Sys.sleep(5)
        nm_init_h2o(n_cores, max_mem_size = default_model_config$max_mem_size, verbose = verbose)
      }
    })
  }

  if (is.null(model)) {
    stop("Failed to train the model after ", default_model_config$max_retries, " attempts.")
  }

  # --- 6. Attach backend and optionally save ---
  attr(model, "backend") <- "h2o"
  if (default_model_config$save_model) {
    nm_save_model(model, default_model_config$path, default_model_config$filename, verbose = verbose)
  }

  return(model)
}


#' Predict using a trained H2O model with batching
#'
#' @description
#' `nm_predict_h2o` performs predictions using a trained H2O model.
#' It supports automatic batching based on memory size, safely handles H2O
#' initialization, and is updated to correctly handle both data.frame and
#' data.table inputs during feature selection.
#'
#' @param model A trained H2O model object.
#' @param newdata A data frame or data.table containing the feature matrix for prediction.
#' @param feature_names Optional character vector of feature names to use.
#' @param parallel Logical flag for multi-threading (not used directly here).
#' @param verbose Logical flag to enable logging messages.
#'
#' @return A numeric vector of predicted values.
nm_predict_h2o <- function(model, newdata, feature_names = NULL, parallel = TRUE, verbose = FALSE) {
  log <- nm_get_logger("model.predict.h2o") #

  # --- 1. Extract and validate feature columns ---
  X <- tryCatch({
    feature_cols <- if (is.null(feature_names)) nm_extract_features(model) else feature_names #
    use_cols <- intersect(feature_cols, colnames(newdata)) #

    if (length(use_cols) != length(feature_cols) && verbose) {
      missing <- setdiff(feature_cols, colnames(newdata)) #
      log$warn("Missing features in newdata: %s", paste(missing, collapse = ", ")) #
    }

    if (length(use_cols) == 0) {
      log$error("No model features found in `newdata`.") #
      stop("No model features found in `newdata`.") #
    }

    # Check if newdata is a data.table and use the appropriate syntax.
    if (inherits(newdata, "data.table")) {
      newdata[, use_cols, with = FALSE] #
    } else {
      newdata[, use_cols, drop = FALSE] # Fallback for standard data.frames
    }

  }, error = function(e) {
    log$warn("Feature extraction failed: %s", e$message) #
    if (ncol(newdata) == 0) stop("`newdata` has no columns to predict on.") #
    newdata #
  })

  n_rows <- nrow(X) #
  if (n_rows == 0) return(numeric(0)) #

  # --- 2. Ensure H2O is initialized ---
  nm_require("h2o", hint = "install.packages('h2o')") #
  if (is.null(h2o::h2o.getConnection())) {
    if (verbose) log$info("Initializing H2O connection...") #
    nm_init_h2o(verbose = verbose) #
  }

  # --- 3. Determine batch size based on memory ---
  target_mb <- nm_auto_target_mb(model) #
  jvm_expand <- as.numeric(Sys.getenv("NM_H2O_JVM_EXPANSION", "1.5")) #
  row_size <- max(1, as.numeric(object.size(X)) / n_rows) #
  auto_batch <- floor(max(
    10000,
    min((target_mb * 1024^2) / (row_size * min(max(jvm_expand, 1.0), 4.0)), n_rows)
  )) #

  # --- 4. Internal prediction helper ---
  predict_batch <- function(df_chunk) {
    hf <- NULL #
    preds <- NULL #
    tryCatch({
      hf <- h2o::as.h2o(df_chunk) #
      pred_h2o <- h2o::h2o.predict(model, hf) #
      preds <- as.data.frame(pred_h2o)$predict #
    }, error = function(e) {
      log$error("H2O prediction failed: %s", e$message) #
      stop(e) #
    }, finally = {
      if (!is.null(hf)) h2o::h2o.rm(hf) #
    })
    return(preds) #
  }

  # --- 5. Execute prediction ---
  if (n_rows <= auto_batch) {
    if (verbose) log$info("Predicting in a single batch of %d rows.", n_rows) #
    return(as.numeric(predict_batch(X))) #
  } else {
    starts <- seq(1, n_rows, by = auto_batch) #
    if (verbose) log$info("Predicting in %d batches of up to %d rows each.", length(starts), auto_batch) #
    parts <- lapply(starts, function(start) {
      stop_idx <- min(start + auto_batch - 1, n_rows) #
      X_chunk <- X[start:stop_idx, , drop = FALSE] #
      predict_batch(X_chunk) #
    })
    return(as.numeric(unlist(parts))) #
  }
}

#' Determine Target Batch Size in MB
#'
#' @param model A trained H2O model object.
#' @return Integer value in MB for batch sizing.
#' @keywords internal
nm_auto_target_mb <- function(model) {
  val <- attr(model, "_predict_batch_mb", exact = TRUE) #
  if (!is.null(val) && is.numeric(val)) return(max(64, min(as.integer(val), 4096))) #

  env_val <- Sys.getenv("NM_H2O_BATCH_MB") #
  if (nzchar(env_val)) {
    mb <- suppressWarnings(as.integer(env_val)) #
    if (!is.na(mb)) return(max(64, min(mb, 4096))) #
  }

  return(512) #
}


#' Shut Down an H2O Cluster
#'
#' \code{nm_stop_h2o} shuts down the attached H2O cluster if one is running.
#'
#' @param quiet If TRUE (the default), shut down without a confirmation prompt.
#'   If FALSE, the user will be prompted to confirm.
#'
#' @return This function is called for its side effects and returns nothing.
#'
#' @export
nm_stop_h2o <- function(quiet = TRUE) {

  log <- nm_get_logger("h2o.stop")

  # This outer tryCatch handles the case where the h2o package might not be installed
  tryCatch({
    nm_require("h2o", hint = "install.packages('h2o')")

    # Check if a cluster is actually running
    if (h2o::h2o.clusterIsUp()) {
      h2o::h2o.shutdown(prompt = !quiet)
      log$info("H2O cluster shutdown requested.")
    } else {
      log$debug("H2O shutdown skipped: no active cluster found.")
    }

  }, error = function(e) {
    # This will catch errors from nm_require (if h2o is not installed) or other issues
    log$debug("H2O shutdown skipped: %s", e$message)
  })

  invisible(NULL)
}
