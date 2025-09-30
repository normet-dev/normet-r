#' @importFrom data.table := .N
#' @importFrom magrittr %>%
#' @importFrom foreach %dopar%
#' @importFrom tidyselect all_of
#' @importFrom ggplot2 ggplot aes geom_line geom_ribbon geom_vline labs theme_minimal
#' @importFrom stats sd median na.omit var
#' @importFrom utils capture.output object.size
#' @importFrom h2o h2o.removeAll


utils::globalVariables(c(
  "..variables_resample", ".", ".N", ".data", "effect", "lower", "upper",
  "set", "s", "normalised", "p_value", "ref_band_event_time",
  "placebo_stats", "date_d", "var"
))
