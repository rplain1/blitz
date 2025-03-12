my_time <- function() strftime(Sys.time(), format = "%H:%M:%S")

log_info <- function(msg) {
  message(paste0(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " - INFO - ", msg))
}

log_warning <- function(msg) {
  warning(paste0(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " - WARNING - ", msg))
}

log_error <- function(msg) {
  warning(paste0(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), " - ERROR - ", msg))
}
