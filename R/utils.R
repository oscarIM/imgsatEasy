#' @title move_file
#' @rdname move_file
#' @keywords internal
#' @param files as input file
move_files <- function(files) {
  l2_pattern <- ".OC.x.nc$|.OC.NRT.nc$|.OC.NRT.x.nc$|.SST.x.nc$|SST.NRT.nc|.SST.NRT.x.nc$"
  l3_pattern <- "L3mapped.nc$"
  logfiles_pattern <- ".*txt"
  df <- tibble(
    file = files,
    date = case_when(
      var_name == "sst" ~ as_date(path_file(files), format = "%Y%m%d"),
      TRUE ~ as_date(path_file(files), format = "%Y%m%d")
    ),
    year = year(date),
    month = month(date),
    month_num = sprintf("%02d", month),
    month_ch = month(date, label = TRUE, abbr = FALSE),
    dir_type = case_when(
      str_detect(file, pattern = l2_pattern) ~ "img_L2",
      str_detect(file, pattern = l3_pattern) ~ "img_L3",
      str_detect(file, pattern = logfiles_pattern) ~ "log_files"
    ),
    dir = paste0(dir_output, "/", year, "/", month_num, "_", month_ch, "/", dir_type)
  )
  dirs <- df %>%
    distinct(dir) %>%
    pull(dir)
  walk(dirs, ~ if (!dir_exists(.)) {
    dir_create(.)
  })
  walk2(files, df$dir, ~ file_move(path = .x, new_path = .y))
}
#' @title tic
#' @rdname tic
#' @keywords internal
#' @param msg as input
tic <- function(msg = NULL) {
  assign(".tic_time", proc.time(), envir = .GlobalEnv)
  if (!is.null(msg)) {
    cat(msg, "\n")
  }
}
toc <- function() {
  start_time <- get(".tic_time", envir = .GlobalEnv)
  elapsed_time <- proc.time() - start_time
  cat("Duración total análisis: ", elapsed_time[3], "segundos\n")
}