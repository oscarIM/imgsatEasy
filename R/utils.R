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
#' @title toc
#' @rdname toc
#' @keywords internal
#' @param msg as input
toc <- function() {
  start_time <- get(".tic_time", envir = .GlobalEnv)
  elapsed_time <- proc.time() - start_time
  cat("Duración total análisis: ", elapsed_time[3], "segundos\n")
}

#' @title nc_to_table
#' @rdname nc_to_table
#' @keywords internal
#' @param file as input
#' @param var_name as input
nc_to_table <- function(file, var_name) {
  nc <- nc_open(file)
  on.exit(nc_close(nc))
  times <- c(ncatt_get(nc, 0, "time_coverage_start")$value, ncatt_get(nc, 0, "time_coverage_end")$value)
  lat <- ncvar_get(nc, "lat") %>% as.vector()
  lon <- ncvar_get(nc, "lon") %>% as.vector()
  var_tmp <- ncvar_get(nc, var_name)
  df <- tidyr::expand_grid(lat = lat, lon = lon) %>%
    dplyr::mutate(!!sym(var_name) := as.vector(var_tmp),
                  date1 = times[1], date2 = times[2])
  return(df)

}
#' @title write_table
#' @rdname write_table
#' @keywords internal
#' @param df as input
#' @param file as input
write_table <- function(df, file, format_output) {
  base_name <- stringr::str_remove(string = file, pattern = "_1km_L3mapped.nc$")
  if (format_output  == "parquet") {
    file_name <- paste0(base_name, ".parquet")
    arrow::write_parquet(x = df, sink = file_name)
  } else if (format_output == "csv") {
    file_name <- paste0(base_name, ".csv")
    ###readr
    write_csv(x = df, file = file_name, progress = FALSE)
  } else {
    stop("Formato no soportado: ", format_output )
  }
}

#' @title process_tables
#' @rdname process_tables
#' @keywords internal
#' @param file as input
#' @param ext_file as input

#' @title scale_x_longitude
#' @rdname scale_x_longitude
#' @keywords internal
scale_x_longitude <- function(name = "", ticks = 30,
                              breaks = seq(-180, 360, by = ticks),
                              expand = c(0, 0),
                              labels = LonLabel,
                              trans = "identity",
                              ...) {
    # labels = waiver()
    ggplot2::scale_x_continuous(name = name, expand = expand,
                                breaks = breaks,
                                labels = labels,
                                trans = trans,
                                ...)
}
#' @title scale_x_latitude
#' @rdname scale_x_latitude
#' @keywords internal
scale_y_latitude <- function(name = "", ticks = 30,
                             breaks = seq(-90, 90, by = ticks),
                             expand = c(0, 0),
                             labels = LatLabel, ...) {
    ggplot2::scale_y_continuous(name = name, expand = expand,
                       breaks = breaks, labels = labels,
                       ...)
}
