#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
#' @param lhs A value or the magrittr placeholder.
#' @param rhs A function call using the magrittr semantics.
#' @return The result of calling `rhs(lhs)`.
NULL

#' @title move_file
#' @rdname move_file
#' @keywords internal
#' @param files an input file
move_files <- function(files) {
    l2_pattern <- ".L2_LAC_OC.x.nc$|SST.x.nc$|SST.NRT.x.nc$"
    l3_pattern <- "L3mapped.nc$"
    logfiles_pattern <- ".*txt"
    df <- tibble(
      file = files,
      date = case_when(
        var_name == "sst" ~ as_date(path_file(files), format = "%Y%m%d"),
        TRUE ~ as_date(path_file(files), format = "%Y%j")
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

#' @title seadas_l2bin
#' @rdname seadas_l2bin
#' @keywords internal
#' @param infile an input file
#' @param ofile an input file
seadas_l2bin <- function(infile, ofile) {
      flaguse <- case_when(
        var_name == "sst" ~ "LAND,HISOLZEN",
        TRUE ~ "ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,ATMWARN,HISOLZEN,NAVFAIL,FILTER,HIGLINT"
      )
      system2(command = seadas_bins[1], args = c(infile, ofile, "day", var_name, res_l2, "off", flaguse, "0"))
    } %>% possibly(., otherwise = "Error en archivo de entrada")

#' @title seadas_l3bin
#' @rdname seadas_l3bin
#' @keywords internal
#' @param infile an input file
#' @param ofile an input file
seadas_l3bin <- function(infile, ofile) {
      system2(command = seadas_bins[2], args = c(infile, ofile, var_name, "netCDF4", "off"))
    } %> possibly(., otherwise = "Error en archivo de entrada")

#' @title seadas_l3mapgen
#' @rdname seadas_l3mapgen
#' @keywords internal
#' @param infile an input file
#' @param ofile an input file
 seadas_l3mapgen <- function(infile, ofile) {
      system2(command = seadas_bins[3], args = c(infile, ofile, var_name, "netcdf4", res_l3, "smi", "area", north, south, west, east, "true", "false"))
    } %>% possibly(., otherwise = "Error en archivo de entrada")
  