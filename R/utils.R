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
#' @title to_move_file
#' @rdname to_move_file
#' @keywords internal
#' @param files an input file
to_move_files <- function(files) {
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


