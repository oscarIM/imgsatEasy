#' @title get_csv_fix
#' @description Función para generar imágenes raster a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param var_name vector de tamyear 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamyear 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return data.frame csv
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move file_copy path_wd
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull mutate case_when bind_rows
#' @importFrom stringr str_split
#' @importFrom purrr map map2 possibly keep possibly keep walk
#' @importFrom furrr future_walk
#' @importFrom future plan cluster makeClusterPSOCK
#' @importFrom parallel stopCluster
#' @importFrom tidync tidync hyper_tibble
#' @importFrom readr write_csv
#' @export get_csv_fix
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/chlor_a/"
#' dir_output <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/test_raster_fix"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' get_csv_fix(dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores)
#' }
get_csv_fix <- function(dir_input, dir_output, var_name, n_cores = 1) {
  cat("\n\n Configurando sistema de archivos temporal...\n\n")
  # arreglar
  if (var_name == "sst") {
    var_type <- "sst"
  } else {
    var_type <- "oc"
  }
  all_nc <- tibble(
    full_path = dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE),
    file = basename(full_path),
    date = case_when(
      var_type == "sst" ~ as_date(file, format = "%Y%m%d"),
      var_type == "oc" ~ as_date(file, format = "%Y%j")
    ),
    year = year(date),
    month = month(date),
    month_num = sprintf("%02d", month),
    month_ch = month(date, label = TRUE, abbr = FALSE),
    week = week(date),
    week_num = sprintf("%02d", week),
    name_week = paste0("s_", week_num),
    directory = paste0(dir_output, "/", year, "/", month_num, "_", month_ch)
  )
  dirs <- all_nc %>%
    distinct(directory) %>%
    pull(directory)
  walk(nombre_dir, ~ dir_create(., recurse = T))
  walk2(all_nc[, 1], all_nc[, 11], ~ file_copy(.x, .y, overwrite = TRUE))
  cat("\n\n Listo...\n\n")
  cat("\n\n Iniciando creación de csv...\n\n")
  # add progess bar
  if (n_cores == 1) {
    walk(nombre_dir, ~ .internal_csv(dir = .))
  } else {
    cl <- makeClusterPSOCK(n_cores)
    plan(cluster, workers = cl)
    future_walk(nombre_dir, ~ .internal_csv(dir = .), verbose = FALSE)
    stopCluster(cl)
  }
  # mover todas las salidas a una carpeta
  dir_create(path = paste0(dir_output, "/", "csv_", var_name))
  res_path <- paste0(dir_output, "/", "csv_", var_name)
  all_csv <- dir_ls(path = dir_output, regexp = ".csv$", type = "file", recurse = TRUE)
  walk(all_csv, ~ file_move(path = ., new_path = res_path))
  dirs <- dir_ls(path = dir_output, type = "directory", recurse = FALSE)
  dir_remove <- dirs %>% str_detect(., pattern = "csv_", negate = TRUE)
  dir_remove <- dirs[dir_remove]
  dir_delete(path = dir_remove)
  cat("\n\n Generación de csv finalizada...\n\n")
}
