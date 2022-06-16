#' @title get_raster_fix_raw
#' @description Función para generar imágenes raster a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3 (formato .nc)
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster (formato raster .tif)
#' @param season temporalidad para la generación de imágenes en formato raster ("week", "month", "year").
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean").  Por defecto, median
#' @param var_name vector de tamyear 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamyear 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return Imágenes raster sin interpolar(.tif)
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move file_copy path_wd
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull case_when
#' @importFrom stringr str_split
#' @importFrom purrr walk walk2 possibly keep map
#' @importFrom terra writeRaster rast
#' @importFrom raster stack calc flip rotate
#' @importFrom furrr future_walk
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom doParallel registerDoParallel
#' @importFrom ncdf4 nc_open
#' @importFrom oceanmap nc2raster
#' @importFrom parallel stopCluster
#' @export get_raster_fix_raw
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/L3files/"
#' dir_output <- "/dir/to/desired_output/"
#' season <- "month"
#' raster_function <- "median"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' get_raster_fix(dir_input = dir_input, dir_output = dir_output, season = season, raster_function = raster_function, var_name = var_name, n_cores = n_cores)
#' }
get_raster_fix_raw <- function(dir_input, dir_output, season = "month", raster_function = "median", var_name, n_cores = 1) {
  cat("\n\n Configurando sistema de archivos temporal...\n\n")
  # acortar
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
    directory = case_when(
      season == "year" ~ paste0(dir_output, "/", year),
      season == "month" ~ paste0(dir_output, "/", year, "/", month_num, "_", month_ch),
      season == "week" ~ paste0(dir_output, "/", year, "/", month_num, "_", month_ch, "/", name_week)
    )
  )
  dirs <- all_nc %>%
    distinct(directory) %>%
    pull(directory)
  walk(dirs, ~ dir_create(., recurse = T))
  walk2(all_nc[, 1], all_nc[, 11], ~ file_copy(.x, .y, overwrite = TRUE))
  cat("\n\n Listo...\n\n")
  # función interna para crear un raster a cada dir a cual se mueva
  cat("\n\n Iniciando creación de rasters...\n\n")
  # mover la función interna a otro archivo
  internal_raster <- function(dir, raster_function) {
    setwd(dir)
    files <- dir_ls(regexp = ".nc$", recurse = T)
    # configurando nombre del stack final
    tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
    if (season == "year") {
      name_year <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", var_name)
    }
    if (season == "month") {
      name_year <- tmp[length(tmp) - 1]
      name_month <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", name_month, "_", var_name)
    }
    if (season == "week") {
      name_year <- tmp[length(tmp) - 2]
      name_month <- tmp[length(tmp) - 1]
      name_week <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", name_month, "_", name_week, "_", var_name)
    }
    # proceso de los archivos
    possible_nc_open <- possibly(.f = nc_open, otherwise = NULL)
    possible_nc2raster <- possibly(.f = nc2raster, otherwise = NULL)
    nc_files_tmp <- map(files, ~ possible_nc_open(.))
    nc_file_tmp <- nc_files_tmp %>% keep(~ !is.null(.))
    nc_raster_tmp <- map(nc_files_tmp, ~ possible_nc2raster(., var_name, lonname = "lon", latname = "lat"))
    nc_raster_tmp <- nc_raster_tmp %>% keep(~ !is.null(.))
    nc_raster_flip_tmp <- map(nc_raster_tmp, ~ flip(., "y"))
    rasters <- map(nc_raster_flip_tmp, ~ rotate(.))
    # ext_df <- purrr::map(rasters, ~raster::extent(.))
    # xmin <- purrr::map(ext_df, "xmin") %>% bind_rows()
    # xmax <- purrr::map(ext_df, "xmax") %>% bind_rows()
    # ymin <- purrr::map(ext_df, "ymin") %>% bind_rows()
    # ymax <- purrr::map(ext_df, "ymax") %>% bind_rows()
    # final_ext <- data.frame("xmin" = t(xmin), "xmax" = t(xmax), "ymin" = t(ymin), "ymax" = t(ymax))
    # ext_mask <- raster(ymx = max(final_ext$ymax), ymn = min(final_ext$ymin), xmn = min(final_ext$xmin), xmx = max(final_ext$xmax), resolution = 0.01)
    # rasters <- purrr::map(rasters, ~raster::resample(., ext_mask, method = "bilinear"))
    rasters <- stack(rasters)
    if (raster_function == "median") {
      stack <- raster::calc(rasters, fun = median, na.rm = TRUE)
      name_file <- paste0(name_file, "_", raster_function)
      stack <- rast(stack)
      writeRaster(x = stack, filename = paste0(name_file, "_raw.tif"), overwrite = TRUE)
    }
    if (raster_function == "mean") {
      stack <- raster::calc(rasters, fun = mean, na.rm = TRUE)
      name_file <- paste0(name_file, "_", raster_function)
      stack <- rast(stack)
      writeRaster(x = stack, filename = paste0(name_file, "_raw.tif"), overwrite = TRUE)
    }
    setwd(dir_output)
  }
  # add progess bar
  if (n_cores == 1) {
    walk(dirs, ~ internal_raster(dir = ., raster_function = raster_function))
  } else {
    cl <- makeForkCluster(n_cores)
    registerDoParallel(cl)
    future_walk(dirs, ~ internal_raster(dir = ., raster_function = raster_function), verbose = FALSE)
    stopCluster(cl)
  }
  dir_create(path = paste0(dir_output, "/", "raster_", var_name))
  res_path <- paste0(dir_output, "/", "raster_", var_name)
  all_tif <- dir_ls(path = dir_output, regexp = ".tif$", type = "file", recurse = TRUE)
  walk(all_tif, ~ file_move(path = ., new_path = res_path))
  dirs <- dir_ls(path = dir_output, type = "directory", recurse = FALSE)
  dir_remove <- dirs %>% str_detect(., pattern = paste0("raster_",var_name), negate = TRUE)
  dir_remove <- dirs[dir_remove]
  dir_delete(path = dir_remove)
  cat("\n\n Generación de rasters finalizada...\n\n")
}
