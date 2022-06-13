#' @title get_raster_fix
#' @description Función para generar imágenes raster a partir de imágenes satelitales L3 (formato aaajulianday)
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param season temporalidad para la generación de imágenes en formato raster ("semana", "mes", año). Se obtiene 1 raster por temporalidad (e.g. 1 raster por cada semana, o un raster por cada mes). Por defecto "mes"
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean").  Por defecto, median
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return Imágenes raster
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move file_copy path_wd
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull case_when
#' @importFrom stringr str_split
#' @importFrom purrr walk walk2
#' @importFrom terra writeRaster rast
#' @importFrom raster raster stack calc
#' @importFrom furrr future_walk
#' @importFrom future plan multisession
#' @importFrom doParallel stopImplicitCluster
#' @export get_raster_fix
#' @examples
#'\dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/chlor_a/"
#' dir_output <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/test_raster_fix"
#' season <- "mes"
#' raster_function <- "median"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' get_raster_fix(dir_input = dir_input, dir_output = dir_output, season = season,raster_function = raster_function,var_name = var_name,n_cores = n_cores)
##'}
get_raster_fix <- function(dir_input, dir_output, season = "mes", raster_function = "median", var_name, n_cores = 1) {
  cat("\n\n Configurando sistema de archivos...\n\n")
  all_nc <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE),
                   archivo = basename(ruta_completa),
                   fecha = as_date(archivo, format = "%Y%j"),
                   año = year(fecha),
                   mes = month(fecha),
                   mes_num = sprintf("%02d", mes),
                   mes_ch = month(fecha, label = TRUE, abbr = FALSE),
                   semana = week(fecha),
                   semana_num = sprintf("%02d", semana),
                   nombre_semana = paste0("s_", semana_num),
                   nombre_dir = case_when(season == "año" ~paste0(dir_output, "/", año),
                                          season == "mes" ~paste0(dir_output, "/", año,"/", mes_num, "_", mes_ch),
                                          season == "semana" ~paste0(dir_output, "/", año,"/", mes_num, "_", mes_ch, "/", nombre_semana)))
  nombre_dir <- all_nc %>% distinct(nombre_dir) %>% pull(nombre_dir)
  walk(nombre_dir, ~dir_create(., recurse = T))
  walk2(all_nc[, 1], all_nc[, 11], ~file_copy(.x, .y, overwrite = TRUE))
  cat("\n\n Listo...\n\n")
  #función interna para crear un raster a cada dir a cual se mueva
  cat("\n\n Iniciando creación de rasters...\n\n")
  internal_raster <- function(dir, raster_function) {
    setwd(dir)
    files <- dir_ls(regexp = ".nc$", recurse = T)
    tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
    n_files <- length(files)
    if (season == "año") {
      name_year <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", var_name)
      if (n_files <= 1) {
        raster <- raster(files, varname = var_name)
        raster <- rast(raster)
        writeRaster(x = raster, filename = paste0(name_file,".tif"), overwrite = TRUE)
      } else {
        raster <- stack(files, varname = var_name)
        if (raster_function == "median") {
          stack <- raster::calc(raster, fun = median, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
        if (raster_function == "mean") {
          stack <- raster::calc(raster, fun = mean, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
      }
    }
    if (season == "mes") {
      name_year <- tmp[length(tmp) - 1]
      name_month <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", name_month, "_", var_name)
      if (n_files <= 1) {
        raster <- raster(files, varname = var_name)
        raster <- rast(raster)
        writeRaster(x = raster, filename = paste0(name_file,".tif"), overwrite = TRUE)
      } else {
        raster <- stack(files, varname = var_name)
        if (raster_function == "median") {
          stack <- raster::calc(raster, fun = median, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
        if (raster_function == "mean") {
          stack <- raster::calc(raster, fun = mean, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
      }
    }
    if (season == "semana") {
      name_year <- tmp[length(tmp) - 2]
      name_month <- tmp[length(tmp) - 1]
      name_week <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", name_month, "_", name_week, "_",var_name)
      if (n_files <= 1) {
        raster <- raster(files, varname = var_name)
        raster <- rast(raster)
        writeRaster(x = raster, filename = paste0(name_file,".tif"), overwrite = TRUE)
      } else {
        raster <- stack(files, varname = var_name)
        if (raster_function == "median") {
          stack <- raster::calc(raster, fun = median, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
        if (raster_function == "mean") {
          stack <- raster::calc(raster, fun = mean, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
      }
    }
    setwd(dir_output)
  }
  #add progess bar
  if (n_cores == 1) {
    walk(nombre_dir, ~internal_raster(dir = ., raster_function = raster_function))
  } else {
    plan(multisession, workers = n_cores)
    future_walk(nombre_dir, ~internal_raster(dir = ., raster_function = raster_function), verbose = FALSE)
    stopImplicitCluster()
  }
  cat("\n\n Generación de rasters finalizada...\n\n")
  dir_create(path = paste0(dir_output,"/", "resultados_raster"))
  res_path <- paste0(dir_output,"/", "resultados_raster")
  all_tif <- dir_ls(path = dir_output, regexp = ".tif", type = "file", recurse = TRUE)
  walk(all_tif, ~file_move(path = ., new_path = res_path))
}
#' @title get_csv_fix
#' @description Función para generar imágenes raster a partir de imágenes satelitales L3 (formato aaajulianday)
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param season temporalidad para la generación de imágenes en formato raster ("semana", "mes", año). Se obtiene 1 raster por temporalidad (e.g. 1 raster por cada semana, o un raster por cada mes). Por defecto "mes"
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return data.frame csv
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move file_copy path_wd
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull mutate
#' @importFrom stringr str_split
#' @importFrom purrr map map2
#' @importFrom terra writeRaster rast
#' @importFrom raster raster as.data.frame
#' @importFrom furrr future_walk
#' @importFrom future plan multisession
#' @importFrom doParallel stopImplicitCluster
#' @export get_csv_fix
#'\dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/chlor_a/"
#' dir_output <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/test_raster_fix"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' get_csv_fix(dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores)
##'}
get_csv_fix <- function(dir_input, dir_output, var_name, n_cores) {
  cat("\n\n Configurando sistema de archivos...\n\n")
  all_nc <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE),
                   archivo = basename(ruta_completa),
                   fecha = as_date(archivo, format = "%Y%j"),
                   año = year(fecha),
                   mes = month(fecha),
                   mes_num = sprintf("%02d", mes),
                   mes_ch = month(fecha, label = TRUE, abbr = FALSE),
                   semana = week(fecha),
                   semana_num = sprintf("%02d", semana),
                   nombre_semana = paste0("s_", semana_num),
                   nombre_dir = paste0(dir_output, "/", año,"/", mes_num, "_", mes_ch))
  nombre_dir <- all_nc %>% distinct(nombre_dir) %>% pull(nombre_dir)
  walk(nombre_dir, ~dir_create(., recurse = T))
  walk2(all_nc[, 1], all_nc[, 11], ~file_copy(.x, .y, overwrite = TRUE))
  cat("\n\n Listo...\n\n")
  cat("\n\n Iniciando creación de csv...\n\n")
  internal_csv <- function(dir) {
    setwd(dir)
    files <- dir_ls(regexp = ".nc$", recurse = T)
    tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
    n_files <- length(files)
    name_year <- tmp[length(tmp) - 1]
    name_month <- tmp[length(tmp)]
    name_out <- paste0(name_year, "_",name_month, "_", var_name, ".csv")
    fecha <- all_nc %>% filter(archivo %in% files) %>% pull(fecha)
    raster_list <- map(files, ~raster::raster(., varname = var_name) %>% raster::as.data.frame(., xy = T))
    raster_list <- map2(raster_list, fecha, ~mutate(.x, fecha =.y))
    df <- bind_rows(raster_list)
    names(df)[1:3] <- c("longitud", "latitud", var_name)
    write_csv(df, file = paste0(name_year, "_", name_month, "_", var_name, ".csv"))
    setwd(dir_output)
  }
  #add progess bar
  if (n_cores == 1) {
    walk(nombre_dir, ~internal_csv(dir = .))
  } else {
    plan(multisession, workers = n_cores)
    future_walk(nombre_dir, ~internal_csv(dir = .), verbose = FALSE)
    stopImplicitCluster()
  }
  #mover todas las salidas a una carpeta llamada output
  dir_create(path = paste0(dir_output,"/", "resultados_csv"))
  res_path <- paste0(dir_output, "/", "resultados_csv")
  all_csv <- dir_ls(path = dir_output, regexp = ".csv$", type = "file", recurse = TRUE)
  walk(all_csv, ~file_move(path = ., new_path = res_path))
}

