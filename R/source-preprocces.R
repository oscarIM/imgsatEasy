#' @title get_L3
#' @description Función para obtener imágenes L3 a partir de imágenes L2 descargadas desde https://oceancolor.gsfc.nasa.gov/cgi/browse.pl
#' @param dir_input directorio en donde se almacenan las imágenes L2
#' @param dir_output directorio en donde se almacenaran las imágenes L3 resultantes
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param n_cores número de núcleos a usar. Por defecto, n_cores = 1 (corrida secuencial)
#' @param res_l2 resolución para l2bin. Por defecto, res = "1" (H: 0.5km, Q: 250m, HQ: 100m, HH: 50m, 1: 1.1km, 2: 2.3km, 4: 4.6km, 9: 9.2km, 18: 18.5km, 36: 36km, 1D: 1 degree, HD: 0.5 degree, QD: 0.25 degree)
#' @param res_l3 resolución para l3mapgen. Por defecto, res = "1km" (36km: 1080 x 540, 18km: 2160 x 1080, 9km: 4320 x 2160, 4km: 8640 x 4320, 2km: 17280 x 8640, 1km: 34560 x 17280, hkm: 69120 x 34560, qkm: 138240 x 69120, smi: 4096 x 2048, smi4: 8192 x 4096, land: 8640 x 4320)
#' @param north latitud norte para la generación de las imágenes L3
#' @param south latitud sur para la generación de las imágenes L3
#' @param west latitud oeste para la generación de las imágenes L3
#' @param east latitud este para la generación de las imágenes L3
#' @return imágenes L3
#' @importFrom fs dir_ls dir_create file_move
#' @importFrom dplyr distinct pull
#' @importFrom lubridate as_date, year, month
#' @importFrom purrr walk walk2
#' @importFrom stringr str_remove, str_detect, str_replace
#' @importFrom tibble tibble
#' @importFrom furrr future_walk
#' @importFrom future plan
#' @importFrom doParallel stopImplicitCluster
#' @export get_L3
#' @examples
#'\dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3"
#' dir_output <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3"
#' var_name <- "chlor_a"
#' n_cores <- 6
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' north <- -36.77544
#' south <- -37.29073
#' west <- -73.67165
#' east <- -73.11573
#' get_L3(dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores, res_l2=res_l2, res_l3 = res_l3, north = north, sout = south, west = west, east = east)
#'}
get_L3 <- function(dir_input, dir_output, var_name, n_cores = 1, res_l2 = "1", res_l3 = "1Km", north, south, west, east) {
  #agregar control de flujo por errores
  #pacman::p_load(fs, tidyverse, furrr, doParallel,lubridate)
  setwd(dir_input)
  cat("Descomprimiendo archivos...\n\n")
  list_tar_tmp <- dir_ls(regexp = "*.tar")
  ex_dir <- str_remove(list_tar_tmp, pattern = ".tar")
  plan(multisession)
  future_walk(list_tar_tmp, ~untar(tarfile = .x, exdir = "nc_files"), .progress = TRUE)
  name_dirs <- dir_ls(recurse = TRUE, type = "directory")
  tmp_folder <- str_detect(name_dirs, pattern = "requested_files")
  input_folder <- name_dirs[tmp_folder]
  stopImplicitCluster()
  setwd(input_folder)
  Sys.sleep(1)
  cat("Renombrado archivos y generando sistema de archivos adecuado...\n\n")
  nc_ruta_completa_tmp <- dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE)
  nc_files_tmp <- basename(nc_ruta_completa_tmp)
  #solo para renombrar
  file_move(path = basename(nc_ruta_completa_tmp), new_path = str_replace(nc_files_tmp, "^\\D+(\\d)", "\\1"))
  nc_ruta_completa_tmp <- dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE)
  nc_files_tmp <- basename(nc_ruta_completa_tmp)
  #crear dataframe
  fechas <- nc_files_tmp %>% as_date(format ="%Y%j") %>%
  tibble(fecha= .,
         ruta_completa = nc_ruta_completa_tmp,
         archivo = nc_files_tmp,
         año = year(fecha),
         mes = month(fecha),
         mes_num = sprintf("%02d", mes),
         mes_ch = month(fecha, label = TRUE, abbr = FALSE),
         directorio = paste0(dir_output, "/", var_name, "/", año, "/", mes_num,"_",mes_ch))
  directorios <- fechas %>% distinct(directorio) %>% pull(directorio)
  rm(list = ls(pattern = "tmp"))
  #crear carpertas por año/mes
  walk(directorios, ~dir_create(path = ., recurse = T))
  walk2(fechas[, 2], fechas[, 8], ~file_move(path = .x, new_path =.y))
  setwd(dir_input)
  dir_delete("nc_files")
  #ahora, moverse a cada folder y correr l2bin-l3mapgen
  cat("corriendo seadas...\n\n")
  Sys.sleep(1)
  seadas_function <- function(dir) {
    setwd(dir)
    #rutas temporales solo para probar la funcion, despues estaran dentro del programa
    l2bin <- system.file("inst", "l2bin.sh", package = "imgsatEasy")
    l3bin <- system.file("inst", "l3bin.sh", package = "imgsatEasy")
    l3mapgen <- system.file("inst", "l3mapgen.sh", package = "imgsatEasy")
    if (var_name == "sst"){
      flaguse <- "LAND, HISOLZEN"
    } else {
      flaguse <- "ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,ATMWARN,HISOLZEN,NAVFAIL,FILTER,HIGLINT"
    }
    #correr seadas scripts
    system2(command = l2bin, args = c("day", var_name, res_l2, "off", flaguse, "1"))
    system2(command = l3bin, args = c(var_name, "netCDF4", "off"))
    system2(command = l3mapgen, args = c(var_name, "netcdf4", res_l3, "smi", "nearest", north, south, west, east, "true", "false"))
    setwd(dir_input)
    }
  plan(multisession, workers = n_cores)
  future_walk(directorios, ~seadas_function(dir = .))
  stopImplicitCluster()
  cat(paste0("Terminado el pre-proceso de " , var_name))
  cat("\n\n")
}
