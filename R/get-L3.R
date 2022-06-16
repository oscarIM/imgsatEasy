#' @title get_L3
#' @description Función para obtener imágenes L3 a partir de imágenes L2 descargadas desde https://oceancolor.gsfc.nasa.gov/cgi/browse.pl
#' @param dir_ocssw directory en donde estan los binarios ocssw (seadas)
#' @param dir_input directory en donde se almacenan las imágenes L2
#' @param dir_output directory en donde se almacenaran las imágenes L3 resultantes
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param n_cores número de núcleos a usar. Por defecto, n_cores = 1 (corrida secuencial). La parelelización es respecto de la cantidad de sub_folder procesados simultaneamente
#' @param res_l2 resolución para l2bin. Por defecto, res = "1" (H: 0.5km, Q: 250m, HQ: 100m, HH: 50m, 1: 1.1km, 2: 2.3km, 4: 4.6km, 9: 9.2km, 18: 18.5km, 36: 36km, 1D: 1 degree, HD: 0.5 degree, QD: 0.25 degree)
#' @param res_l3 resolución para l3mapgen. Por defecto, res = "1km" (36km: 1080 x 540, 18km: 2160 x 1080, 9km: 4320 x 2160, 4km: 8640 x 4320, 2km: 17280 x 8640, 1km: 34560 x 17280, hkm: 69120 x 34560, qkm: 138240 x 69120, smi: 4096 x 2048, smi4: 8192 x 4096, land: 8640 x 4320)
#' @param north latitud norte para la generación de las imágenes L3
#' @param south latitud sur para la generación de las imágenes L3
#' @param west latitud oeste para la generación de las imágenes L3
#' @param east latitud este para la generación de las imágenes L3
#' @param keep_all mantener sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @return imágenes L3
#' @importFrom fs dir_ls dir_create file_move dir_delete file_delete
#' @importFrom dplyr distinct pull
#' @importFrom lubridate as_date year month
#' @importFrom purrr walk walk2
#' @importFrom stringr str_remove str_detect str_replace
#' @importFrom tibble tibble
#' @importFrom furrr future_walk
#' @importFrom future plan cluster makeClusterPSOCK
#' @importFrom parallel stopCluster
#' @export get_L3
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/tar_files/"
#' dir_output <- "/dir/to/desired_output/"
#' dir_ocssw <- "/dir/to/ocssw/"
#' var_name <- "chlor_a"
#' n_cores <- 6
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' north <- -36.77544
#' south <- -37.29073
#' west <- -73.67165
#' east <- -73.11573
#' get_L3(dir_ocssw = dir_ocssw, dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores, res_l2 = res_l2, res_l3 = res_l3, north = north, south = south, west = west, east = east, keep_all = "TRUE")
#' }
get_L3 <- function(dir_ocssw, dir_input, dir_output, var_name, n_cores = 1, res_l2 = "1", res_l3 = "1Km", north, south, west, east, keep_all = FALSE) {
  # agregar control de flujo por errores
  setwd(dir_input)
  cat("Descomprimiendo files...\n\n")
  list_tar_tmp <- dir_ls(regexp = "*.tar")
  ex_dir <- str_remove(list_tar_tmp, pattern = ".tar")
  nc_need <- length(list_tar_tmp)
  cl <- makeClusterPSOCK(nc_need)
  plan(cluster, workers = cl)
  future_walk(list_tar_tmp, ~ untar(tarfile = .x, exdir = "nc_files"))
  stopCluster(cl)
  name_dirs <- dir_ls(recurse = TRUE, type = "directory")
  tmp_folder <- str_detect(name_dirs, pattern = "requested_files")
  input_folder <- name_dirs[tmp_folder]
  setwd(input_folder)
  Sys.sleep(1)
  cat("\n\n Renombrado files y generando sistema de files...\n\n")
  if (var_name == "sst") {
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "SST.x.nc$|SST.NRT.x.nc$", recurse = TRUE)
    nc_files_tmp <- basename(nc_full_path_tmp)
    file_move(path = basename(nc_full_path_tmp), new_path = str_replace(nc_files_tmp, "^\\D+(\\d)", "\\1"))
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "SST.x.nc$|SST.NRT.x.nc$", recurse = TRUE)
    nc_files_tmp <- basename(nc_full_path_tmp)
    files_remove <- dir_ls(path = dir_input, regexp = "OC.x.nc$", recurse = TRUE)
    dates <- nc_files_tmp %>% as_date(format = "%Y%m%d")
  } else {
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "OC.x.nc$", recurse = TRUE)
    nc_files_tmp <- basename(nc_full_path_tmp)
    file_move(path = basename(nc_full_path_tmp), new_path = str_replace(nc_files_tmp, "^\\D+(\\d)", "\\1"))
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "OC.x.nc$", recurse = TRUE)
    nc_files_tmp <- basename(nc_full_path_tmp)
    files_remove <- dir_ls(path = dir_input, regexp = "SST.x.nc$|SST.NRT.x.nc$", recurse = TRUE)
    dates <- nc_files_tmp %>% as_date(format = "%Y%j")
  }
  file_delete(files_remove)
  # crear dataframe
  dates <- dates %>%
    tibble(
      fecha = .,
      full_path = nc_full_path_tmp,
      file = nc_files_tmp,
      year = year(fecha),
      month = month(fecha),
      month_num = sprintf("%02d", month),
      month_ch = month(fecha, label = TRUE, abbr = FALSE),
      directory = paste0(dir_output, "/", var_name, "/", year, "/", month_num, "_", month_ch)
    )
  dirs <- dates %>%
    distinct(directory) %>%
    pull(directory)
  rm(list = ls(pattern = "tmp"))
  # crear carpertas por year/month
  walk(dirs, ~ dir_create(path = ., recurse = T))
  walk2(dates[, 2], dates[, 8], ~ file_move(path = .x, new_path = .y))
  setwd(dir_input)
  dir_delete("nc_files")
  # ahora, moverse a cada folder y correr l2bin-l3mapgen
  cat("Corriendo seadas...\n\n")
  Sys.sleep(1)
  # rutas temporales solo para probar la funcion, despues estaran dentro del programa
  l2bin <- system.file("exec", "l2bin.sh", package = "imgsatEasy")
  l3bin <- system.file("exec", "l3bin.sh", package = "imgsatEasy")
  l3mapgen <- system.file("exec", "l3mapgen.sh", package = "imgsatEasy")
  # modificar los scripts
  l2bin <- readLines(l2bin)
  l3bin <- readLines(l3bin)
  l3mapgen <- readLines(l3mapgen)
  l2bin[2] <- l3bin[2] <- l3mapgen[2] <- paste0("export OCSSWROOT=${OCSSWROOT:-", dir_ocssw, "}")
  # escribir los scripts
  writeLines(l2bin, con = paste0(dir_output, "/", ".l2bin.sh"))
  writeLines(l3bin, con = paste0(dir_output, "/", ".l3bin.sh"))
  writeLines(l3mapgen, con = paste0(dir_output, "/", ".l3mapgen.sh"))
  l2bin <- paste0(dir_output, "/", ".l2bin.sh")
  l3bin <- paste0(dir_output, "/", ".l3bin.sh")
  l3mapgen <- paste0(dir_output, "/", ".l3mapgen.sh")
  system2(command = "chmod", args = c("+x", l2bin))
  system2(command = "chmod", args = c("+x", l3bin))
  system2(command = "chmod", args = c("+x", l3mapgen))
  seadas_function <- function(dir) {
    setwd(dir)
    if (var_name == "sst") {
      flaguse <- "LAND,HISOLZEN"
    } else {
      flaguse <- "ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,ATMWARN,HISOLZEN,NAVFAIL,FILTER,HIGLINT"
    }
    # correr seadas scripts
    system2(command = l2bin, args = c("day", var_name, res_l2, "off", flaguse, "1"))
    system2(command = l3bin, args = c(var_name, "netCDF4", "off"))
    system2(command = l3mapgen, args = c(var_name, "netcdf4", res_l3, "smi", "area", north, south, west, east, "true", "false"))
    setwd(dir_input)
  }
  cl <- makeClusterPSOCK(n_cores)
  plan(cluster, workers = cl)
  future_walk(dirs, ~ seadas_function(dir = .))
  stopCluster(cl)
  bins <- c(l2bin, l3bin, l3mapgen)
  file_delete(bins)
  if (keep_all) {
    cat(paste0("Terminado el pre-proceso de ", var_name))
    cat("...\n\n")
  } else {
    dir_create(path = paste0(dir_output, "/", paste0("L3_", var_name)))
    res_path <- paste0(dir_output, "/", paste0("L3_", var_name))
    all_nc <- dir_ls(path = dir_output, regexp = ".nc$", type = "file", recurse = TRUE)
    walk(all_nc, ~ file_move(path = ., new_path = res_path))
    dirs <- dir_ls(path = dir_output, type = "directory", recurse = FALSE)
    dir_remove <- dirs %>% str_detect(., pattern = paste0("L3_", var_name), negate = TRUE)
    dir_remove <- dirs[dir_remove]
    dir_delete(path = dir_remove)
    cat(paste0("Terminado el pre-proceso de ", var_name))
    cat("...\n\n")
  }
}
