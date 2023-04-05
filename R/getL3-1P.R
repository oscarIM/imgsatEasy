#' @title getL3_1P
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
#' @param need_extract mantener sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @param sort_files crearr sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @param flaguse cadena de texto con los nosmbres de los flag a usar
#' @param period_name cadena texto temporalidad a usar (e.g "semana_01")
#' @param area_weighting area_weighting
#' @param fudge ??fusionador
#' @param begin_day fecha inicio de temporalidad(ymd)
#' @param end_day fecha temporalidad de temporalidad (ymd)
#' @return imágenes L3
#' @importFrom fs dir_ls dir_create file_move dir_delete file_delete path_file file_copy
#' @importFrom readr read_lines write_lines
#' @importFrom dplyr distinct pull filter
#' @importFrom lubridate as_date year month
#' @importFrom purrr walk walk2 possibly
#' @importFrom stringr str_remove str_detect str_replace
#' @importFrom tibble tibble
#' @importFrom furrr future_walk future_walk2
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom utils untar
#' @importFrom tictoc tic toc
#' @export getL3_1P
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecol/Desktop/R_package/test_functions/test_new_L3/data_raw"
#' dir_output <- "/home/evolecol/Desktop/R_package/test_functions/test_new_L3"
#' dir_ocssw <- "/home/evolecol/SeaDAS/ocssw"
#' var_name <- "sst"
#' n_cores <- 6
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' north <- -35
#' south <- -40
#' west <- -75
#' east <- -70
#' need_extract_and_format <- TRUE
#' sort_files <- FALSE
#' period_name <- "w_08"
#' area_weighting <- 0
#' fudge <- 2
#' begin_day <- "2022-12-26"
#' end_day <- "2022-12-28"
#' getL3_1P(dir_ocssw = dir_ocssw, dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores, res_l2 = res_l2, res_l3 = res_l3, north = north, south = south, west = west, east = east, need_extract_and_format = need_extract_and_format, sort_files = sort_files, begin_day = begin_day, end_day = end_day, period_name = period_name)
#' }
getL3_1P <- function(dir_ocssw, dir_input, dir_output, var_name, n_cores = 1, res_l2 = "1", res_l3 = "1Km", north, south, west, east, need_extract_and_format = TRUE, sort_files = FALSE, fudge, area_weighting, begin_day, end_day, period_name) {
  current_wd <- path_wd()
  oc <- c(".OC.x.nc$", ".OC.NRT.nc$", ".OC.NRT.x.nc$")
  patterns_oc <- paste(oc, collapse = "|")
  sst <- c(".SST.x.nc$", "SST.NRT.nc", ".SST.NRT.x.nc$")
  patterns_sst <- paste(sst, collapse = "|")
  patterns_l2 <- c(oc, sst)
  patterns_l2 <- paste0(patterns_l2, collapse = "|")
  tic(msg = "Duración total análisis")
  if (need_extract_and_format) {
    setwd(dir_input)
    cat("Descomprimiendo archivos...\n\n")
    list_tar_tmp <- dir_ls(regexp = "*.tar")
    ## chequar si length list_tar_tmp == 0, indica error
    ex_dir <- str_remove(list_tar_tmp, pattern = ".tar")
    if (length(list_tar_tmp) <= 1) {
      walk(list_tar_tmp, ~ untar(tarfile = .x, exdir = "nc_files"))
    } else {
      cl <- parallel::makeForkCluster(n_cores)
      plan(cluster, workers = cl)
      future_walk(list_tar_tmp, ~ untar(tarfile = .x, exdir = "nc_files"))
      parallel::stopCluster(cl)
      rm(cl)
    }
    name_dirs <- dir_ls(recurse = TRUE, type = "directory")
    tmp_folder <- str_detect(name_dirs, pattern = "requested_files")
    input_folder <- name_dirs[tmp_folder]
    setwd(input_folder)
    Sys.sleep(1)
    cat("Formateando archivos...\n\n")
    if (var_name == "sst") {
      nc_full_path_tmp <- dir_ls(path = dir_input, regexp = patterns_sst, recurse = TRUE)
      old_name <- basename(nc_full_path_tmp)
      new_name <- str_replace(old_name, "^\\D+(\\d)", "\\1")
      file_move(path = old_name, new_path = new_name)
      nc_full_path_tmp <- dir_ls(path = dir_input, regexp = patterns_sst, recurse = TRUE)
      nc_files_tmp <- basename(nc_full_path_tmp)
      files_remove <- dir_ls(path = dir_input, regexp = patterns_oc, recurse = TRUE)
    } else {
      nc_full_path_tmp <- dir_ls(path = dir_input, regexp = patterns_oc, recurse = TRUE)
      old_name <- basename(nc_full_path_tmp)
      new_name <- str_replace(old_name, "^\\D+(\\d)", "\\1")
      file_move(path = old_name, new_path = new_name)
      nc_full_path_tmp <- dir_ls(path = dir_input, regexp = patterns_oc, recurse = TRUE)
      nc_files_tmp <- basename(nc_full_path_tmp)
      files_remove <- dir_ls(path = dir_input, regexp = patterns_sst, recurse = TRUE)
    }
    file_delete(files_remove)
    dir_create(path = dir_output)
    walk(nc_full_path_tmp, ~ file_move(path = ., new_path = dir_output))
    dir_delete(path = paste0(dir_input, "/nc_files"))
  } else {
    setwd(dir_input)
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = patterns_l2, recurse = TRUE)
    Sys.sleep(1)
    cat("Moviendo archivos L2 formateados..\n\n")
    dir_create(path = dir_output)
    walk(nc_full_path_tmp, ~ file_copy(path = ., new_path = dir_output, overwrite = TRUE))
  }
  setwd(dir_output)
  # crear infile_list
  files_df <- dir_ls(path = dir_output, regexp = ".nc$") %>%
    tibble(
      infile_l2bin = .
    ) %>%
    mutate(date = case_when(
      var_name == "sst" ~ as.Date(path_file(infile_l2bin), format = "%Y%m%d"),
      TRUE ~ as.Date(path_file(infile_l2bin), format = "%Y%m%d")
    ))
  files_df <- files_df %>%
    filter(between(date, as_date(begin_day), as_date(end_day)))
  cat(files_df$infile_l2bin, file = "infile.txt", sep = "\n")
  outfile_l2bin <- paste0(period_name, "_", var_name, "_", res_l2, "km_L3b_tmp.nc")
  outfile_mapgen <- paste0(period_name, "_", var_name, "_", res_l2, "km_L3mapped.nc")
  rm(list = ls(pattern = "tmp"))
  # correr l2bin-l3mapgen
  cat("Corriendo wrappers de seadas...\n\n")
  Sys.sleep(1)
  # rutas temporales solo para probar la funcion, despues estaran dentro del programa
  if (var_name == "sst") {
    seadas_bins <- dir_ls(path = system.file("exec", package = "imgsatEasy"))[-1]
  } else {
    seadas_bins <- dir_ls(path = system.file("exec", package = "imgsatEasy"))[-2]
  }
  names(seadas_bins) <- path_file(seadas_bins)
  seadas_bins <- map(seadas_bins, ~ read_lines(.))
  # pasar a map para consistencia del código
  for (i in 1:length(seadas_bins)) {
    seadas_bins[[i]][2] <- paste0("export OCSSWROOT=${OCSSWROOT:-", dir_ocssw, "}")
  }
  # escribir los scripts
  names_bins <- paste0(".", names(seadas_bins))
  walk2(seadas_bins, names_bins, ~ write_lines(.x, file = paste0(dir_input, "/", .y)))
  seadas_bins <- map(names_bins, ~ paste0(dir_input, "/", .))
  # AUX#
  if (var_name == "sst") {
    seadas_l2bin <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[1]))
      system2(command = seadas_bins[1], args = c(infile, ofile, "regional", var_name, res_l2, "off", "LAND,HISOLZEN", 2, north, south, east, west, area_weighting, "qual_sst", "SST"))
    }
  } else {
    seadas_l2bin <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[1]))
      system2(command = seadas_bins[1], args = c(infile, ofile, "regional", var_name, res_l2, "off", "ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,ATMWARN,HISOLZEN,NAVFAIL,FILTER,HIGLINT", 2, north, south, east, west, area_weighting))
    }
  }
  # AUX#
  seadas_l3mapgen <- function(infile, ofile) {{ system2(command = "chmod", args = c("+x", seadas_bins[2]))
    system2(command = seadas_bins[2], args = c(infile, ofile, var_name, "netcdf4", res_l3, "platecarree", "area", north, south, west, east, "true", "no", fudge)) }}
  tic(msg = "Duración l2bin")
  seadas_l2bin(infile = "infile.txt", ofile = outfile_l2bin)
  toc()
  # stopCluster(cl)
  # filtrar solo los archivos para los cuales hubo resultados
  l3binned_files <- dir_ls(path = dir_output, regexp = "_L3b_tmp.nc$", recurse = TRUE)
  tic(msg = "Duración l3mapgen")
  seadas_l3mapgen(infile = l3binned_files, ofile = outfile_mapgen)
  toc()
  cat(paste0("Fin de la generación de imágenes L3 de ", var_name, "\n\n"))
  ##############################################################################
  ## movimiento de archivos. ACA CREAR CARPETA OUTPUT Y MOVER TODO
  files_l3mapped <- dir_ls(path = dir_output, regexp = "L3mapped.nc$", recurse = FALSE)
  files_l2 <- dir_ls(path = dir_output, regexp = patterns_l2, recurse = FALSE)
  files_logfiles <- dir_ls(path = dir_output, regexp = ".txt$", recurse = FALSE)
  file_delete(c(files_l2, files_logfiles))
  cat("Moviendo archivos a sus respectivos directorios...\n\n")
  if (sort_files) {
    file_list <- list(files_l2, files_l3mapped, files_logfiles)
    walk(file_list, ~ move_files(files = .))
    file_delete(c(seadas_bins[[1]], seadas_bins[[2]], seadas_bins[[3]]))
    files_remove <- dir_ls(path = dir_output, regexp = "_tmp.nc$")
    file_delete(files_remove)
  } else {
    file_move(path = files_l3mapped, new_path = dir_output)
    file_delete(c(seadas_bins[[1]], seadas_bins[[2]]))
    files_remove <- dir_ls(path = dir_output, regexp = "_tmp.nc$")
    file_delete(files_remove)
  }
  setwd(current_wd)
  cat("Fin \n\n")
  toc()
}
