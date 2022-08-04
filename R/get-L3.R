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
#' @param need_extract mantener sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @param sort_files crearr sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @return imágenes L3
#' @importFrom fs dir_ls dir_create file_move dir_delete file_delete path_file
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
#' @importFrom progressr with_progress progressor
#' @export get_L3
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/tar_files/"
#' dir_output <- paste0(dir_input, "/", "output")
#' dir_ocssw <- "/dir/to/ocssw/"
#' var_name <- "chlor_a"
#' n_cores <- 6
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' north <- -36.77544
#' south <- -37.29073
#' west <- -73.67165
#' east <- -73.11573
#' need_extract <- TRUE
#' sort_files <- FALSE
#' get_L3(dir_ocssw = dir_ocssw, dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores, res_l2 = res_l2, res_l3 = res_l3, north = north, south = south, west = west, east = east, need_extract = need_extract, sort_files = sort_files)
#' }
get_L3 <- function(dir_ocssw, dir_input, dir_output, var_name, n_cores = 1, res_l2 = "1", res_l3 = "1Km", north, south, west, east, need_extract = TRUE, sort_files = FALSE) {
  # agregar control de flujo por errores
  tic(msg = "Duración total análisis")
  if (need_extract) {
    setwd(dir_input)
    cat("Descomprimiendo archivos...\n\n")
    list_tar_tmp <- dir_ls(regexp = "*.tar")
    ex_dir <- str_remove(list_tar_tmp, pattern = ".tar")
    if (length(list_tar_tmp) <= 1) {
      walk(list_tar_tmp, ~ untar(tarfile = .x, exdir = "nc_files"))
    } else {
      nc_need <- length(list_tar_tmp)
      cl <- parallel::makeForkCluster(nc_need)
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
  } else {
    setwd(dir_input)
    Sys.sleep(1)
    cat("Formateando archivos...\n\n")
  }
  if (var_name == "sst") {
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "SST.x.nc$|SST.NRT.x.nc$", recurse = TRUE)
    old_name <- basename(nc_full_path_tmp)
    new_name <- str_replace(old_name, "^\\D+(\\d)", "\\1")
    file_move(path = old_name, new_path = new_name)
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "SST.x.nc$|SST.NRT.x.nc$", recurse = TRUE)
    nc_files_tmp <- basename(nc_full_path_tmp)
    files_remove <- dir_ls(path = dir_input, regexp = "OC.x.nc$", recurse = TRUE)
  } else {
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "OC.x.nc$", recurse = TRUE)
    old_name <- basename(nc_full_path_tmp)
    new_name <- str_replace(old_name, "^\\D+(\\d)", "\\1")
    file_move(path = old_name, new_path = new_name)
    nc_full_path_tmp <- dir_ls(path = dir_input, regexp = "OC.x.nc$", recurse = TRUE)
    nc_files_tmp <- basename(nc_full_path_tmp)
    files_remove <- dir_ls(path = dir_input, regexp = "SST.x.nc$|SST.NRT.x.nc$", recurse = TRUE)
  }
  file_delete(files_remove)
  # mover todo a la carpeta output
  dir_create(path = dir_output)
  walk(nc_full_path_tmp, ~ file_move(path = ., new_path = dir_output))
  dir_delete(path = paste0(dir_input, "nc_files"))
  setwd(dir_output)
  # crear dataframe
  files_df <- dir_ls(path = dir_output, regexp = ".nc$") %>%
    tibble(
      infile_l2bin = .,
      ofile_l2bin = str_replace(
        string = infile_l2bin,
        pattern = ".L2_LAC_OC.x.nc|.L2.SST.x.nc|.L2.OC.x.nc",
        replacement = paste0("_", var_name, "_", res_l2, "km_L3b_tmp.nc")
      ),
      ofile_l3bin = str_replace(
        string = ofile_l2bin,
        pattern = "L3b_tmp.nc",
        replacement = "L3m_tmp.nc"
      ),
      ofile_l3mapgen = str_replace(
        string = ofile_l3bin,
        pattern = "L3m_tmp.nc",
        replacement = "L3mapped.nc"
      )
    )

  rm(list = ls(pattern = "tmp"))
  # correr l2bin-l3mapgen
  cat("Corriendo wrappers de seadas...\n\n")
  Sys.sleep(1)
  # rutas temporales solo para probar la funcion, despues estaran dentro del programa
  seadas_bins <- dir_ls(path = system.file("exec", package = "imgsatEasy"))
  names(seadas_bins) <- path_file(seadas_bins)
  seadas_bins <- map(seadas_bins, ~ read_lines(.))
  # pasar a map para consistencia del código
  for (i in 1:length(seadas_bins)) {
    seadas_bins[[i]][2] <- paste0("export OCSSWROOT=${OCSSWROOT:-", dir_ocssw, "}")
  }
  # escribir los scripts
  names_bins <- paste0(".", names(seadas_bins))
  walk2(seadas_bins, names_bins, ~ write_lines(.x, file = paste0(dir_input, .y)))
  seadas_bins <- map(names_bins, ~ paste0(dir_input, .))
  # AUX
  seadas_l2bin <- function(infile, ofile) {
      flaguse <- case_when(
        var_name == "sst" ~ "LAND,HISOLZEN",
        TRUE ~ "ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,ATMWARN,HISOLZEN,NAVFAIL,FILTER,HIGLINT"
      )
      system2(command = "chmod", args = c("+x", seadas_bins[1]))
      system2(command = seadas_bins[1], args = c(infile, ofile, "day", var_name, res_l2, "off", flaguse, "0"))
    } %>% possibly(., otherwise = "Error en archivo de entrada")
  # AUX
  seadas_l3bin <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[2]))
      system2(command = seadas_bins[2], args = c(infile, ofile, var_name, "netCDF4", "off"))
    } %>% possibly(., otherwise = "Error en archivo de entrada")
  # AUX#
  seadas_l3mapgen <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[3]))
      system2(command = seadas_bins[3], args = c(infile, ofile, var_name, "netcdf4", res_l3, "smi", "area", north, south, west, east, "true", "false"))
    } %>% possibly(., otherwise = "Error en archivo de entrada")
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  cat("Corriendo l2bin...\n\n")
  tic(msg = "Duración l2bin")
  with_progress({
    p <- progressor(steps = length(files_df$infile_l2bin))
    future_walk2(files_df$infile_l2bin, files_df$ofile_l2bin, ~ {
      p()
      Sys.sleep(.2)
      seadas_l2bin(infile = .x, ofile = .y)
    })
  })
  toc()
  stopCluster(cl)
  # filtrar solo los archivos para los cuales hubo resultados
  l2binned_files <- dir_ls(path = dir_output, regexp = "_L3b_tmp.nc", recurse = TRUE)
  files_to_l3bin <- files_df %>% filter(ofile_l2bin %in% l2binned_files)
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  cat("Corriendo l3bin...\n\n")
  tic(msg = "Duración l3bin")
  with_progress({
    p <- progressor(steps = length(files_to_l3bin$ofile_l2bin))
    future_walk2(files_to_l3bin$ofile_l2bin, files_to_l3bin$ofile_l3bin, ~ {
      p()
      Sys.sleep(.2)
      seadas_l3bin(infile = .x, ofile = .y)
    })
  })
  toc()
  stopCluster(cl)
  # filtrar solo los archivos para los cuales hubo resultados
  l3binned_files <- dir_ls(path = dir_output, regexp = "_L3m_tmp.nc", recurse = TRUE)
  files_to_l3mapgen <- files_df %>% filter(ofile_l3bin %in% l3binned_files)
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  cat("Corriendo l3mapgen...\n\n")
  tic(msg = "Duración l3mapgen")
  with_progress({
    p <- progressor(steps = length(files_to_l3mapgen$ofile_l3bin))
    future_walk2(files_to_l3mapgen$ofile_l3bin, files_to_l3mapgen$ofile_l3mapgen, ~ {
      p()
      Sys.sleep(.2)
      seadas_l3mapgen(infile = .x, ofile = .y)
    })
  })
  toc()
  stopCluster(cl)
  rm(cl)
  cat(paste0("Fin de la generación de imágenes L3 de ", var_name, "\n\n"))
  ##############################################################################
  ## movimiento de archivos
  files_l2 <- dir_ls(path = dir_output, regexp = ".L2_LAC_OC.x.nc$|SST.x.nc$|SST.NRT.x.nc$|.L2.OC.x.nc$", recurse = FALSE)
  files_l3mapped <- dir_ls(path = dir_output, regexp = "L3mapped.nc$", recurse = FALSE)
  files_logfiles <- dir_ls(path = dir_output, regexp = ".txt$", recurse = FALSE)
  cat("Moviendo archivos a sus respectivos directorios...\n\n")
  if (sort_files) {
    file_list <- list(files_l2, files_l3mapped, files_logfiles)
    walk(file_list, ~ move_files(files = .))
    file_delete(c(seadas_bins[[1]], seadas_bins[[2]], seadas_bins[[3]]))
    files_remove <- dir_ls(path = dir_output, regexp = "_tmp.nc$")
    file_delete(files_remove)
  } else {
    dir_create(path = paste0(dir_output, "all_img_L2"))
    dir_create(path = paste0(dir_output, "all_img_L3"))
    dir_create(path = paste0(dir_output, "all_log_files"))
    walk(files_l2, ~ file_move(path = ., new_path = paste0(dir_output, "all_img_L2")))
    walk(files_l3mapped, ~ file_move(path = ., new_path = paste0(dir_output, "all_img_L3")))
    walk(files_logfiles, ~ file_move(path = ., new_path = paste0(dir_output, "all_log_files")))
    file_delete(c(seadas_bins[[1]], seadas_bins[[2]], seadas_bins[[3]]))
    files_remove <- dir_ls(path = dir_output, regexp = "_tmp.nc$")
    file_delete(files_remove)
  }
  setwd(dir_input)
  cat("Fin \n\n")
  toc()
}
