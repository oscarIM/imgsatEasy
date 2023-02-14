###function to sort file in multiTime analisis)
#directorio de los datos brutos fg 
pacman::p_load(future, furrr, fs, parallel, stringr, dplyr, lubridate, purrr, readr, tictoc)

#' @title getL3_MP
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
#' @param fudge factor "relleno"
#' @param area_weighting factor de ponderado, dejar en 0 
#' @return imágenes L3
#' @importFrom fs dir_ls path_wd path_file dir_create dir_delete
#' @importFrom readr read_lines write_lines
#' @importFrom dplyr filter rename mutate case_when group_by group_split
#' @importFrom lubridate as_date year month
#' @importFrom purrr map walk2 future_walk2
#' @importFrom stringr str_detect str_replace
#' @importFrom tibble tibble
#' @importFrom furrr future_walk future_walk2
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom utils untar
#' @importFrom tictoc tic toc
#' @importFrom progressr with_progress progressor
#' @export getL3_MP
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/proyecto_packard/data_raw"
#' dir_output <- "/media/evolecolab/PortableSSD/Proyecto_Packard/chlor_a/part_2"
#' dir_ocssw <- "/home/evolecolab/seadas/ocssw"
#' season <- "day"
#' var_name <- "chlor_a"
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' flaguse <- "ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,ATMWARN,HISOLZEN,NAVFAIL,FILTER,HIGLINT"
#' n_cores <- 12
#' north <- -18.3501
#' south <- -59.8527
#' west <- -84.8414
#' east <- -65.7267
#' fudge <- 2
#' area_weighting <- 0
#' getL3_MP(dir_input = dir_input, dir_output = dir_output,dir_ocssw = dir_ocssw, season = season, var_name = var_name, res_l2 = res_l2, res_l3 = res_l3, flaguse = flaguse, n_cores = n_cores, north = north, south = south,west = west, east = east,fudge = fudge, area_weighting = area_weighting)
#' }
getL3_MP <- function(dir_input, dir_output, dir_ocssw, season, var_name, res_l2, res_l3, flaguse,n_cores, north,
                     south, west, east, fudge, area_weighting = 0) {
  tic("Duración total")
  setwd(dir_input)
  oc <- c(".OC.x.nc$", ".OC.NRT.nc$", ".OC.NRT.x.nc$")
  patterns_oc <- paste(oc, collapse = "|")
  sst <- c(".SST.x.nc$", ".SST.NRT.x.nc$")
  patterns_sst <- paste(sst, collapse = "|")
  patterns_l2 <- c(oc, sst)
  patterns_l2 <- paste0(patterns_l2, collapse = "|")
  ###obtener lista de archivos en cada .tar 
  list_tar_tmp <- dir_ls(path = dir_input, regexp = "*.tar") %>% as.list()
  get_df_from_tar <- function(tar_file) {
    file = path_file(tar_file)
    if (var_name == "sst") {
      all_compress_files <- untar(tarfile = tar_file, list = TRUE) %>% 
        as.data.frame() %>%
        rename("compress_files" = 1) %>% 
        filter(str_detect(string = compress_files, pattern = patterns_sst)) %>%
        mutate(parent_file = file )  
    } else {
      all_compress_files <- untar(tarfile = tar_file, list = TRUE) %>% 
        as.data.frame() %>%
        rename("compress_files" = 1) %>% 
        filter(str_detect(string = compress_files, pattern = patterns_oc)) %>%
        mutate(parent_file = file )
    }
  }
list_files_to_extract <- map(list_tar_tmp, ~get_df_from_tar(.))
#quizas paralelizar
cat("Extrayendo archivos desde archivadores .tar...\n\n")
tic("\n\n Duración extraccion")
#walk2(list_tar_tmp, list_files_to_extract, ~untar(tarfile = .x, files =.y$compress_files, exdir = "nc_files"))
  if (length(list_files_to_extract) <= 1) {
 walk2(list_tar_tmp, list_files_to_extract, ~untar(tarfile = .x, files =.y$compress_files, exdir = "nc_files"))
  } else {
    cl <- parallel::makeForkCluster(n_cores)
    plan(cluster, workers = cl)
    future_walk2(list_tar_tmp, list_files_to_extract, ~untar(tarfile = .x, files =.y$compress_files, exdir = "nc_files"), .progress = TRUE)
    parallel::stopCluster(cl)
    rm(cl)
    }
toc() 
 folder_data <- dir_ls(recurse = TRUE, type = "directory", regexp = "requested_files" )
  setwd(folder_data)
  folder_data <- path_wd()
  nc_full_path_tmp <- dir_ls(path = folder_data, regexp = ".nc$")
  old_name_tmp <- basename(nc_full_path_tmp)
  new_name_tmp <- str_replace(old_name_tmp, "^\\D+(\\d)", "\\1")
  file_move(path = old_name_tmp, new_path = new_name_tmp)
  rm(list = ls(pattern = "_tmp$"))
   ######## crear df para tener claro las fechas y crear los directorios
  files_df <- dir_ls(path = folder_data, regexp = ".nc$", recurse = FALSE) %>%
    tibble(
      infile_l2bin = .
    ) %>%
    mutate(
      date = case_when(
        var_name == "sst" ~ as.Date(path_file(infile_l2bin), format = "%Y%m%d"),
        TRUE ~ as.Date(path_file(infile_l2bin), format = "%Y%m%d")
      ),
      ano = year(date),
      month = month(date, label = TRUE, abbr = FALSE),
      day = day(date),
      month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE)),
      folder_name = case_when(
        season == "month" ~ paste0(dir_output, "/", var_name, "/", ano, "_", month_name),
        season == "year" ~ paste0(dir_output, "/", var_name, "/", ano)
      )
    )
  ##### agrupar por season
  if (season == "year") {
    files_df_list <- files_df %>%
      group_by(ano) %>%
      group_split() %>%
      setNames(map(., ~ unique(.[["ano"]])))
  }
  if (season == "month") {
    files_df_list <- files_df %>%
      group_by(ano, month_name) %>%
      group_split() %>%
      setNames(map(., ~ paste0(unique(.[["ano"]]), "_", unique(.[["month_name"]]))))
  }
  if (season == "day") {
    files_df_list <- files_df %>%
      group_by(ano, month_name, day) %>%
      group_split() %>%
      setNames(map(., ~ paste0(unique(.[["ano"]]), "_", unique(.[["month_name"]]),"_",unique(.[["day"]]))))
  }
  # crear folder para meter los infiles, l2binned y l3mapgen
  folder_infile <- dir_create(paste0(dir_output, "/", "l2_infiles")) %>% fs_path()
  folder_l2binned <- dir_create(paste0(dir_output, "/", "l2binned_files")) %>% fs_path()
  folder_l3mapped <- dir_create(paste0(dir_output, "/", "l3mapped_files")) %>% fs_path()
  folder_seadas_script <- dir_create(paste0(dir_output, "/", "seadas_script")) %>% fs_path()
  ## crear nombre de las salidas
  names_infiles <- paste0("infile_", names(files_df_list), ".txt")
  names_infiles <- paste0(folder_infile, "/", names_infiles)
  walk2(files_df_list, names_infiles, ~ cat(.x$infile_l2bin, file = .y, sep = "\n"))
  ## crear nombre de las salidas
  outfile_l2bin <- paste0(names(files_df_list), "_", var_name, "_", res_l2, "km_L3b_tmp.nc")
  outfile_l2bin <- paste0(folder_l2binned, "/", outfile_l2bin)
  ### preparación binarios
  # rutas temporales solo para probar la función, después estarán dentro del programa
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
  walk2(seadas_bins, names_bins, ~ write_lines(.x, file = paste0(folder_seadas_script, "/", .y)))
  seadas_bins <- map(names_bins, ~ paste0(folder_seadas_script, "/", .))
  # AUX#
  if (var_name == "sst") {
    seadas_l2bin <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[1]))
      system2(command = seadas_bins[1], args = c(infile, ofile, "regional", var_name, res_l2, "off", flaguse, 0, north, south, east, west, area_weighting, "qual_sst", "SST"))
    }
  } else {
    seadas_l2bin <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[1]))
      system2(command = seadas_bins[1], args = c(infile, ofile, "regional", var_name, res_l2, "off", flaguse, 0, north, south, east, west, area_weighting))
    }
  }
  # AUX#
  seadas_l3mapgen <- function(infile, ofile) {{ system2(command = "chmod", args = c("+x", seadas_bins[2]))
    system2(command = seadas_bins[2], args = c(infile, ofile, var_name, "netcdf4", res_l3, "platecarree", "area", north, south, west, east, "true", "no", fudge)) }}
  cat("Corriendo wrappers de seadas: l2bin...\n\n")
  tic(msg = "\n\n Duración l2bin")
  cl <- parallel::makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  future_walk2(names_infiles, outfile_l2bin, ~ seadas_l2bin(infile = .x, ofile = .y), .progress = TRUE)
  toc()
  parallel::stopCluster(cl)
  rm(cl)
  l2binned_files <- dir_ls(path = folder_l2binned, regexp = "_L3b_tmp.nc$", recurse = FALSE)
  #####config out for mapped files
  outfile_mapped <- str_replace(string = l2binned_files, pattern =  "_L3b_tmp.nc", replacement = "_L3mapped.nc" )
  outfile_mapped <- path_file(outfile_mapped)
  outfile_mapped <- paste0(folder_l3mapped, "/", outfile_mapped)
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  cat("Corriendo wrappers de seadas: l3mapgen...\n\n")
  tic(msg = "\n\n Duración l3mapgen")
  future_walk2(l2binned_files, outfile_mapped, ~ seadas_l3mapgen(infile = .x, ofile = .y), .progress = TRUE)
  toc()
  stopCluster(cl)
  #renombrar a algo como arhcivolos l2 brutos y moverlos o barrar..quizas hacer un if
  folder_delete <- paste0(dir_input, "/", "nc_files")
  dir_delete(folder_delete)
  cat(paste0("Fin de la generación de imágenes L3 de ", var_name, "\n\n"))
  toc()
}
