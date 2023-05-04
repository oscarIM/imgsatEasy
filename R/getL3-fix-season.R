#' @title getL3_fix_season
#' @description Función para obtener imágenes L3 a partir de imágenes L2 descargadas desde https://oceancolor.gsfc.nasa.gov/cgi/browse.pl
#' @param dir_ocssw directory en donde estan los binarios ocssw (seadas)
#' @param dir_input directory en donde se almacenan las imágenes L2
#' @param dir_output directory en donde se almacenaran las imágenes L3 resultantes
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param season temporalidad para la generación de L3 ("day", "week", "month", "year" por ahora)
#' @param n_cores número de núcleos a usar. Por defecto, n_cores = 1 (corrida secuencial). La parelelización es respecto de la cantidad de sub_folder procesados simultaneamente
#' @param res_l2 resolución para l2bin. Por defecto, res = "1" (H: 0.5km, Q: 250m, HQ: 100m, HH: 50m, 1: 1.1km, 2: 2.3km, 4: 4.6km, 9: 9.2km, 18: 18.5km, 36: 36km, 1D: 1 degree, HD: 0.5 degree, QD: 0.25 degree)
#' @param res_l3 resolución para l3mapgen. Por defecto, res = "1km" (36km: 1080 x 540, 18km: 2160 x 1080, 9km: 4320 x 2160, 4km: 8640 x 4320, 2km: 17280 x 8640, 1km: 34560 x 17280, hkm: 69120 x 34560, qkm: 138240 x 69120, smi: 4096 x 2048, smi4: 8192 x 4096, land: 8640 x 4320)
#' @param north latitud norte para la generación de las imágenes L3
#' @param south latitud sur para la generación de las imágenes L3
#' @param west latitud oeste para la generación de las imágenes L3
#' @param east latitud este para la generación de las imágenes L3
#' @param need_extract mantener sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @param sort_files crearr sistema de archivos año/mes? (TRUE/FALSE).Por defecto, FALSE
#' @param area_weighting area_weighting
#' @param fudge ??fusionador
#' @return imágenes L3
#' @importFrom fs dir_ls dir_create file_move dir_delete file_delete path_file file_copy
#' @importFrom readr read_lines write_lines
#' @importFrom dplyr distinct pull filter
#' @importFrom lubridate as_date year month isoweek day
#' @importFrom purrr walk walk2
#' @importFrom stringr str_remove str_detect str_replace
#' @importFrom tibble tibble
#' @importFrom furrr future_walk future_walk2
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom utils untar
#' @importFrom tictoc tic toc
#' @export getL3_fix_season
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input_tar"
#' dir_output <- "/dir/to/output"
#' dir_ocssw <- "/dir/to/ocssw_binaries"
#' var_name <- "sst"
#' season <- "day"
#' n_cores <- 6
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' north <- -35
#' south <- -40
#' west <- -75
#' east <- -70
#' need_extract_and_format <- TRUE
#' sort_files <- FALSE
#' fudge <- 2
#' area_weighting <- 0
#' getL3_fixed_season(dir_ocssw = dir_ocssw, dir_input = dir_input, dir_output = dir_output, var_name = var_name, season = season, n_cores = n_cores, res_l2 = res_l2, res_l3 = res_l3, north = north, south = south, west = west, east = east, need_extract_and_format = need_extract_and_format, sort_files = sort_files)
#' }
getL3_fix_season <- function(dir_ocssw, dir_input, dir_output, var_name, season, n_cores = 1, res_l2 = "1", res_l3 = "1Km", north, south, west, east, need_extract_and_format = TRUE, sort_files = FALSE, fudge, area_weighting) {
  current_wd <- fs::path_wd()
  oc <- c(".OC.x.nc$", ".OC.NRT.nc$", ".OC.NRT.x.nc$")
  patterns_oc <- paste(oc, collapse = "|")
  sst <- c(".SST.x.nc$", "SST.NRT.nc", ".SST.NRT.x.nc$")
  patterns_sst <- paste(sst, collapse = "|")
  patterns_l2 <- c(oc, sst)
  patterns_l2 <- paste0(patterns_l2, collapse = "|")
  tictoc::tic(msg = "Duración total análisis")
  if (need_extract_and_format) {
    setwd(dir_input)
    cat("Descomprimiendo archivos...\n\n")
    list_tar_tmp <- fs::dir_ls(regexp = "*.tar")
    ## chequar si length list_tar_tmp == 0, indica error
    ex_dir <- stringr::str_remove(list_tar_tmp, pattern = ".tar")
    if (length(list_tar_tmp) <= 1) {
      purrr::walk(list_tar_tmp, ~ untar(tarfile = .x, exdir = "nc_files"))
    } else {
      cl <- parallel::makeForkCluster(n_cores)
      future::plan(cluster, workers = cl)
      furrr::future_walk(list_tar_tmp, ~ untar(tarfile = .x, exdir = "nc_files"))
      parallel::stopCluster(cl)
      rm(cl)
    }
    name_dirs <- fs::dir_ls(recurse = TRUE, type = "directory")
    tmp_folder <- stringr::str_detect(name_dirs, pattern = "requested_files")
    input_folder <- name_dirs[tmp_folder]
    setwd(input_folder)
    Sys.sleep(1)
    cat("Formateando archivos...\n\n")
    if (var_name == "sst") {
      nc_full_path_tmp <- fs::dir_ls(path = dir_input, regexp = patterns_sst, recurse = TRUE)
      old_name <- basename(nc_full_path_tmp)
      new_name <- stringr::str_replace(old_name, "^\\D+(\\d)", "\\1")
      fs::file_move(path = old_name, new_path = new_name)
      nc_full_path_tmp <- fs::dir_ls(path = dir_input, regexp = patterns_sst, recurse = TRUE)
      nc_files_tmp <- basename(nc_full_path_tmp)
      files_remove <- fs::dir_ls(path = dir_input, regexp = patterns_oc, recurse = TRUE)
    } else {
      nc_full_path_tmp <- fs::dir_ls(path = dir_input, regexp = patterns_oc, recurse = TRUE)
      old_name <- basename(nc_full_path_tmp)
      new_name <- stringr::str_replace(old_name, "^\\D+(\\d)", "\\1")
      file_move(path = old_name, new_path = new_name)
      nc_full_path_tmp <- fs::dir_ls(path = dir_input, regexp = patterns_oc, recurse = TRUE)
      nc_files_tmp <- basename(nc_full_path_tmp)
      files_remove <- fs::dir_ls(path = dir_input, regexp = patterns_sst, recurse = TRUE)
    }
    fs::file_delete(files_remove)
    fs::dir_create(path = dir_output)
    purrr::walk(nc_full_path_tmp, ~ fs::file_move(path = ., new_path = dir_output))
    fs::dir_delete(path = paste0(dir_input, "/nc_files"))
  } else {
    setwd(dir_input)
    nc_full_path_tmp <- fs::dir_ls(path = dir_input, regexp = patterns_l2, recurse = TRUE)
    Sys.sleep(0.1)
    cat("Moviendo archivos L2 formateados..\n\n")
    fs::dir_create(path = dir_output)
    purrr::walk(nc_full_path_tmp, ~ fs::file_copy(path = ., new_path = dir_output, overwrite = TRUE))
  }
  setwd(dir_output)
  # crear infile_list
  files_df <- fs::dir_ls(path = dir_output, regexp = ".nc$") %>%
    tibble::tibble(
      infile_l2bin = .
    ) %>%
    dplyr::mutate(
      date = dplyr::case_when(
        var_name == "sst" ~ as.Date(fs::path_file(infile_l2bin), format = "%Y%m%d"),
        TRUE ~ as.Date(fs::path_file(infile_l2bin), format = "%Y%m%d")
      ),
      ano = lubridate::year(date),
      month = lubridate::month(date),
      week = lubridate::isoweek(date),
      day = lubridate::day(date)
    )

  if (season == "year") {
    files_df_list <- files_df %>%
      dplyr::group_by(ano) %>%
      dplyr::group_split() %>%
      setNames(purrr::map(., ~ unique(.[["ano"]])))
  }
  if (season == "month") {
    files_df_list <- files_df %>%
      dplyr::group_by(ano, month) %>%
      dplyr::group_split() %>%
      setNames(purrr::map(., ~ paste0(unique(.[["ano"]]), "_", unique(.[["month"]]))))
  }
  if (season == "week") {
    files_df_list <- files_df %>%
      dplyr::group_by(ano, month, week) %>%
      dplyr::group_split() %>%
      setNames(purrr::map(., ~ paste0(unique(.[["ano"]]), "_", unique(.[["month"]]), "_w", unique(.[["week"]]))))
  }
  if (season == "day") {
    files_df_list <- files_df %>%
      dplyr::group_by(ano, month, day) %>%
      dplyr::group_split() %>%
      setNames(purrr::map(., ~ paste0(unique(.[["ano"]]), "_", unique(.[["month"]]), "_", unique(.[["day"]]))))
  }
  #### generar y exportarinfiles por season####
  file <- paste0(names(files_df_list), "_infile.txt")
  purrr::walk2(files_df_list, file, ~ cat(.x$infile_l2bin, file = .y, sep = "\n"))
  outfile_l2bin <- paste0(names(files_df_list), "_", var_name, "_", res_l2, "km_L3b_tmp.nc")
  rm(list = ls(pattern = "tmp"))
  # correr l2bin-l3mapgen

  Sys.sleep(1)
  # rutas temporales solo para probar la funcion, despues estaran dentro del programa
  if (var_name == "sst") {
    seadas_bins <- fs::dir_ls(path = system.file("exec", package = "imgsatEasy"))[-1]
  } else {
    seadas_bins <- fs::dir_ls(path = system.file("exec", package = "imgsatEasy"))[-2]
  }
  names(seadas_bins) <- fs::path_file(seadas_bins)
  seadas_bins <- purrr::map(seadas_bins, ~ readr::read_lines(.))
  # pasar a map para consistencia del código
  for (i in 1:length(seadas_bins)) {
    seadas_bins[[i]][2] <- paste0("export OCSSWROOT=${OCSSWROOT:-", dir_ocssw, "}")
  }
  # escribir los scripts
  names_bins <- paste0(".", names(seadas_bins))
  purrr::walk2(seadas_bins, names_bins, ~ readr::write_lines(.x, file = paste0(dir_input, "/", .y)))
  seadas_bins <- purrr::map(names_bins, ~ paste0(dir_input, "/", .))
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
  seadas_l3mapgen <- function(infile, ofile) {
    system2(command = "chmod", args = c("+x", seadas_bins[2]))
    system2(command = seadas_bins[2], args = c(infile, ofile, var_name, "netcdf4", res_l3, "platecarree", "area", north, south, west, east, "true", "no", fudge))
  }

  ####aca agrgar un if length files >= 10. hacerlo multicore####
  cat("Corriendo wrappers de seadas: l2bin...\n\n")
  tictoc::tic(msg = "Duración l2bin")
  if(length(file) <= 10) {
    purrr::walk2(file, outfile_l2bin, ~ seadas_l2bin(infile = .x, ofile = .y), .progress = TRUE)
  } else {
    cl <- parallel::makeForkCluster(n_cores)
    future::plan(cluster, workers = cl)
    progressr::with_progress({
      p <- progressor(steps = length(file))
      furrr::future_walk2(file, outfile_l2bin, ~ {
        p()
        Sys.sleep(.2)
        seadas_l2bin(infile = .x, ofile = .y))
      }, .options = furrr_options(seed = TRUE))
    })
    parallel::stopCluster(cl)
    rm(cl)
  }
  tictoc::toc()
  # filtrar solo los archivos para los cuales hubo resultados
  l3binned_files <- fs::dir_ls(path = dir_output, regexp = "_L3b_tmp.nc$", recurse = TRUE)
  outfile_mapgen <- stringr::str_replace(
    string = l3binned_files,
    pattern = "_L3b_tmp.nc",
    replacement = "_L3mapped.nc"
  )
  tictoc::tic(msg = "Duración l3mapgen")
  if(length(file) <= 10) {
    purrr::walk2(l3binned_files, outfile_mapgen, ~ seadas_l3mapgen(infile = .x, ofile = .y), .progress = TRUE)
  } else {
    cl <- parallel::makeForkCluster(n_cores)
    future::plan(cluster, workers = cl)
    furrr::future_walk2(l3binned_files, outfile_mapgen, ~ seadas_l3mapgen(infile = .x, ofile = .y))
    parallel::stopCluster(cl)
    rm(cl)
    }
  tictoc::toc()
  cat(paste0("Fin de la generación de imágenes L3 de ", var_name, "\n\n"))
  ##############################################################################
  ## movimiento de archivos. ACA CREAR CARPETA OUTPUT Y MOVER TODO
  files_l3mapped <- fs::dir_ls(path = dir_output, regexp = "L3mapped.nc$", recurse = FALSE)
  files_l2 <- fs::dir_ls(path = dir_output, regexp = patterns_l2, recurse = FALSE)
  files_logfiles <- fs::dir_ls(path = dir_output, regexp = ".txt$", recurse = FALSE)
  fs::file_delete(c(files_l2, files_logfiles))
  cat("Moviendo archivos a sus respectivos directorios...\n\n")
  if (sort_files) {
    file_list <- list(files_l2, files_l3mapped, files_logfiles)
    purrr::walk(file_list, ~ fs::move_files(files = .))
    fs::file_delete(c(seadas_bins[[1]], seadas_bins[[2]], seadas_bins[[3]]))
    files_remove <- fs::dir_ls(path = dir_output, regexp = "_tmp.nc$")
    fs::file_delete(files_remove)
  } else {
    fs::file_move(path = files_l3mapped, new_path = dir_output)
    fs::file_delete(c(seadas_bins[[1]], seadas_bins[[2]]))
    files_remove <- fs::dir_ls(path = dir_output, regexp = "_tmp.nc$")
    fs::file_delete(files_remove)
  }
  setwd(current_wd)
  cat("Fin \n\n")
  tictoc::toc()
}
