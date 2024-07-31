#' @title get_dataframe
#' @description Función para obtener tablas de datos en parquet o csv a partir de imágenes L2descargadas desde ://oceancolor.gsfc.nasa.gov/cgi/browse.pl. Solo se pueden extraer archivos diarios, mensuales o anuales.
#' @param dir_ocssw directorio en donde estan los binarios ocssw (seadas)
#' @param dir_input directorio en donde se almacenan las imágenes L2
#' @param dir_output directorio en donde se almacenaran los archivos resultantes
#' @param format_output formato de los archivos resultantes. Toma valores "parquet" o "csv. Por defecto parquet.
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param season temporalidad para la generación de las tablas de datos ("day",  "month", "year" por ahora)
#' @param sensor String que indica de que sensor tomar datos. POr ahora esta MODIS AQUA (aqua) y MODIS TERRA (terra) o ambos ("all)
#' @param n_cores número de núcleos a usar. Por defecto, n_cores = 1 (corrida secuencial). La parelelización es respecto de la cantidad de sub_folder procesados simultaneamente
#' @param res_l2 resolución para l2bin. Por defecto, res = "1" (H: 0.5km, Q: 250m, HQ: 100m, HH: 50m, 1: 1.1km, 2: 2.3km, 4: 4.6km, 9: 9.2km, 18: 18.5km, 36: 36km, 1D: 1 degree, HD: 0.5 degree, QD: 0.25 degree)
#' @param res_l3 resolución para l3mapgen. Por defecto, res = "1km" (36km: 1080 x 540, 18km: 2160 x 1080, 9km: 4320 x 2160, 4km: 8640 x 4320, 2km: 17280 x 8640, 1km: 34560 x 17280, hkm: 69120 x 34560, qkm: 138240 x 69120, smi: 4096 x 2048, smi4: 8192 x 4096, land: 8640 x 4320)
#' @param north latitud norte para la generación de las imágenes L3
#' @param south latitud sur para la generación de las imágenes L3
#' @param west latitud oeste para la generación de las imágenes L3
#' @param east latitud este para la generación de las imágenes L3
#' @param area_weighting area_weighting
#' @param fudge ??fusionador
#' @return tabla de datos en csv o parquet
#' @importFrom arrow write_parquet
#' @importFrom dplyr across all_of filter group_by group_split mutate pull select tibble
#' @importFrom lubridate day month year
#' @importFrom future plan
#' @importFrom furrr future_walk
#' @importFrom ncdf4 nc_open ncatt_get ncvar_get
#' @importFrom parallel makeForkCluster stopCluster
#' @importFrom progressr progressor with_progress
#' @importFrom purrr map walk walk2
#' @importFrom readr write_lines write_csv
#' @importFrom rlang !! sym :=
#' @importFrom stringr str_detect str_remove str_replace str_subset
#' @importFrom tidyr separate expand_grid
#' @export get_dataframe
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input_tar"
#' dir_output <- "/dir/to/output"
#' dir_ocssw <- "/dir/to/ocssw_binaries"
#' format_output <- "parquet"
#' var_name <- "sst"
#' season <- "day"
#' n_cores <- 6
#' res_l2 <- "1"
#' res_l3 <- "1km"
#' north <- -35
#' south <- -40
#' west <- -75
#' east <- -70
#' fudge <- 2
#' area_weighting <- 0
#' sensor <- "all"
#' get_dataframe(dir_ocssw = dir_ocssw, dir_input = dir_input, dir_output = dir_output, format_ouput = format_ouput, var_name = var_name, season = season, n_cores = n_cores, res_l2 = res_l2, res_l3 = res_l3, north = north, south = south, west = west, east = east, sensor = sensor)
#' }
get_dataframe <- function(dir_ocssw, dir_input, dir_output, format_output = "parquet" , sensor, var_name, season, n_cores = 1, res_l2 = "1", res_l3 = "1Km", north, south, west, east, fudge, area_weighting = 0) {
  tic()
  current_wd <- getwd()
  patterns_oc <- paste(c(".OC.x.nc$", ".OC.NRT.nc$", ".OC.NRT.x.nc$"), collapse = "|")
  patterns_sst <- paste(c(".SST.x.nc$", ".SST.NRT.nc$", ".SST.NRT.x.nc$"), collapse = "|")
  patterns_l2 <- paste(c(patterns_oc, patterns_sst), collapse = "|")
  setwd(dir_input)
  cat("Descomprimiendo archivos...\n\n")
  list_tar_tmp <- list.files(full.names = TRUE, pattern = "*.tar")
  ## chequar si length list_tar_tmp == 0, indica error
  ex_dir <- stringr::str_remove(list_tar_tmp, pattern = ".tar")
  if (length(list_tar_tmp) <= 10) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(list_tar_tmp))
      purrr::walk(list_tar_tmp, ~ {
        p()
        Sys.sleep(.2)
        untar(tarfile = .x, exdir = "nc_files")
        })
      })
    } else {
      cl <- parallel::makeForkCluster(n_cores)
      future::plan("cluster", workers = cl)
      progressr::with_progress({
        p <- progressr::progressor(steps = length(list_tar_tmp))
        furrr::future_walk(list_tar_tmp, ~ {
          p()
          Sys.sleep(.2)
          untar(tarfile = .x, exdir = "nc_files")
      }, .options = furrr_options(seed = TRUE))
    })
      parallel::stopCluster(cl)
      rm(cl)
  }

  input_folder <- list.dirs(full.names = TRUE) %>%
    stringr::str_subset(pattern = "requested_files")
  setwd(input_folder)

  cat("Transformando archivos L2 a L3: Configurando archivos de entrada... \n\n")
  all_files_tmp <- list.files(full.names = TRUE, pattern = ".nc$") %>%
    dplyr::tibble(file = .) %>%
    tidyr::separate(col = "file",
                    into = c("tmp", "sensor", "full_time", "level","var_type"),
                    extra = "drop",
                    sep = "\\.",
                    remove = FALSE) %>%
    dplyr::select(-tmp) %>%
    dplyr::mutate(sensor = stringr::str_remove(string = sensor, pattern = "^/"))
  sensor_pattern  <- switch(sensor,
                            "aqua"  = "^AQUA",
                            "terra" = "^TERRA",
                            "all"   = "^AQUA|^TERRA",
                            default = NULL)

  if (!is.na(sensor_pattern)) {
    cat(paste0("Utilizano datos del sensor: ", sensor, "\n\n"))
    all_files_tmp <- all_files_tmp %>%
      dplyr::filter(stringr::str_detect(sensor, pattern = sensor_pattern))
  } else {
    stop("ingresar sensor: aqua, terra o all.. \n\n")
  }

  if (!dir.exists(dir_output)) {
    dir.create(dir_output)
  } else {
    cat("El directorio ya existe:", dir_output, "\n")
  }

  selected_files_tmp <- all_files_tmp %>%
    dplyr::filter(var_type == ifelse(var_name == "sst", "SST", "OC")) %>%
    dplyr::pull(file)
  destination_files_tmp <- file.path(dir_output, basename(selected_files_tmp))
  remove_files_tmp <- all_files_tmp %>%
    dplyr::filter(var_type == ifelse(var_name == "sst", "OC", "SST")) %>%
    dplyr::pull(file)
  invisible(file.rename(selected_files_tmp, destination_files_tmp))
  #limpiar un poco
  unlink(x = paste0(dir_input, "/nc_files"), recursive = TRUE)
  #unlink(x = remove_files_tmp)
  rm(list = ls(pattern = "tmp"))
  gc()
  #Siguiente fase
  setwd(dir_output)
  # crear infile_list
  files_df <- list.files(path = dir_output, pattern = ".nc$", full.names = TRUE) %>%
    dplyr::tibble(infile_l2bin = .) %>%
    dplyr::mutate(tmp_col = basename(infile_l2bin)) %>%
    tidyr::separate(col = "tmp_col", into = c("sensor", "full_time"), sep = "\\.", extra = "drop") %>%
    dplyr::mutate(date = as.Date(full_time, format = "%Y%m%d"),
                  year = lubridate::year(date),
                  month = lubridate::month(date),
                  day = lubridate::day(date))
  group_vars <- switch(season,
                       "year"  = "year",
                       "month" = c("year", "month"),
                       "day"   = "date")

  files_df_list <- files_df %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(group_vars))) %>%
    dplyr::group_split() %>%
    setNames(map(., ~ {
      if (season == "year") {
        unique(.[["year"]])
      } else if (season == "month") {
        paste0(unique(.[["year"]]), "_", unique(.[["month"]]))
      } else if (season == "day") {
        paste0(unique(.[["year"]]), "_", unique(.[["month"]]), "_", unique(.[["day"]]))
      }
    }))
  #### generar y exportarinfiles por season####
  files <- paste0(names(files_df_list), "_infile.txt")
  purrr::walk2(files_df_list, files, ~ cat(.x$infile_l2bin, file = .y, sep = "\n"))
  outfile_l2bin <- paste0(names(files_df_list), "_", var_name, "_", res_l2, "km_L3b_tmp.nc")
  # correr l2bin-l3mapgen

  Sys.sleep(1)
  # rutas temporales solo para probar la funcion, despues estaran dentro del programa
  seadas_bins_path <- system.file("exec", package = "imgsatEasy")
  if (var_name == "sst") {
    seadas_bins <- list.files(path = seadas_bins_path, full.names = TRUE)[-1]
  } else {
    seadas_bins <- seadas_bins <- list.files(path = seadas_bins_path, full.names = TRUE)[-2]
  }
  names(seadas_bins) <- basename(seadas_bins)
  seadas_bins <- purrr::map(seadas_bins, ~ readr::read_lines(.))
  # pasar a map para consistencia del código
  for (i in 1:length(seadas_bins)) {
    seadas_bins[[i]][2] <- paste0("export OCSSWROOT=${OCSSWROOT:-", dir_ocssw, "}")
  }
  # escribir los scripts
  names_bins <- paste0(".", names(seadas_bins))
  purrr::walk2(seadas_bins, names_bins, ~ readr::write_lines(.x, file = paste0(dir_output, "/", .y)))
  seadas_bins <- purrr::map(names_bins, ~ paste0(dir_output, "/", .))
  # AUX#
  if (var_name == "sst") {
    seadas_l2bin <- function(infile, ofile) {
      system2(command = "chmod", args = c("+x", seadas_bins[1]))
      system2(command = seadas_bins[1], args = c(infile, ofile, "regional", var_name, res_l2, "off", "LAND,HISOLZEN", 2, north, south, east, west, area_weighting, "qual_sst", "SST"))
    } } else {
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
  cat("Transformando archivos L2 a L3: Corriendo l2bin...\n\n")
  if (length(files) <= 10) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(files))
      purrr::walk2(files, outfile_l2bin, ~ {
        p()
        Sys.sleep(.2)
        seadas_l2bin(infile = .x, ofile = .y)
      })
    })
  } else {
    cl <- parallel::makeForkCluster(n_cores)
    future::plan("cluster", workers = cl)
    progressr::with_progress({
      p <- progressor(steps = length(files))
      furrr::future_walk2(files, outfile_l2bin, ~ {
        p()
        Sys.sleep(.2)
        seadas_l2bin(infile = .x, ofile = .y)
      }, .options = furrr_options(seed = TRUE))
    })
    parallel::stopCluster(cl)
    rm(cl)
  }
  # filtrar solo los archivos para los cuales hubo resultados
  l3binned_files <- list.files(path = dir_output, pattern = "_L3b_tmp.nc$", full.names = TRUE)
  outfile_mapgen <- stringr::str_replace(
    string = l3binned_files,
    pattern = "_L3b_tmp.nc",
    replacement = "_L3mapped.nc"
  )
  cat("Transformando archivos L2 a L3: Corriendo l3mapgen...\n\n")
  if (length(l3binned_files) <= 10) {
      progressr::with_progress({
        p <- progressr::progressor(steps = length(l3binned_files))
        purrr::walk2(l3binned_files, outfile_mapgen, ~ {
          p()
          Sys.sleep(.2)
          seadas_l3mapgen(infile = .x, ofile = .y)
          })
        }) } else {
          cl <- parallel::makeForkCluster(n_cores)
          future::plan("cluster", workers = cl)
          progressr::with_progress({
            p <- progressor(steps = length(l3binned_files))
            furrr::future_walk2(l3binned_files, outfile_mapgen, ~ {
              p()
              Sys.sleep(.2)
              seadas_l3mapgen(infile = .x, ofile = .y)
              }, .options = furrr::furrr_options(seed = TRUE))
            })
          parallel::stopCluster(cl)
          rm(cl)
    }
  cat(paste0("Fin de la generación de imágenes L3 de ", var_name, "\n\n"))
  ##############################################################################
  ## movimiento de archivos. ACA CREAR CARPETA OUTPUT Y MOVER TODO
  #files to delete
  pattern_del <- paste(c(patterns_l2, ".txt$","_L3b_tmp.nc$"), collapse = "|")
  files_del <- list.files(path = dir_output, pattern = pattern_del, full.names = TRUE, recursive = FALSE)
  files_l3mapped <- list.files(path = dir_output, pattern = "L3mapped.nc$", full.names = TRUE, recursive = FALSE)
  unlink(c(files_del, seadas_bins[[1]], seadas_bins[[2]]))

  cat(paste0("Iniciando generación de archivos de ", var_name ," en formato ", format_output, "\n\n"))
  if (length(files_l3mapped) <= 10) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(files_l3mapped))
      purrr::walk(files_l3mapped, function(file) {
        p()
        Sys.sleep(.2)
        df <- nc_to_table(file, var_name)
        write_table(df, file, format_output)
        })
      }) } else {
        cl <- parallel::makeForkCluster(n_cores)
        future::plan("cluster", workers = cl)
        progressr::with_progress({
          p <- progressor(steps = length(files_l3mapped))
          furrr::future_walk(files_l3mapped, ~ {
            p()
            Sys.sleep(.2)
            df <- nc_to_table(.x, var_name)
            write_table(df, .x, format_output)
            }, .options = furrr::furrr_options(seed = TRUE))
          })
        parallel::stopCluster(cl)
        rm(cl)
  }

  unlink(files_l3mapped)
  setwd(current_wd)
  toc()
  cat("Fin \n\n")
}
