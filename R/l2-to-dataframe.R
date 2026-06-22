#' @title l2_to_dataframe
#' @description Procesa archivos L2 (SeaDAS) y los transforma a L3 y luego a tabla (e.g., parquet)
#' @param dir_ocssw Directorio OCSSW (SeaDAS)
#' @param dir_input Directorio con archivos L2 (o .tar si data_compress = TRUE)
#' @param dir_output Directorio de salida para L3 y archivos finales
#' @param format_output Formato de salida para las tablas ('parquet' por defecto)
#' @param sensor Sensor MODIS/Sentinel ('aqua', 'terra', etc.)
#' @param var_name Nombre del producto (e.g. 'chlor_a')
#' @param season Periodo de agregación ('year', 'month', 'day')
#' @param n_cores Núm. de núcleos para procesamiento paralelo
#' @param res_l2 Resolución de binning (default '1')
#' @param res_l3 Resolución de mapeo ('1Km')
#' @param north,south,west,east Límites espaciales
#' @param fudge Valor fudge para mapgen
#' @param area_weighting Si se aplica ponderación por área
#' @param data_compress Si TRUE, espera archivos .tar; si FALSE, archivos .nc directos
#' @param flaguse flaguse
#' @importFrom dplyr tibble mutate filter pull select group_by summarise case_when
#' @importFrom tidyr separate
#' @importFrom stringr str_remove str_detect str_subset str_replace
#' @importFrom purrr walk walk2 map_chr map keep
#' @importFrom readr read_lines write_lines
#' @importFrom progressr with_progress progressor
#' @importFrom lubridate year month day
#' @importFrom future plan
#' @importFrom furrr future_walk future_walk2 furrr_options
#' @importFrom utils untar
#' @importFrom glue glue glue_collapse
#' @export l2_to_dataframe

l2_to_dataframe <- function(
  dir_ocssw,
  dir_input,
  dir_output,
  format_output = "parquet",
  sensor,
  var_name,
  season,
  n_cores = 1,
  res_l2 = "1",
  res_l3 = "1Km",
  north, south, west, east,
  fudge,
  area_weighting = 0,
  data_compress = TRUE,
  flaguse
) {
  # ---- 1. l2_to_dataframe: Inicialización ----------------------------------

  tic()
  current_wd <- getwd()
  on.exit(setwd(current_wd), add = TRUE)

  ## 1.1. Normalizar rutas a absolutas ----------------------------------------
  # FIX: convierte todas las rutas a absolutas ANTES de cualquier setwd(),
  # evitando errores cuando el usuario pasa rutas relativas.
  dir_input <- normalizePath(dir_input, mustWork = FALSE)
  if (!dir.exists(dir_output)) {
    dir.create(dir_output, recursive = TRUE, showWarnings = FALSE)
  }
  dir_output <- normalizePath(dir_output, mustWork = FALSE)
  dir_ocssw <- normalizePath(dir_ocssw, mustWork = FALSE)

  ## 1.2. Patrones de archivos L2 ----------------------------------------------
  patterns_oc <- glue::glue_collapse(c(".OC.x.nc$", ".OC.nc$"), sep = "|")
  patterns_sst <- glue::glue_collapse(c(".SST.x.nc$", ".SST.nc$"), sep = "|")
  patterns_l2 <- glue::glue("{patterns_oc}|{patterns_sst}")

  setwd(dir_input)

  # ---- 2. l2_to_dataframe: Descompresión de archivos -----------------------

  if (data_compress) {
    ## 2.1. Descomprimir .tar (serial o paralelo según cantidad) ---------------
    # NOTA: la descompresión maneja su propio cluster temporal porque el wd
    # cambiará después (a input_folder). makeForkCluster hereda el wd del
    # momento en que se crea, así que no puede compartir cluster con etapas
    # posteriores que corren en otro directorio.
    cat("Descomprimiendo archivos...\n\n")
    list_tar_tmp <- list.files(full.names = TRUE, pattern = "*.tar")

    if (length(list_tar_tmp) <= 10 || n_cores <= 1) {
      progressr::with_progress({
        p <- progressr::progressor(steps = length(list_tar_tmp))
        purrr::walk(list_tar_tmp, ~ {
          Sys.sleep(.2)
          untar(tarfile = .x, exdir = "nc_files")
          p()
        })
      })
    } else {
      cl_tar <- parallel::makeForkCluster(n_cores)
      future::plan("cluster", workers = cl_tar)
      progressr::with_progress({
        p <- progressr::progressor(steps = length(list_tar_tmp))
        furrr::future_walk(list_tar_tmp, ~ {
          Sys.sleep(.2)
          untar(tarfile = .x, exdir = "nc_files")
          p()
        }, .options = furrr_options(seed = TRUE))
      })
      parallel::stopCluster(cl_tar)
      future::plan("sequential")
    }

    ## 2.2. Navegar al subdirectorio de archivos extraídos ---------------------
    input_folder <- list.dirs(full.names = TRUE) %>%
      stringr::str_subset(pattern = "requested_files")
    setwd(input_folder)
  } else {
    cat("Procesando archivos directamente sin descompresión...\n\n")
  }

  # ---- 2.3. Crear cluster compartido para etapas 6, 7, 8 -------------------
  # Se crea DESPUÉS de todos los setwd(), para que los workers hereden
  # el directorio de trabajo correcto (donde están los _infile.txt y .nc).
  # Un solo cluster se reutiliza en l2bin, l3mapgen y conversión a tabla.
  cl <- NULL
  if (n_cores > 1) {
    cl <- parallel::makeForkCluster(n_cores)
    future::plan("cluster", workers = cl)
    on.exit(
      {
        parallel::stopCluster(cl)
        future::plan("sequential")
      },
      add = TRUE
    )
  }

  # ---- 3. l2_to_dataframe: Preparación de inputs L2 -----------------------

  cat("Transformando archivos L2 a L3: Configurando archivos de entrada...\n\n")

  ## 3.1. Construir tabla de archivos con metadatos de fecha y sensor ---------
  files_df <- list.files(full.names = TRUE, pattern = ".nc$") %>%
    dplyr::tibble(file = .) %>%
    tidyr::separate(
      col = "file",
      into = c("tmp", "sensor", "full_time", "level", "var_type"),
      extra = "drop", sep = "\\.", remove = FALSE
    ) %>%
    dplyr::select(-tmp) %>%
    dplyr::mutate(sensor = stringr::str_remove(string = sensor, pattern = "^/"))

  ## 3.2. Filtrar por sensor --------------------------------------------------
  sensor_pattern <- switch(sensor,
    "aqua"       = "^AQUA|^AQUA_",
    "terra"      = "^TERRA|^TERRA_",
    "modis_aq"   = "^AQUA|^TERRA",
    "sentinel3A" = "^S3A_",
    "sentinel3B" = "^S3B_",
    "sentinelAB" = "^S3A_|^S3B_",
    NULL
  )

  if (!is.null(sensor_pattern)) {
    cat(paste0("Utilizando datos del sensor: ", sensor, "\n\n"))
    files_df <- files_df %>%
      dplyr::filter(stringr::str_detect(sensor, pattern = sensor_pattern))
  } else {
    stop("Ingresar sensor válido: aqua, terra, modis_aq, sentinel3A, sentinel3B, sentinelAB\n\n")
  }

  ## 3.3. Agregar columnas de fecha, periodo y nombres de archivos de salida --
  files_df <- files_df %>%
    dplyr::mutate(
      date = as.Date(full_time, format = "%Y%m%d"),
      year = lubridate::year(date),
      month = sprintf("%02d", lubridate::month(date)),
      day = lubridate::day(date),
      season_id = dplyr::case_when(
        season == "year" ~ glue::glue("{year}"),
        season == "month" ~ glue::glue("{year}-{month}"),
        season == "day" ~ glue::glue("{year}-{month}-{day}")
      ),
      infile_txt = glue::glue("{season_id}_infile.txt"),
      outfile_l2bin = glue::glue("{season_id}_{var_name}_{res_l2}_km_L3b_tmp.nc"),
      outfile_mapgen = stringr::str_replace(outfile_l2bin, "_L3b_tmp.nc", "_L3mapped.nc")
    )

  ## 3.4. Agrupar archivos por periodo de agregación -------------------------
  files <- files_df %>%
    dplyr::group_by(infile_txt, outfile_l2bin, outfile_mapgen) %>%
    dplyr::summarise(file_list = list(file), .groups = "drop")

  ## 3.5. Escribir archivos de texto con las listas de inputs ----------------
  purrr::walk2(files$file_list, files$infile_txt, ~ cat(.x, file = .y, sep = "\n"))

  # ---- 4. l2_to_dataframe: Configuración de scripts SeaDAS ----------------

  ## 4.1. Localizar y leer scripts de SeaDAS ----------------------------------
  seadas_bins_path <- system.file("exec", package = "imgsatEasy")
  seadas_bins <- list.files(path = seadas_bins_path, full.names = TRUE)

  if (var_name == "sst") {
    seadas_bins <- seadas_bins[-1]
  } else {
    seadas_bins <- seadas_bins[-2]
  }

  names(seadas_bins) <- basename(seadas_bins)
  seadas_bins <- purrr::map(seadas_bins, ~ readr::read_lines(.))

  ## 4.2. Inyectar ruta OCSSW en los scripts ---------------------------------
  for (i in seq_along(seadas_bins)) {
    seadas_bins[[i]][2] <- paste0("export OCSSWROOT=${OCSSWROOT:-", dir_ocssw, "}")
  }

  ## 4.3. Escribir scripts en dir_input (ruta ya normalizada → siempre válida)
  # FIX: se usa dir_input (ruta absoluta) en lugar de una ruta relativa que
  # podría romperse tras llamadas a setwd() anteriores.
  names_bins <- paste0(".", names(seadas_bins))
  purrr::walk2(
    seadas_bins, names_bins,
    ~ readr::write_lines(.x, file = file.path(dir_input, .y))
  )
  seadas_bins <- purrr::map(names_bins, ~ file.path(dir_input, .x))

  # ---- 5. l2_to_dataframe: Definición de funciones SeaDAS -----------------

  ## 5.1. Función l2bin (OC o SST) -------------------------------------------
  if (var_name == "sst") {
    seadas_l2bin <- function(infile, ofile) {
      system2("chmod", c("+x", seadas_bins[[1]]))
      system2(seadas_bins[[1]], c(
        infile, ofile, "regional", var_name, res_l2, "off",
        "LAND,HISOLZEN", 2,
        north, south, east, west,
        area_weighting, "qual_sst", "SST"
      ))
    }
  } else {
    seadas_l2bin <- function(infile, ofile) {
      system2("chmod", c("+x", seadas_bins[[1]]))
      system2(seadas_bins[[1]], c(
        infile, ofile, "regional", var_name, res_l2, "off", flaguse, 2, north, south, east, west, area_weighting
      ))
    }
  }

  ## 5.2. Función l3mapgen ---------------------------------------------------
  seadas_l3mapgen <- function(infile, ofile) {
    system2("chmod", c("+x", seadas_bins[[2]]))
    system2(seadas_bins[[2]], c(
      infile, ofile, var_name, "netcdf4",
      res_l3, "platecarree", "bin",
      north, south, west, east,
      "true", "no", fudge
    ))
  }

  # ---- 6. l2_to_dataframe: Ejecución l2bin --------------------------------

  cat("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("🚀 Transformando archivos L2 a L3\n")
  cat("➡️  Corriendo l2bin...\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

  infiles <- files$infile_txt
  outfiles <- files$outfile_l2bin

  if (length(infiles) <= 10 || is.null(cl)) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(infiles))
      purrr::walk2(infiles, outfiles, ~ {
        Sys.sleep(.2)
        seadas_l2bin(.x, .y)
        p()
      })
    })
  } else {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(infiles))
      furrr::future_walk2(infiles, outfiles, ~ {
        Sys.sleep(.2)
        seadas_l2bin(.x, .y)
        p()
      }, .options = furrr_options(seed = TRUE))
    })
  }

  # ---- 7. l2_to_dataframe: Ejecución l3mapgen -----------------------------

  cat("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("🚀 Transformando archivos L2 a L3\n")
  cat("➡️  Corriendo l3mapgen...\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

  l3binned_files <- files$outfile_l2bin
  outfile_mapgen <- files$outfile_mapgen

  if (length(l3binned_files) <= 10 || is.null(cl)) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(l3binned_files))
      purrr::walk2(l3binned_files, outfile_mapgen, ~ {
        Sys.sleep(.2)
        seadas_l3mapgen(.x, .y)
        p()
      })
    })
  } else {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(l3binned_files))
      furrr::future_walk2(l3binned_files, outfile_mapgen, ~ {
        Sys.sleep(.2)
        seadas_l3mapgen(.x, .y)
        p()
      }, .options = furrr_options(seed = TRUE))
    })
  }

  # ---- 8. l2_to_dataframe: Conversión a tabla (parquet/csv) ---------------

  files_l3mapped <- list.files(path = ".", pattern = "_L3mapped.nc$", full.names = TRUE)

  cat(glue::glue("
━━━━━━━━━━━━━━━━━━━━━━━━━━━
🧩 Iniciando generación de archivos de {var_name}
💾 Formato de salida: {format_output}
━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"))

  if (length(files_l3mapped) <= 10 || is.null(cl)) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(files_l3mapped))
      purrr::walk(files_l3mapped, function(file) {
        Sys.sleep(.2)
        df <- nc_to_table(file, var_name)
        write_table(df, file, format_output)
        p()
      })
    })
  } else {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(files_l3mapped))
      furrr::future_walk(files_l3mapped, ~ {
        Sys.sleep(.2)
        df <- nc_to_table(.x, var_name)
        write_table(df, .x, format_output)
        p()
      }, .options = furrr_options(seed = TRUE))
    })
  }

  # ---- 9. l2_to_dataframe: Traslado de archivos a dir_output --------------
  ## 9.2. Mover archivos finales al directorio de salida ---------------------
  pattern_out <- glue::glue("*.csv|*.parquet")
  list_final_files <- list.files(path = ".", pattern = pattern_out, full.names = TRUE)
  invisible(file.rename(list_final_files, file.path(dir_output, basename(list_final_files))))

  # ---- 10. l2_to_dataframe: Limpieza y cierre ------------------------------
  # setwd() y stopCluster() se manejan automáticamente por on.exit()

  pattern_del <- glue::glue("*.txt|*tmp.nc|*L3mapped.nc")
  del_files <- list.files(path = ".", pattern = pattern_del, full.names = TRUE)
  if (dir.exists(file.path(dir_input, "nc_files"))) {
    del_folder <- file.path(dir_input, "nc_files")
    unlink(del_folder, recursive = TRUE)
  }

  unlink(del_files)
  unlink(seadas_bins)
  toc()

  cat("
━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Proceso finalizado
━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
}
