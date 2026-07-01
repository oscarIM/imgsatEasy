# De L1A a al L2 de MODIS-Aqua listo para entrar a l2_to_dataframe().
#
# Pipeline por granulo:
#   1. Descomprimir L1A (bzip2 silencioso)
#   2. Actitud/efemerides + geolocalizacion (modis_atteph + modis_GEO -> GEO)
#   3. Calibracion (modis_L1B -> L1B 1km/500m/250m)
#   4. Recorte por pixel/linea (lonlat2pixline sobre GEO, escalado a 250m)
#   5. Ancillary optima (getanc -> .anc)
#   6. Producto L2 (l2gen -> Rrs_645/859 a 250m)
#
# ---- Utilidades de entorno --------------------------------------------------

#' Armar el string que sourcea el entorno de OCSSW
#'
#' Apunta los certificados al CA bundle del sistema porque el curl de OCSSW
#'
#' @param dir_ocssw Directorio raiz de OCSSW (OCSSWROOT).
#' @param ca_bundle Ruta al CA bundle del sistema.
#' @importFrom glue glue
#' @return String de shell listo para anteponer a cualquier herramienta OCSSW.
#' @export
get_ocssw_env <- function(dir_ocssw, ca_bundle = "/etc/ssl/certs/ca-certificates.crt") {
  glue::glue(
    "export OCSSWROOT={dir_ocssw} && source $OCSSWROOT/OCSSW_bash.env",
    " && export CURL_CA_BUNDLE={ca_bundle}",
    " && export SSL_CERT_FILE={ca_bundle}",
    " && export REQUESTS_CA_BUNDLE={ca_bundle}"
  )
}

#' Correr una herramienta de OCSSW y verificar el codigo de salida
#'
#' @param env Entorno de OCSSW (de get_ocssw_env()).
#' @param cmd Comando a correr.
#' @param capturar Si TRUE devuelve stdout; si FALSE el status (invisible).
#' @importFrom glue glue
#' @return stdout si capturar = TRUE; status invisible si FALSE.
#' @export
run_ocssw <- function(env, cmd, capturar = FALSE) {
  full <- shQuote(glue::glue("{env} && {cmd}"))
  if (capturar) {
    return(system2("bash", c("-c", full), stdout = TRUE))
  }
  status <- system2("bash", c("-c", full))
  if (status != 0) stop(glue::glue("Fallo OCSSW (status {status}): {cmd}"))
  invisible(status)
}

# ---- Fase 1: preparar un granulo (serie) ------------------------------------

#' Preparar UN granulo hasta dejarlo listo para l2gen
#'
#' Ejecuta en orden: descompresion, atteph + GEO, L1B, recorte pixel/linea y
#' descarga de ancillary. Todo corre en serie porque atteph y getanc escriben
#' en bases compartidas. Al terminar, generate_l2() puede correr sin red ni DB.
#'
#' @param l1a_file L1A crudo (.L1A_LAC).
#' @param dir_ocssw Directorio raiz de OCSSW.
#' @param dir_trabajo Directorio raiz donde se crea la carpeta de trabajo.
#' @param north,south,west,east Caja lat/lon del area de interes.
#' @param resolution Resolucion de salida en metros (250).
#' @importFrom glue glue
#' @importFrom stringr str_remove str_match str_detect
#' @importFrom tools file_path_sans_ext
#' @return Lista con work, l1b, geo, px, anc y anc_no_optima.
#' @export
prepare_granule <- function(
  l1a_file,
  dir_ocssw,
  dir_trabajo,
  north = -36.3,
  south = -36.5,
  west = -73.1,
  east = -72.25,
  resolution = 250
) {
  env <- get_ocssw_env(normalizePath(dir_ocssw, mustWork = TRUE))
  l1a_file <- normalizePath(l1a_file, mustWork = TRUE)

  work <- file.path(
    dir_trabajo,
    glue::glue("_work_{tools::file_path_sans_ext(basename(l1a_file))}")
  )
  dir.create(work, showWarnings = FALSE, recursive = TRUE)

  # -- 1) Descomprimir si viene en bzip2 (algunos L1A de OB.DAAC) -------------
  tipo <- system2("file", shQuote(l1a_file), stdout = TRUE)
  if (stringr::str_detect(tipo, "bzip2")) {
    comprimido <- glue::glue("{l1a_file}.bz2")
    file.rename(l1a_file, comprimido)
    system2("bunzip2", shQuote(comprimido))
  }

  # -- 2) Atteph + GEO --------------------------------------------------------
  base <- stringr::str_remove(basename(l1a_file), "\\.L1A.*$")
  geo <- file.path(work, glue::glue("{base}.GEO"))

  system2("bash", c("-c", shQuote(glue::glue(
    "{env} && cd {work} && modis_atteph --refreshDB --force-download {l1a_file}"
  ))))
  system2("bash", c("-c", shQuote(glue::glue(
    "{env} && cd {work} && modis_GEO --refreshDB --force-download -o {geo} {l1a_file}"
  ))))
  if (!file.exists(geo)) {
    stop(glue::glue("modis_GEO no genero {geo} (revisa att/eph de {basename(l1a_file)})"))
  }

  # -- 3) L1B (1km + HKM + QKM) -----------------------------------------------
  run_ocssw(env, glue::glue("cd {work} && modis_L1B {l1a_file} {geo}"))
  l1b <- list.files(work, pattern = "_MODIS\\.\\d{8}T\\d{6}\\.L1B\\.hdf$", full.names = TRUE)
  if (length(l1b) == 0) stop("No se encontro el L1B de 1km tras modis_L1B")
  l1b <- l1b[1]

  # -- 4) Recorte pixel/linea escalado a la resolucion de salida ---------------
  salida_crop <- run_ocssw(
    env,
    glue::glue("lonlat2pixline {geo} {west} {south} {east} {north}"),
    capturar = TRUE
  )
  extraer <- function(clave) {
    m <- stringr::str_match(salida_crop, glue::glue("{clave}=(\\d+)"))
    as.integer(m[!is.na(m[, 2]), 2][1])
  }
  idx <- vapply(c("spixl", "epixl", "sline", "eline"), extraer, integer(1))
  if (anyNA(idx)) stop("lonlat2pixline fallo; la caja no intersecta el granulo")

  f <- 1000L %/% as.integer(resolution)
  px <- list(
    spixl = (idx[["spixl"]] - 1L) * f + 1L,
    epixl = idx[["epixl"]] * f,
    sline = (idx[["sline"]] - 1L) * f + 1L,
    eline = idx[["eline"]] * f
  )

  # -- 5) Ancillary optima (getanc) --------------------------------------------
  salida_anc <- run_ocssw(
    env,
    glue::glue("cd {work} && getanc --refreshDB {l1b}"),
    capturar = TRUE
  )
  anc <- glue::glue("{l1b}.anc")
  if (!file.exists(anc)) stop(glue::glue("getanc no genero {anc}"))
  anc_no_optima <- any(stringr::str_detect(salida_anc, "Non-optimal data"))

  list(
    work = work, l1b = l1b, geo = geo, px = px,
    anc = anc, anc_no_optima = anc_no_optima
  )
}

# ---- Fase 2: generar L2 (paralelizable) ------------------------------------

#' Generar el L2 con Rrs a 645 y 859 nm a 250m
#'
#' Corre l2gen con correccion atmosferica NIR-SWIR (aer_opt = -9) y recorte por
#' pixel/linea. Solo lee archivos locales (L1B, GEO, .anc).
#'
#' @param env Entorno de OCSSW.
#' @param l1b_file L1B de 1km.
#' @param geo_file GEO.
#' @param px Lista con spixl/epixl/sline/eline (de prepare_granule()).
#' @param out_dir Directorio de salida.
#' @param resolution Resolucion en metros (250).
#' @param anc_file Ruta al .anc ya bajado en la fase 1.
#' @importFrom glue glue
#' @importFrom stringr str_subset
#' @importFrom readr read_lines write_lines
#' @importFrom magrittr %>%
#' @importFrom tools file_path_sans_ext
#' @return Ruta del L2.
#' @export
generate_l2 <- function(env, l1b_file, geo_file, px, out_dir,
                        resolution = 250, anc_file = NULL) {
  l2 <- file.path(out_dir, sub("\\.L1B\\.hdf$", ".L2.OC.nc", basename(l1b_file)))

  par_lines <- c(
    glue::glue("ifile={l1b_file}"),
    glue::glue("geofile={geo_file}"),
    glue::glue("ofile={l2}"),
    "suite=OC",
    "l2prod=Rrs_645 Rrs_859",
    glue::glue("resolution={resolution}"),
    "aer_opt=-9",
    "aer_wave_short=748",
    "aer_wave_long=869",
    "aer_swir_short=1240",
    "aer_swir_long=2130",
    "cloud_thresh=0.018",
    "cloud_wave=2130",
    "maskhilt=0",
    glue::glue("spixl={px$spixl}"),
    glue::glue("epixl={px$epixl}"),
    glue::glue("sline={px$sline}"),
    glue::glue("eline={px$eline}")
  )

  if (!is.null(anc_file) && file.exists(anc_file)) {
    anc_lines <- readr::read_lines(anc_file) %>%
      stringr::str_subset("^[^#].*=") %>%
      stringr::str_subset("^(ifile|ofile|geofile)=", negate = TRUE)
    par_lines <- c(par_lines, anc_lines)
  }

  par_file <- file.path(out_dir, glue::glue("{tools::file_path_sans_ext(basename(l2))}.par"))
  readr::write_lines(par_lines, par_file)
  run_ocssw(env, glue::glue("l2gen par={par_file}"))
  l2
}

# ---- run por granulo ------------------------------------------------

#' Procesar un granulo del l1a a L2
#'
#'
#' @param l1a_file L1A (.L1A_LAC).
#' @param dir_ocssw Directorio raiz de OCSSW.
#' @param out_dir Directorio de salida (donde queda el L2).
#' @param north,south,west,east Caja lat/lon.
#' @param resolution Resolucion en metros (250).
#' @param limpiar Si TRUE, borra la carpeta de trabajo al terminar.
#' @importFrom glue glue
#' @return Ruta del L2 generado.
#' @export
process_l1a <- function(l1a_file, dir_ocssw, out_dir,
                        north = -36.3, south = -36.5,
                        west = -73.1, east = -72.25,
                        resolution = 250, limpiar = TRUE) {
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  dir_ocssw <- normalizePath(dir_ocssw, mustWork = TRUE)
  out_dir <- normalizePath(out_dir, mustWork = TRUE)
  env <- get_ocssw_env(dir_ocssw)

  prep <- prepare_granule(
    l1a_file, dir_ocssw,
    dir_trabajo = out_dir,
    north = north, south = south, west = west, east = east,
    resolution = resolution
  )
  if (limpiar) on.exit(unlink(prep$work, recursive = TRUE), add = TRUE)
  if (isTRUE(prep$anc_no_optima)) {
    warning(glue::glue("Ancillary no optima para {basename(l1a_file)} (forecast/NRT)"))
  }

  l2 <- generate_l2(
    env, prep$l1b, prep$geo, prep$px, prep$work,
    resolution = resolution, anc_file = prep$anc
  )
  destino <- file.path(out_dir, basename(l2))
  file.rename(l2, destino)
  destino
}

# ---- run directorio completo --------------------------------------

#' Procesar un directorio de L1A crudos a L2
#'
#' Procesa por lotes de batch_size. Cada lote:
#'   Fase 1 (SERIE): prepare_granule por granulo (atteph/getanc tocan DB compartida).
#'   Fase 2 (workers): generate_l2 en paralelo via furrr (solo lee archivos locales).
#'
#' Un granulo que falle avisa con warning y no bota el lote.
#'
#' @param dir_ocssw Directorio raiz de OCSSW.
#' @param dir_input Directorio con los L1A crudos (.L1A_LAC).
#' @param dir_output Directorio donde se escriben los L2.
#' @param north,south,west,east Caja lat/lon.
#' @param resolution Resolucion en metros (250).
#' @param workers Procesos paralelos para l2gen. 1 = todo en serie.
#' @param batch_size Granulos por lote (por defecto = workers).
#' @param limpiar Si TRUE, borra intermedios de cada granulo tras mover su L2.
#' @importFrom glue glue
#' @importFrom purrr safely map compact
#' @importFrom furrr future_map
#' @importFrom future plan multisession sequential
#' @importFrom progressr with_progress progressor
#' @importFrom magrittr %>%
#' @return Invisible: vector con las rutas de los L2 generados.
#' @export
l1_to_l2 <- function(dir_ocssw, dir_input, dir_output,
                     north = -36.3, south = -36.5,
                     west = -73.1, east = -72.25,
                     resolution = 250, workers = 1,
                     batch_size = NULL, limpiar = TRUE) {
  dir_ocssw <- normalizePath(dir_ocssw, mustWork = TRUE)
  dir_input <- normalizePath(dir_input, mustWork = TRUE)
  if (!dir.exists(dir_output)) dir.create(dir_output, recursive = TRUE)
  dir_output <- normalizePath(dir_output, mustWork = TRUE)

  l1a_files <- list.files(dir_input, pattern = "\\.L1A_LAC$", full.names = TRUE)
  if (length(l1a_files) == 0) stop("No hay L1A para procesar")

  if (is.null(batch_size)) batch_size <- max(workers, 1L)
  env <- get_ocssw_env(dir_ocssw)

  if (workers > 1) {
    future::plan(future::multisession, workers = workers)
  } else {
    future::plan(future::sequential)
  }
  on.exit(future::plan(future::sequential), add = TRUE)

  lotes <- split(l1a_files, ceiling(seq_along(l1a_files) / batch_size))
  cat(glue::glue(
    "Procesando {length(l1a_files)} granulo(s) en {length(lotes)} lote(s) ",
    "de hasta {batch_size}, con {workers} worker(s)...\n\n"
  ))

  seguro_preparar <- purrr::safely(prepare_granule)
  seguro_l2 <- purrr::safely(generate_l2)

  resultados <- progressr::with_progress({
    p <- progressr::progressor(steps = length(l1a_files))

    purrr::map(lotes, function(lote) {
      # -- FASE 1 (serie): prepare_granule por granulo -------------------------
      preparados <- purrr::map(lote, function(l1a) {
        r <- seguro_preparar(
          l1a, dir_ocssw,
          dir_trabajo = dir_output,
          north = north, south = south, west = west, east = east,
          resolution = resolution
        )
        if (!is.null(r$error)) {
          warning(glue::glue("Fallo preparando {basename(l1a)}: {conditionMessage(r$error)}"))
          p()
          return(NULL)
        }
        if (isTRUE(r$result$anc_no_optima)) {
          warning(glue::glue("Ancillary no optima para {basename(l1a)} (forecast/NRT)"))
        }
        r$result
      })
      preparados <- purrr::compact(preparados)

      # -- FASE 2 (workers): l2gen puro ----------------------------------------
      furrr::future_map(preparados, function(prep) {
        r <- seguro_l2(
          env, prep$l1b, prep$geo, prep$px, prep$work,
          resolution = resolution, anc_file = prep$anc
        )
        p()
        if (!is.null(r$error)) {
          warning(glue::glue("Fallo l2gen en {basename(prep$l1b)}: {conditionMessage(r$error)}"))
          return(NULL)
        }
        destino <- file.path(dir_output, basename(r$result))
        file.rename(r$result, destino)
        if (limpiar) unlink(prep$work, recursive = TRUE)
        destino
      })
    })
  })

  l2s <- resultados %>%
    unlist(recursive = FALSE) %>%
    purrr::compact() %>%
    unlist()

  cat(glue::glue("\nListo: {length(l2s)} L2 en {dir_output}\n\n"))
  invisible(l2s)
}
