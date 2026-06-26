# Del L1A crudo de MODIS-Aqua al L2 listo para entrar a l2_to_dataframe().
# Cada paso del pipeline es una funcion corta y de un solo proposito; los
# orquestadores (process_l1a para un granulo, l1_to_l2 para un directorio)
# solo las encadenan en orden.
#
# Orden del pipeline (por granulo):
#   1. decompress_l1a   L1A bz2 -> L1A usable
#   2. generate_geo     L1A -> GEO            (modis_GEO; atteph adentro)
#   3. generate_l1b     L1A + GEO -> L1B      (modis_L1B; 1km/500m/250m)
#   4. compute_crop     GEO + caja -> indices pixel/linea a 250m (lonlat2pixline)
#   5. download_anc     L1B -> .anc           (getanc; toca la base ancilar compartida)
#   6. generate_l2      L1B + GEO + anc -> L2 (l2gen; Rrs_645/859 a 250m)
#
# Los pasos 2 y 5 (modis_atteph y getanc) escriben en bases de datos de
# ancillary/atteph COMPARTIDAS entre granulos. Si dos procesos las tocan a la
# vez quedan con registros a medio escribir o pisados. Por eso prepare_granule
# (pasos 1-5) corre SIEMPRE en serie, y solo generate_l2 (paso 6, l2gen, que
# despues de la fase 1 solo lee archivos locales) se paraleliza.
#
# El L2 sale nombrado en convencion OB.DAAC (<SENSOR>_MODIS.<fecha>.L2.OC.nc),
# que es la que espera el parser de l2_to_dataframe().
#
# Entrada principal: l1_to_l2() procesa un directorio entero de L1A y escribe
# los L2 en otro directorio. Trabaja por efecto (escribe archivos), sin devolver
# nada que haya que asignar, igual que l2_to_dataframe(). El argumento workers
# controla el paralelismo de la fase 2: workers = 1 -> todo en serie;
# workers > 1 -> l2gen en paralelo via furrr. process_l1a() es el motor de un
# solo granulo, por si se quiere correr uno suelto.
# ______________________________________________________________________________

# ---- Utilidades de entorno --------------------------------------------------

#' Armar el string que sourcea el entorno de OCSSW
#'
#' Tras sourcear el entorno, apunta los certificados al CA bundle del SISTEMA.
#' El curl que trae OCSSW (opt/bin/curl) viene compilado buscando el bundle de
#' RedHat (/etc/pki/tls/certs/ca-bundle.crt), que en Ubuntu no existe; sin esto
#' las descargas HTTPS (atteph, getanc) fallan callado y dejan archivos vacios.
#'
#' @param dir_ocssw Directorio raiz de OCSSW (OCSSWROOT).
#' @param ca_bundle Ruta al CA bundle del sistema (Ubuntu/Debian por defecto).
#' @importFrom glue glue
#' @return String de shell que exporta OCSSWROOT, sourcea OCSSW_bash.env y fija el CA.

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
#' Antepone el entorno a la herramienta en una sola llamada de bash, porque
#' `source` solo vive en ese shell. Verifica el status: si no es 0, se detiene
#' (asi una falla silenciosa de l2gen/l2bin no pasa como exito).
#'
#' @param env Entorno de OCSSW (de get_ocssw_env()).
#' @param cmd Comando a correr (ej. "modis_GEO -o salida.GEO entrada.L1A_LAC").
#' @param capturar Si TRUE devuelve la salida de texto; si FALSE el status (invisible).
#' @importFrom glue glue
#' @return stdout si capturar = TRUE; el status (invisible) si FALSE.
#' @export
run_ocssw <- function(env, cmd, capturar = FALSE) {
  full <- shQuote(glue::glue("{env} && {cmd}"))
  if (capturar) {
    return(system2("bash", c("-c", full), stdout = TRUE))
  }
  status <- system2("bash", c("-c", full))
  if (status != 0) {
    stop(glue::glue("Fallo OCSSW (status {status}): {cmd}"))
  }
  invisible(status)
}

# ---- Paso 1: descomprimir ---------------------------------------------------

#' Descomprimir el L1A si viene en bzip2
#'
#' Algunos L1A de OB.DAAC vienen comprimidos en bzip2 sin que el nombre lo
#' indique, y eso rompe modis_GEO. Se detecta con `file` y se descomprime.
#'
#' @param l1a_file Ruta del L1A.
#' @importFrom glue glue
#' @importFrom stringr str_detect
#' @return Ruta del L1A usable (la misma si no estaba comprimido).
#' @export
decompress_l1a <- function(l1a_file) {
  l1a_file <- normalizePath(l1a_file, mustWork = TRUE)
  tipo <- system2("file", shQuote(l1a_file), stdout = TRUE)
  if (stringr::str_detect(tipo, "bzip2")) {
    comprimido <- glue::glue("{l1a_file}.bz2")
    file.rename(l1a_file, comprimido)
    system2("bunzip2", shQuote(comprimido))
  }
  l1a_file
}

# ---- Paso 2: geolocalizacion ------------------------------------------------

#' Generar la geolocalizacion (GEO) del granulo
#'
#' Antes de geolocalizar, baja la actitud/efemerides (atteph) forzando
#' --refreshDB --force-download. Esto es necesario porque la base de ancillary
#' puede quedar con registros vacios de intentos fallidos (p. ej. de corridas en
#' paralelo que reventaron); sin forzar, modis_atteph consulta la base, cree que
#' ya tiene el att/eph y corta sin bajar, dejando un .atteph vacio que hace
#' fallar a modis_GEO con "Missing attitude files".
#'
#' NOTA: este paso toca la base de datos compartida de atteph y por eso debe ir
#' SIEMPRE en serie entre granulos. Vive dentro de prepare_granule (fase 1),
#' que l1_to_l2 corre serial justamente por esto.
#'
#' @param env Entorno de OCSSW (de get_ocssw_env()).
#' @param l1a_file L1A descomprimido.
#' @param out_dir Directorio de salida.
#' @importFrom glue glue
#' @importFrom stringr str_remove
#' @return Ruta del archivo GEO.
#' @export
generate_geo <- function(env, l1a_file, out_dir) {
  base <- stringr::str_remove(basename(l1a_file), "\\.L1A.*$")
  geo <- file.path(out_dir, glue::glue("{base}.GEO"))
  # 1) atteph fresco. modis_atteph sale con codigo != 0 aunque baje bien (vicio
  # de OCSSW), asi que NO se chequea el status; el exito se mide despues por el
  # .GEO. --refreshDB limpia registros basura; --force-download ignora la cache.
  system2("bash", c("-c", shQuote(glue::glue(
    "{env} && modis_atteph --refreshDB --force-download {l1a_file}"
  ))))
  # 2) geolocalizacion. Exito = el .GEO existe (no el status, igual de poco fiable).
  system2("bash", c("-c", shQuote(glue::glue(
    "{env} && modis_GEO --refreshDB --force-download -o {geo} {l1a_file}"
  ))))
  if (!file.exists(geo)) {
    stop(glue::glue("modis_GEO no genero {geo} (revisa el att/eph de {basename(l1a_file)})"))
  }
  geo
}

# ---- Paso 3: calibracion (L1B) ----------------------------------------------

#' Generar el L1B (1km, 500m y 250m) y devolver la ruta del de 1km
#'
#' modis_L1B escribe tres archivos en out_dir. El de 1km es el ifile de l2gen;
#' los _HKM (500m) y _QKM (250m) quedan en la misma carpeta y l2gen los busca
#' solo. El patron exige "_MODIS." antes de la fecha, asi descarta _HKM/_QKM.
#'
#' @param env Entorno de OCSSW.
#' @param l1a_file L1A descomprimido.
#' @param geo_file GEO (de generate_geo()).
#' @param out_dir Directorio de salida.
#' @importFrom glue glue
#' @return Ruta del L1B de 1km.
#' @export
generate_l1b <- function(env, l1a_file, geo_file, out_dir) {
  run_ocssw(env, glue::glue("cd {out_dir} && modis_L1B {l1a_file} {geo_file}"))
  l1b <- list.files(
    out_dir,
    pattern = "_MODIS\\.\\d{8}T\\d{6}\\.L1B\\.hdf$",
    full.names = TRUE
  )
  if (length(l1b) == 0) {
    stop("No se encontro el L1B de 1km tras modis_L1B")
  }
  l1b[1]
}

# ---- Paso 4: recorte por pixel/linea ----------------------------------------

#' Calcular los indices de recorte pixel/linea a la resolucion de salida
#'
#' Se usa recorte por pixel/linea (no el geografico north/south/east/west)
#' porque el geografico gatilla un segfault de l2gen a 250m. lonlat2pixline
#' corre sobre la GEO (1km), asi que sus indices son de 1km; l2gen a 250m
#' trabaja una grilla 4x mas fina, por eso se escalan por f = 1000/resolution.
#'
#' @param env Entorno de OCSSW.
#' @param geo_file GEO del granulo.
#' @param north,south,west,east Caja lat/lon.
#' @param resolution Resolucion de salida en metros (250).
#' @importFrom glue glue
#' @importFrom stringr str_match
#' @return Lista con spixl, epixl, sline, eline ya escalados.
#' @export
compute_crop <- function(env, geo_file, north, south, west, east, resolution = 250) {
  # lonlat2pixline ifile SWlon SWlat NElon NElat
  salida <- run_ocssw(
    env,
    glue::glue("lonlat2pixline {geo_file} {west} {south} {east} {north}"),
    capturar = TRUE
  )

  valor <- function(clave) {
    m <- stringr::str_match(salida, glue::glue("{clave}=(\\d+)"))
    as.integer(m[!is.na(m[, 2]), 2][1])
  }
  idx <- vapply(c("spixl", "epixl", "sline", "eline"), valor, integer(1))
  if (anyNA(idx)) {
    stop("lonlat2pixline no devolvio el recorte; la caja no intersecta el granulo")
  }

  f <- 1000L %/% as.integer(resolution)
  list(
    spixl = (idx[["spixl"]] - 1L) * f + 1L,
    epixl = idx[["epixl"]] * f,
    sline = (idx[["sline"]] - 1L) * f + 1L,
    eline = idx[["eline"]] * f
  )
}

# ---- Paso 5: ancillary -------------------------------------------------------

#' Descargar la ancillary optima para un granulo
#'
#' getanc lee y escribe en una base de ancillary compartida entre granulos; por
#' eso esta descarga debe ir SIEMPRE en serie cuando se procesan varios granulos
#' (vive en prepare_granule, que l1_to_l2 corre serial). generate_l2() en cambio
#' solo LEE el .anc ya bajado por esta funcion (refresh_anc = FALSE), y por eso
#' es segura de correr en paralelo.
#'
#' Sobre el warning "Non-optimal data exist in local repository": getanc lo
#' emite cuando, para la fecha del granulo, lo mejor disponible en el servidor
#' es ancillary forecast/near-real-time (el reanalisis final aun no se publica).
#' --refreshDB ya pide lo optimo; si igual avisa, es porque lo optimo no existe
#' todavia para esa fecha. No es una falla: l2gen corre igual. Si quieres el
#' producto optimo, reprocesa ese granulo semanas despues. Se devuelve tambien
#' si la ancillary quedo no-optima (campo no_optima) para poder rastrearlo.
#'
#' @param env Entorno de OCSSW.
#' @param l1b_file L1B de 1km (getanc se corre sobre este).
#' @param work_dir Directorio de trabajo del granulo.
#' @importFrom glue glue
#' @importFrom stringr str_detect
#' @return Lista con anc (ruta del .anc) y no_optima (TRUE si getanc aviso de ancillary no optima).
#' @export
download_anc <- function(env, l1b_file, work_dir) {
  salida <- run_ocssw(
    env,
    glue::glue("cd {work_dir} && getanc --refreshDB {l1b_file}"),
    capturar = TRUE
  )
  anc <- glue::glue("{l1b_file}.anc")
  if (!file.exists(anc)) {
    stop(glue::glue("getanc no genero {anc}"))
  }
  no_optima <- any(stringr::str_detect(salida, "Non-optimal data"))
  list(anc = anc, no_optima = no_optima)
}

# ---- Paso 6: producto L2 -----------------------------------------------------

#' Generar el L2 con Rrs a 645 y 859 nm a 250m
#'
#' Corre l2gen con correccion atmosferica NIR-SWIR (aer_opt = -9) y recorte por
#' pixel/linea. El L2 se nombra <SENSOR>_MODIS.<fecha>.L2.OC.nc, derivado del
#' L1B, para que entre directo al parser de l2_to_dataframe.
#'
#' Esta funcion en si misma (con refresh_anc = FALSE) NO toca la base de datos
#' compartida de ancillary: solo lee L1B, GEO y el .anc ya bajados y corre
#' l2gen, que trabaja sobre archivos locales. Por eso es la unica parte del
#' pipeline segura de paralelizar con furrr (ver fase 2 de l1_to_l2).
#'
#' @param env Entorno de OCSSW.
#' @param l1b_file L1B de 1km (de generate_l1b()).
#' @param geo_file GEO.
#' @param px Lista con spixl/epixl/sline/eline (de compute_crop()).
#' @param out_dir Directorio de salida.
#' @param resolution Resolucion en metros (250).
#' @param refresh_anc Si TRUE, descarga la ancillary aqui mismo (download_anc),
#'   tocando la base compartida; usar solo en modo serie. Si FALSE, se usa el
#'   .anc ya bajado en anc_file (modo de dos fases).
#' @param anc_file Ruta a un .anc ya bajado (de la fase 1 de l1_to_l2 o de
#'   download_anc). Se usa cuando refresh_anc es FALSE.
#' @importFrom glue glue
#' @importFrom stringr str_subset
#' @importFrom readr read_lines write_lines
#' @importFrom magrittr %>%
#' @importFrom tools file_path_sans_ext
#' @return Ruta del L2.
#' @export
generate_l2 <- function(
  env, l1b_file, geo_file, px, out_dir,
  resolution = 250, refresh_anc = TRUE, anc_file = NULL
) {
  l2 <- file.path(out_dir, sub("\\.L1B\\.hdf$", ".L2.OC.nc", basename(l1b_file)))

  # Parametros base del par de l2gen
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

  # Ancillary optima: o se descarga aqui mismo (modo serie, toca la base
  # compartida via download_anc), o se usa un .anc ya bajado en la fase 1 del
  # modo de dos fases (refresh_anc = FALSE + anc_file).
  anc <- NULL
  if (refresh_anc) {
    anc <- download_anc(env, l1b_file, out_dir)$anc
  } else if (!is.null(anc_file) && !is.na(anc_file) && file.exists(anc_file)) {
    anc <- anc_file
  }
  if (!is.null(anc) && file.exists(anc)) {
    anc_lines <- readr::read_lines(anc) %>%
      stringr::str_subset("^[^#].*=") %>%
      stringr::str_subset("^(ifile|ofile|geofile)=", negate = TRUE)
    par_lines <- c(par_lines, anc_lines)
  }

  par_file <- file.path(out_dir, glue::glue("{tools::file_path_sans_ext(basename(l2))}.par"))
  readr::write_lines(par_lines, par_file)
  run_ocssw(env, glue::glue("l2gen par={par_file}"))
  l2
}

# ---- Fase 1: preparar un granulo hasta dejarlo listo para l2gen -------------

#' Preparar UN granulo hasta dejarlo listo para l2gen (fase 1, serial)
#'
#' Hace todo lo que toca bases de datos compartidas: descomprime, genera GEO
#' (atteph adentro), L1B, el recorte por pixel/linea y descarga la ancillary
#' (getanc). Al terminar, el granulo queda con todo lo que generate_l2() necesita
#' para correr sin tocar red ni DB, por lo que puede llamarse en paralelo en la
#' fase 2 de l1_to_l2().
#'
#' Aisla cada granulo en su propia carpeta de trabajo dentro de dir_trabajo,
#' para que generate_l1b no agarre el L1B de otro granulo si varios comparten raiz.
#'
#' @param l1a_file L1A crudo (.L1A_LAC).
#' @param dir_ocssw Directorio raiz de OCSSW.
#' @param dir_trabajo Directorio raiz donde se crea la carpeta de trabajo del granulo.
#' @param north,south,west,east Caja del Itata.
#' @param resolution Resolucion en metros (250).
#' @importFrom glue glue
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
  l1a <- decompress_l1a(l1a_file)

  work <- file.path(dir_trabajo, glue::glue("_work_{tools::file_path_sans_ext(basename(l1a))}"))
  dir.create(work, showWarnings = FALSE, recursive = TRUE)

  geo <- generate_geo(env, l1a, work)
  l1b <- generate_l1b(env, l1a, geo, work)
  px <- compute_crop(env, geo, north, south, west, east, resolution)
  anc <- download_anc(env, l1b, work)

  list(
    work = work, l1b = l1b, geo = geo, px = px,
    anc = anc$anc, anc_no_optima = anc$no_optima
  )
}

# ---- Orquestador de un solo granulo -----------------------------------------

#' Procesar UN granulo del crudo al L2
#'
#' Encadena fase 1 (prepare_granule: descomprime + GEO + L1B + recorte +
#' getanc) y fase 2 (generate_l2: l2gen) en una carpeta de trabajo propia, y deja
#' el L2 en out_dir, listo para l2_to_dataframe. Reusa los mismos helpers que
#' l1_to_l2, asi que no hay logica duplicada entre el modo de un granulo y el de
#' directorio completo.
#'
#' @param l1a_file L1A crudo (.L1A_LAC).
#' @param dir_ocssw Directorio raiz de OCSSW.
#' @param out_dir Directorio de salida (donde queda el L2).
#' @param north,south,west,east Caja del Itata.
#' @param resolution Resolucion en metros (250).
#' @param limpiar Si TRUE, borra la carpeta de trabajo (intermedios) al terminar.
#' @importFrom glue glue
#' @return Ruta del L2 generado.
#' @export
process_l1a <- function(
  l1a_file,
  dir_ocssw,
  out_dir,
  north = -36.3,
  south = -36.5,
  west = -73.1,
  east = -72.25,
  resolution = 250,
  limpiar = TRUE
) {
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
  dir_ocssw <- normalizePath(dir_ocssw, mustWork = TRUE)
  out_dir <- normalizePath(out_dir, mustWork = TRUE)
  env <- get_ocssw_env(dir_ocssw)

  prep <- prepare_granule(
    l1a_file = l1a_file, dir_ocssw = dir_ocssw, dir_trabajo = out_dir,
    north = north, south = south, west = west, east = east, resolution = resolution
  )
  if (limpiar) on.exit(unlink(prep$work, recursive = TRUE), add = TRUE)
  if (isTRUE(prep$anc_no_optima)) {
    warning(glue::glue("Ancillary no optima para {basename(l1a_file)} (forecast/NRT)"))
  }

  l2 <- generate_l2(
    env = env, l1b_file = prep$l1b, geo_file = prep$geo, px = prep$px,
    out_dir = prep$work, resolution = resolution,
    refresh_anc = FALSE, anc_file = prep$anc
  )

  destino <- file.path(out_dir, basename(l2))
  file.rename(l2, destino)
  destino
}

# ---- Entrada principal: directorio de L1A -> directorio de L2 ---------------

#' Procesar un directorio de L1A crudos a L2 (serie o paralelo segun workers)
#'
#' Lee todos los L1A de dir_input y los procesa por LOTES de batch_size. Cada
#' lote pasa por dos fases:
#'
#'   Fase 1 (SIEMPRE serie): prepare_granule por granulo (descomprime + GEO +
#'     L1B + recorte + getanc). Serial porque modis_atteph y getanc escriben en
#'     bases de datos compartidas; en paralelo se pisan.
#'
#'   Fase 2 (segun workers): generate_l2 (l2gen). Con workers = 1 corre en serie
#'     (future sequential); con workers > 1, en paralelo via furrr. l2gen aqui
#'     solo lee archivos locales (L1B, GEO, .anc), no toca red ni DB, asi que es
#'     seguro paralelizarlo.
#'
#' El troceado en lotes acota el pico de disco: solo coexisten los intermedios
#' de un lote a la vez (se limpian al cerrar cada lote, si limpiar = TRUE),
#' en vez de los de todo el directorio.
#'
#' Trabaja por efecto: escribe los L2 en dir_output y devuelve sus rutas
#' invisible. Un granulo que falle (en cualquier fase) avisa con warning y no
#' bota el lote.
#'
#' @param dir_ocssw Directorio raiz de OCSSW.
#' @param dir_input Directorio con los L1A crudos (.L1A_LAC).
#' @param dir_output Directorio donde se escriben los L2.
#' @param north,south,west,east Caja del Itata.
#' @param resolution Resolucion en metros (250).
#' @param workers Procesos paralelos para la fase 2 (l2gen). 1 = todo en serie.
#'   l2gen es intensivo en CPU/memoria; no conviene pasar el numero de nucleos
#'   fisicos.
#' @param batch_size Granulos por lote. Por defecto max(workers, 1): de a uno en
#'   serie, o una tanda por pool de workers en paralelo. Subirlo amortiza el
#'   setup de la fase 2 pero aumenta el pico de disco; bajarlo lo reduce.
#' @param limpiar Si TRUE, borra los intermedios de cada granulo tras mover su L2.
#' @importFrom glue glue
#' @importFrom purrr safely map compact
#' @importFrom furrr future_map
#' @importFrom future plan multisession sequential
#' @importFrom progressr with_progress progressor
#' @importFrom magrittr %>%
#' @return Invisible: vector con las rutas de los L2 generados.
#' @export
l1_to_l2 <- function(
  dir_ocssw,
  dir_input,
  dir_output,
  north = -36.3,
  south = -36.5,
  west = -73.1,
  east = -72.25,
  resolution = 250,
  workers = 1,
  batch_size = NULL,
  limpiar = TRUE
) {
  dir_ocssw <- normalizePath(dir_ocssw, mustWork = TRUE)
  dir_input <- normalizePath(dir_input, mustWork = TRUE)
  if (!dir.exists(dir_output)) dir.create(dir_output, recursive = TRUE)
  dir_output <- normalizePath(dir_output, mustWork = TRUE)

  l1a_files <- list.files(dir_input, pattern = "\\.L1A_LAC$", full.names = TRUE)
  if (length(l1a_files) == 0) {
    stop("No hay L1A para procesar")
  }

  # Lote por defecto: en serie de a uno (disco minimo); en paralelo una tanda
  # del tamano del pool de workers (pico de disco acotado a esa tanda).
  if (is.null(batch_size)) {
    batch_size <- max(workers, 1L)
  }

  env <- get_ocssw_env(dir_ocssw)

  # Plan de future una sola vez. workers > 1 -> multisession; si no, sequential
  # (asi furrr::future_map corre identico al serial sin ramas aparte).
  if (workers > 1) {
    future::plan(future::multisession, workers = workers)
  } else {
    future::plan(future::sequential)
  }
  on.exit(future::plan(future::sequential), add = TRUE)

  lotes <- split(l1a_files, ceiling(seq_along(l1a_files) / batch_size))

  cat(glue::glue(
    "Procesando {length(l1a_files)} granulo(s) en {length(lotes)} lote(s) de hasta ",
    "{batch_size}, con {workers} worker(s)...\n\n"
  ))

  seguro_preparar <- purrr::safely(prepare_granule)
  seguro_l2 <- purrr::safely(generate_l2)

  resultados <- progressr::with_progress({
    p <- progressr::progressor(steps = length(l1a_files))

    purrr::map(lotes, function(lote) {
      # ---- FASE 1 (serie): GEO + L1B + recorte + getanc por granulo ---------
      preparados <- purrr::map(lote, function(l1a) {
        r <- seguro_preparar(
          l1a_file = l1a, dir_ocssw = dir_ocssw, dir_trabajo = dir_output,
          north = north, south = south, west = west, east = east,
          resolution = resolution
        )
        if (!is.null(r$error)) {
          warning(glue::glue("Fallo preparando {basename(l1a)}: {conditionMessage(r$error)}"))
          p() # este granulo no llega a la fase 2; cuenta su paso aqui
          return(NULL)
        }
        if (isTRUE(r$result$anc_no_optima)) {
          warning(glue::glue("Ancillary no optima para {basename(l1a)} (forecast/NRT)"))
        }
        r$result
      })
      preparados <- purrr::compact(preparados)

      # ---- FASE 2 (workers): l2gen puro, solo lee archivos locales -----------
      furrr::future_map(preparados, function(prep) {
        r <- seguro_l2(
          env = env, l1b_file = prep$l1b, geo_file = prep$geo, px = prep$px,
          out_dir = prep$work, resolution = resolution,
          refresh_anc = FALSE, anc_file = prep$anc
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
  att_files <- list.files(getwd(), pattern = ".atteph$", full.names = TRUE, recursive = TRUE)
  unlink(att_files)
  l2s <- resultados %>%
    unlist(recursive = FALSE) %>%
    purrr::compact() %>%
    unlist()

  cat(glue::glue("\nListo: {length(l2s)} L2 en {dir_output}\n\n"))
  invisible(l2s)
}
