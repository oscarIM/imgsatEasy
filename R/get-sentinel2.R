#' Identificar y graficar tiles Sentinel-2 que cubren una ROI
#'
#' Lee la grilla de tiles Sentinel-2 desde un archivo KML, identifica los tiles
#' que intersectan una región de interés (ROI) definida por sus límites
#' geográficos, calcula el porcentaje de la ROI cubierto por cada tile y produce
#' un mapa de los tiles seleccionados sobre el continente.
#'
#' @param north Numérico. Límite norte de la ROI (latitud, grados decimales).
#' @param south Numérico. Límite sur de la ROI (latitud, grados decimales).
#' @param west Numérico. Límite oeste de la ROI (longitud, grados decimales).
#' @param east Numérico. Límite este de la ROI (longitud, grados decimales).
#'
#' @return Vector de caracteres con los nombres de los tiles que cubren la ROI,
#'   ordenados de mayor a menor porcentaje de cobertura. Como efecto secundario,
#'   imprime un mapa de los tiles seleccionados.
#'
#' @importFrom magrittr %>%
#' @importFrom sf st_read st_as_sfc st_bbox st_crs st_intersects st_transform st_area st_intersection st_drop_geometry st_make_valid st_crop st_centroid
#' @importFrom dplyr select filter pull mutate arrange desc
#' @importFrom ggsci pal_nejm
#' @importFrom rnaturalearth ne_countries
#' @importFrom ggplot2 ggplot geom_sf scale_color_manual geom_sf_text coord_sf theme_minimal theme labs element_rect aes
#' @importFrom glue glue
#'
#' @examples
#' \dontrun{
#' get_s2_tiles(north = -36.0, south = -37.0, west = -73.5, east = -72.5)
#' }
#'
#' @export
get_s2_tiles <- function(north, south, west, east) {
  tile <- get_tile_grid() %>%
    dplyr::select(Name, geometry)
  roi <- sf::st_as_sfc(
    sf::st_bbox(
      c(xmin = west, ymin = south, xmax = east, ymax = north),
      crs = sf::st_crs(tile)
    )
  )
  tile_name <- tile %>%
    dplyr::filter(lengths(sf::st_intersects(geometry, roi)) > 0) %>%
    dplyr::pull(Name)
  tile_utm <- tile %>%
    dplyr::filter(Name %in% tile_name) %>%
    sf::st_transform(32718)
  roi_utm <- sf::st_transform(roi, 32718)
  roi_area <- sf::st_area(roi_utm)
  tile_cover <- tile_utm %>%
    sf::st_intersection(roi_utm) %>%
    dplyr::mutate(
      area_int = sf::st_area(geometry),
      pct_roi  = as.numeric(area_int / roi_area * 100)
    ) %>%
    sf::st_drop_geometry() %>%
    dplyr::select(Name, pct_roi) %>%
    dplyr::arrange(dplyr::desc(pct_roi)) %>%
    dplyr::filter(pct_roi > 0) %>%
    dplyr::pull(Name)
  # plot section
  bbox_wgs84 <- tile_utm %>%
    sf::st_transform(4326) %>%
    sf::st_bbox()
  cols <- ggsci::pal_nejm()(length(tile_cover))
  # Expandir el bbox un poco para no cortar justo en el borde
  bbox_wgs84["xmin"] <- bbox_wgs84["xmin"] - 1
  bbox_wgs84["xmax"] <- bbox_wgs84["xmax"] + 1
  bbox_wgs84["ymin"] <- bbox_wgs84["ymin"] - 1
  bbox_wgs84["ymax"] <- bbox_wgs84["ymax"] + 1
  continente <- rnaturalearth::ne_countries(scale = "large", returnclass = "sf") %>%
    sf::st_make_valid() %>%
    sf::st_crop(bbox_wgs84) %>% # recortar en WGS84 primero
    sf::st_transform(32718) %>%
    sf::st_make_valid()
  tile_labels <- tile_utm %>%
    dplyr::filter(Name %in% tile_cover) %>%
    dplyr::mutate(centroid = sf::st_centroid(geometry))
  p <- ggplot2::ggplot() +
    # Continente
    ggplot2::geom_sf(data = continente, fill = "grey40", color = "grey50", linewidth = 0.3) +
    # Todos los tiles: solo borde, sin relleno
    ggplot2::geom_sf(data = tile_utm %>% dplyr::filter(Name %in% tile_cover), ggplot2::aes(color = Name), fill = NA, linewidth = 0.5) +
    ggplot2::scale_color_manual(name = "Tiles", values = cols) +
    # ROI
    ggplot2::geom_sf(data = roi_utm, fill = "gray80", color = "red", linetype = "dashed", linewidth = 0.6, alpha = 0.50) +
    # Etiquetas
    ggplot2::geom_sf_text(
      data = tile_labels,
      ggplot2::aes(label = Name),
      size = 3,
      fontface = "bold",
      color = "grey20"
    ) +
    ggplot2::coord_sf(
      xlim = sf::st_bbox(tile_utm)[c("xmin", "xmax")],
      ylim = sf::st_bbox(tile_utm)[c("ymin", "ymax")]
    ) +
    ggplot2::theme_minimal() +
    ggplot2::theme(panel.background = ggplot2::element_rect(fill = "#c6e0f5", color = NA)) +
    ggplot2::labs(
      title    = "Sentinel-2 tiles",
      subtitle = glue::glue("Tiles seleccionados: {paste(tile_cover, collapse = ', ')}")
    )
  print(p)

  return(tile_cover)
}

#' Obtener un token de acceso de Copernicus Data Space Ecosystem (CDSE)
#'
#' Solicita un token OAuth2 al endpoint de identidad de CDSE usando el flujo
#' \code{password} y el cliente público \code{cdse-public}.
#'
#' @param user Cadena. Usuario de CDSE. Por defecto toma la variable de entorno
#'   \code{CDSE_USER}.
#' @param pass Cadena. Contraseña de CDSE. Por defecto toma la variable de
#'   entorno \code{CDSE_PASS}.
#'
#' @return Cadena con el \code{access_token} vigente.
#'
#' @importFrom magrittr %>%
#' @importFrom httr2 request req_body_form req_perform resp_body_json
#' @importFrom purrr pluck
#'
#' @examples
#' \dontrun{
#' token <- get_cdse_token()
#' }
#'
#' @export
get_cdse_token <- function(
  user = Sys.getenv("CDSE_USER"),
  pass = Sys.getenv("CDSE_PASS")
) {
  httr2::request("https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token") %>%
    httr2::req_body_form(
      grant_type = "password",
      client_id  = "cdse-public",
      username   = user,
      password   = pass
    ) %>%
    httr2::req_perform() %>%
    httr2::resp_body_json() %>%
    purrr::pluck("access_token")
}

#' Construir intervalos temporales en formato ISO 8601 para consultas CDSE
#'
#' Genera intervalos de tiempo a partir de un rango de fechas, ya sea como un
#' único intervalo continuo o como una lista de intervalos por mes calendario
#' (uno por cada año presente en el rango).
#'
#' @param start_date Fecha (o cadena coercible a fecha). Inicio del rango.
#' @param end_date Fecha (o cadena coercible a fecha). Fin del rango.
#' @param type Cadena. Tipo de intervalo: \code{"continuous"} para un único
#'   intervalo, o \code{"by_month"} para una lista de intervalos por mes.
#' @param month Numérico. Número de mes (1-12). Obligatorio cuando
#'   \code{type = "by_month"}.
#'
#' @return Si \code{type = "continuous"}, una cadena con el intervalo ISO 8601.
#'   Si \code{type = "by_month"}, una lista de cadenas, una por cada año del
#'   rango que contenga el mes solicitado.
#'
#' @importFrom magrittr %>%
#' @importFrom lubridate as_datetime month year
#' @importFrom glue glue
#' @importFrom tibble tibble
#' @importFrom dplyr mutate filter group_by group_split
#' @importFrom purrr map
#' @importFrom cli cli_abort
#'
#' @examples
#' \dontrun{
#' get_time_interval("2023-01-01", "2023-12-31", type = "continuous")
#' get_time_interval("2020-01-01", "2023-12-31", type = "by_month", month = 6)
#' }
#'
#' @export
get_time_interval <- function(start_date, end_date, type, month = NULL) {
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  # Helper interno: convierte un par de fechas a string ISO 8601
  to_iso_interval <- function(d1, d2) {
    t1 <- lubridate::as_datetime(glue::glue("{d1}T00:00:00Z"))
    t2 <- lubridate::as_datetime(glue::glue("{d2}T23:59:59Z"))
    glue::glue("{format(t1, '%Y-%m-%dT%H:%M:%SZ')}/{format(t2, '%Y-%m-%dT%H:%M:%SZ')}")
  }

  # ── Intervalo continuo ──────────────────────────────────────────────────────
  if (type == "continuous") {
    interval_time <- to_iso_interval(start_date, end_date)
    return(interval_time)
  }

  # ── Por mes calendario ──────────────────────────────────────────────────────
  if (type == "by_month") {
    if (is.null(month)) {
      cli::cli_abort(
        c("El argumento {.arg month} es obligatorio cuando {.arg type} = {.val by_month}.",
          "i" = "Proporciona un número de mes entre 1 y 12."
        )
      )
    }

    all_dates <- tibble::tibble(full_date = seq(from = start_date, to = end_date, by = "day")) %>%
      dplyr::mutate(
        month_date = lubridate::month(full_date),
        year       = lubridate::year(full_date)
      ) %>%
      dplyr::filter(month_date == month)

    list_dates <- all_dates %>%
      dplyr::group_by(year) %>%
      dplyr::group_split()

    interval_time <- purrr::map(list_dates, ~ {
      range_date <- range(.x$full_date)
      to_iso_interval(range_date[1], range_date[2])
    })

    return(interval_time)
  }

  # ── Tipo no reconocido ──────────────────────────────────────────────────────
  cli::cli_abort(
    c("El tipo {.val {type}} no está implementado.",
      "i" = "Opciones válidas: {.val continuous}, {.val by_month}."
    )
  )
}

#' Crear un gestor de token CDSE con cacheo y renovación automática
#'
#' Devuelve una clausura (función sin argumentos) que entrega un token de CDSE
#' vigente, renovándolo automáticamente cuando está próximo a expirar. Mantiene
#' el token y su marca de tiempo en un entorno privado, evitando solicitudes
#' innecesarias al endpoint de identidad.
#'
#' @param ttl Numérico. Tiempo de vida del token en segundos (por defecto 600).
#' @param margin Numérico. Margen de seguridad en segundos antes de la
#'   expiración para forzar la renovación (por defecto 60).
#'
#' @return Una función sin argumentos que, al ser llamada, devuelve una cadena
#'   con un token de acceso CDSE vigente.
#'
#' @seealso \code{\link{get_cdse_token}}
#'
#' @examples
#' \dontrun{
#' get_token <- make_token_manager()
#' get_token()
#' }
#'
#' @export
make_token_manager <- function(ttl = 600, margin = 60) {
  state <- new.env(parent = emptyenv())
  state$token <- NULL
  state$obtained <- Sys.time() - ttl

  function() {
    elapsed <- as.numeric(Sys.time() - state$obtained, units = "secs")
    if (!is.null(state$token) && elapsed < (ttl - margin)) {
      return(state$token)
    }
    state$token <- get_cdse_token()
    state$obtained <- Sys.time()
    state$token
  }
}

#' Descargar y descomprimir productos Sentinel-2 desde CDSE
#'
#' Descarga en lote una lista de productos Sentinel-2 (formato \code{.SAFE})
#' desde el catálogo OData de CDSE, descomprime cada uno y mantiene un historial
#' persistente para evitar descargas repetidas. Omite los productos ya
#' presentes en disco o registrados en el historial.
#'
#' @param all_files Vector de caracteres con los nombres de los productos a
#'   descargar (sin la extensión \code{.SAFE}).
#' @param manager Función gestora de token, típicamente devuelta por
#'   \code{\link{make_token_manager}}.
#' @param output_dir Cadena. Directorio de destino donde se guardan y descomprimen los
#'   productos. Se crea si no existe.
#' @param history_path Cadena. Ruta al archivo \code{.rds} donde se persiste el
#'   historial de descargas (por defecto \code{"download_files.rds"}).
#'
#' @return De forma invisible, un \code{tibble} con el historial actualizado de
#'   productos descargados y sus marcas de tiempo.
#'
#' @importFrom magrittr %>%
#' @importFrom httr2 request req_url_query req_perform resp_body_json req_auth_bearer_token
#' @importFrom glue glue
#' @importFrom fs path dir_exists
#' @importFrom utils unzip
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows
#' @importFrom purrr keep walk
#'
#' @seealso \code{\link{make_token_manager}}, \code{\link{get_s2_tiles}}
#'
#' @examples
#' \dontrun{
#' manager <- make_token_manager()
#' run_download(all_files, manager, dest = "data/raw")
#' }
#'
#' @export
run_download <- function(all_files, manager, output_dir, history_path = "download_files.rds") {
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  }

  # --- Descarga de un producto (cierra sobre manager y dest) ---
  download_safe <- function(product_name) {
    meta <- httr2::request("https://catalogue.dataspace.copernicus.eu/odata/v1/Products") %>%
      httr2::req_url_query(`$filter` = glue::glue("Name eq '{product_name}.SAFE'")) %>%
      httr2::req_perform() %>%
      httr2::resp_body_json()
    uuid <- meta$value[[1]]$Id
    zip_path <- fs::path(output_dir, glue::glue("{product_name}.zip"))

    httr2::request(glue::glue("https://zipper.dataspace.copernicus.eu/odata/v1/Products({uuid})/$value")) %>%
      httr2::req_auth_bearer_token(manager()) %>%
      httr2::req_perform(path = zip_path)

    utils::unzip(zip_path, exdir = output_dir)
  }

  # --- Historial: en memoria (environment) y persistido en .rds ---
  state <- new.env(parent = emptyenv())
  state$history <- if (file.exists(history_path)) {
    readRDS(history_path)
  } else {
    tibble::tibble(product_name = character(), fecha = as.POSIXct(character()))
  }

  register_done <- function(product_name) {
    state$history <- dplyr::bind_rows(
      state$history,
      tibble::tibble(product_name = product_name, fecha = Sys.time())
    )
    saveRDS(state$history, history_path)
  }

  # --- Pendientes: excluye lo del historial Y lo que ya está en disco ---
  on_disk <- all_files %>%
    purrr::keep(~ fs::dir_exists(fs::path(output_dir, glue::glue("{.x}.SAFE"))))

  pending <- setdiff(all_files, union(state$history$product_name, on_disk))
  message(glue::glue("Pendientes: {length(pending)} de {length(all_files)}"))

  # --- Descargar y registrar cada éxito ---
  purrr::walk(pending, function(pn) {
    tryCatch(
      {
        download_safe(pn)
        register_done(pn)
      },
      error = function(e) {
        message(glue::glue("Error en {pn}: {conditionMessage(e)}"))
      }
    )
  }, .progress = TRUE)

  invisible(state$history)
}
