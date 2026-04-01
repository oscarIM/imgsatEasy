#' Limpia datos oceanográficos eliminando registros en tierra
#'
#' Convierte un data frame con coordenadas a objeto `sf`, elimina los puntos
#' que intersectan con una máscara terrestre y exporta el resultado como
#' archivo CSV con columnas `lon` y `lat` restauradas.
#'
#' @param data_raw Data frame con columnas `lon` y `lat` en coordenadas
#'   geográficas (CRS 4326).
#' @param output_file Ruta del archivo CSV de salida (e.g.
#'   `"data/clean/datos_limpios.csv"`).
#' @param shp_mask Objeto `sf` con la máscara terrestre. Si es `NULL`, se
#'   descarga automáticamente una máscara de baja resolución mediante
#'   [rnaturalearth::ne_countries()].
#'
#' @return Data frame limpio (sin geometría) con las mismas columnas que
#'   `data_raw` más `lon` y `lat` actualizadas, exportado además como CSV en
#'   `output_file`.
#'
#' @details
#' La función ejecuta los siguientes pasos:
#' \enumerate{
#'   \item Prepara la máscara terrestre (repara geometrías y disuelve polígonos).
#'   \item Convierte `data_raw` a objeto `sf` y reproyecta al CRS de la máscara.
#'   \item Elimina los puntos que intersectan con la máscara terrestre.
#'   \item Recupera las coordenadas proyectadas como columnas `lon` / `lat`.
#'   \item Exporta el resultado a `output_file`.
#' }
#'
#' @note Si `shp_mask` es `NULL` se requiere conexión a internet para descargar
#'   la máscara desde Natural Earth.
#'
#' @seealso [sf::st_intersects()], [rnaturalearth::ne_countries()]
#'
#' @importFrom sf st_as_sf st_transform st_crs st_make_valid st_union
#'   st_intersects st_coordinates st_drop_geometry
#' @importFrom dplyr filter bind_cols rename
#' @importFrom readr write_csv
#' @importFrom rnaturalearth ne_countries
#' @importFrom magrittr %>%
#'
#' @export clean_data
#'
#' @examples
#' \dontrun{
#' shp_mask <- sf::st_read("Golfo_Arauco_prj2.shp")
#'
#' clean_data(
#'   data_raw    = mis_datos,
#'   output_file = "data/clean/datos_limpios.csv",
#'   shp_mask    = shp_mask
#' )
#' }
clean_data <- function(data_raw, output_file, shp_mask = NULL) {
  # 1. Preparar máscara terrestre --------------------------------------------

  if (is.null(shp_mask)) {
    cli::cli_inform("Se usará una máscara de baja resolución espacial.")
    shp_mask <- rnaturalearth::ne_countries(scale = "large", returnclass = "sf") %>%
      sf::st_make_valid() %>% # repara vértices duplicados y auto-intersecciones
      sf::st_union()
  } else {
    shp_mask <- shp_mask %>%
      sf::st_make_valid() %>% # repara vértices duplicados y auto-intersecciones
      sf::st_union() # disuelve polígonos en geometría única
  }

  # 2. Convertir a sf y reproyectar ------------------------------------------

  data_sf <- sf::st_as_sf(data_raw, coords = c("lon", "lat"), crs = 4326) %>%
    sf::st_transform(crs = sf::st_crs(shp_mask))

  # 3. Eliminar puntos en tierra ----------------------------------------------

  en_tierra <- as.vector(sf::st_intersects(data_sf, shp_mask, sparse = FALSE))

  data_sf <- data_sf %>%
    dplyr::filter(!en_tierra)

  cli::cli_inform("Se eliminaron {sum(en_tierra)} punto(s) terrestre(s).")

  # 4. Recuperar coordenadas y eliminar geometría ----------------------------

  coords_final <- sf::st_coordinates(data_sf)

  clean_data <- data_sf %>%
    sf::st_drop_geometry() %>%
    dplyr::bind_cols(as.data.frame(coords_final)) %>%
    dplyr::rename(lon = X, lat = Y)

  # 5. Exportar --------------------------------------------------------------

  readr::write_csv(x = clean_data, file = output_file)

  cli::cli_inform(c(
    "Archivo limpio guardado exitosamente.",
    "*" = "Archivo: {output_file}",
    "*" = "Registros eliminados: {sum(en_tierra)}",
    "*" = "Registros en archivo limpio: {nrow(clean_data)}"
  ))

  return(invisible(clean_data))
}
