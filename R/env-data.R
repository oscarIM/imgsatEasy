.s2_cache <- new.env(parent = emptyenv())

#' Cargar (y cachear) la grilla de tiles Sentinel-2
#'
#' @param kml Cadena. Ruta al archivo KML/FGB con la grilla de tiles.
#' @param refresh Lógico. Si \code{TRUE}, fuerza la relectura desde disco.
#' @return Objeto \code{sf} con columnas \code{Name} y \code{geometry}.
#' @importFrom magrittr %>%
#' @importFrom sf st_read
#' @importFrom dplyr select
#' @importFrom memoise memoise
get_tile_grid <- memoise::memoise(function(kml) {
  sf::st_read(kml, layer = "Features", quiet = TRUE) %>%
    dplyr::select(Name, geometry)
})
