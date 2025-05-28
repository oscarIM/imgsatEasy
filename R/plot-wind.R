#' @title plot_wind
#' @description Genera un gráfico semanal de velocidad y dirección promedio del viento sobre una región específica.
#' @param data Dataframe con columnas `lon`, `lat`, `date`, `u`, `v`, `speed_mean`.
#' @param start_date Fecha de inicio del periodo (formato "YYYY-MM-DD").
#' @param end_date Fecha de término del periodo (formato "YYYY-MM-DD").
#' @param cell_size Tamaño de las celdas para agregación espacial (por defecto 150).
#' @param shp_sf Objeto `sf` con los polígonos del shapefile de fondo (ya transformado a CRS 4326).
#' @param n_col Número de columnas para la grilla de facetas.
#' @param xlim Vector con límites longitudinales (e.g., c(-76, -71.5)).
#' @param ylim Vector con límites latitudinales (e.g., c(-38, -36)).
#' @param output_name Nombre del archivo de salida (debe terminar en .png).
#' @param width Ancho del archivo en pulgadas.
#' @param height Alto del archivo en pulgadas.
#' @return Exporta un gráfico `.png` que muestra la velocidad y dirección promedio del viento por semana.
#' @export plot_wind
#' @importFrom dplyr mutate filter between group_by group_split bind_rows select row_number rename as_tibble reframe
#' @importFrom sf st_as_sf st_transform st_read st_make_grid st_sf st_contains st_geometry st_centroid st_coordinates
#' @importFrom purrr map map_dbl
#' @importFrom tibble deframe
#' @importFrom glue glue
#' @importFrom lubridate isoweek month
#' @importFrom stringr str_to_sentence
#' @importFrom grid convertWidth stringWidth unit
#' @import ggplot2
#' @examples
#' \dontrun{
#' data <- arrow::read_parquet("data_viento_2025.parquet")
#' shp_sf <- poligon.shp
#' plot_wind(
#'   data = data,
#'   start_date = "2025-01-01",
#'   end_date = "2025-01-31",
#'   cell_size = 150,
#'   shp_sf = shp_sf,
#'   n_col = 2,
#'   xlim = c(-76, -71.5),
#'   ylim = c(-38, -36),
#'   output_name = "plot.png",
#'   width = 7,
#'   height = 10
#' )
#' }
plot_wind <- function(data, start_date, end_date, cell_size = 150, shp_sf, n_col, xlim, ylim, output_name, width, height) {
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  df_plot <- data %>%
    dplyr::mutate(date = as.Date(date)) %>% 
    dplyr::filter(dplyr::between(lat, ylim[1], ylim[2]), 
                  dplyr::between(lon, xlim[1], xlim[2]),
                  dplyr::between(date, start_date, end_date)) %>% 
    dplyr::mutate(week = lubridate::isoweek(date),
                  month = lubridate::month(date),
                  week_name = glue::glue("Semana {week}"))
  # ==== CONVERTIR A SF Y AGRUPAR POR SEMANA ====
  df_sf <- df_plot %>%
    sf::st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    dplyr::group_by(week, week_name) %>%
    dplyr::group_split()
  # ==== CREAR GRILLA BASE ====
  grid <- df_sf[[1]] %>%
    sf::st_make_grid(n = c(cell_size, cell_size)) %>%
    sf::st_sf() %>%
    dplyr::mutate(id = dplyr::row_number())
  # ==== FUNCIÓN PARA PROMEDIAR POR CELDA ====
  fn_grid <- function(grid, data_sf) {
    # Calcular la relación de contención (fuera de mutate)
    contained <- sf::st_contains(grid, data_sf)
    # Calcular estadísticas por celda (usando map_dbl sobre índices)
    grid %>%
      dplyr::mutate(
        obs = lengths(contained),
        week = unique(data_sf$week),
        week_name = unique(data_sf$week_name),
        u = purrr::map_dbl(contained, ~ mean(data_sf[.x, ]$u, na.rm = TRUE)),
        v = purrr::map_dbl(contained, ~ mean(data_sf[.x, ]$v, na.rm = TRUE)),
        speed_mean = purrr::map_dbl(contained, ~ mean(data_sf[.x, ]$speed_mean, na.rm = TRUE))
      )
  }
  # ==== APLICAR FUNCIÓN A CADA SEMANA ====
  grid_filled <- df_sf %>%
    purrr::map(~ fn_grid(grid, .x)) %>%
    dplyr::bind_rows() %>%
    dplyr::select(obs, u, v, speed_mean, week, week_name, geometry)
  # ==== CALCULAR CENTROIDES ====
  grid_points <- sf::st_centroid(grid_filled) %>%
    sf::st_coordinates() %>%
    dplyr::as_tibble() %>%
    dplyr::rename(lon = X, lat = Y)
  attributes <- grid_filled
  sf::st_geometry(attributes) <- NULL
  gridded_wind <- bind_cols(grid_points, attributes)
  # ==== plots vars ====
  lmax <- max(ceiling(df_plot$speed_mean), na.rm = TRUE)
  labeller_vector <- df_plot %>%
    dplyr::group_by(week_name) %>%
    dplyr::reframe(
      label = paste0(
        week_name, " (",
        format(min(date), "%d"), "–", format(max(date), "%d %b"), ")")) %>%
    tibble::deframe()
  # ==== GRAFICAR ====
  guide_title <- "Velocidad promedio  [m·s⁻¹]"
  barwidth <- grid::convertWidth(grid::stringWidth(guide_title), unitTo = "lines", valueOnly = TRUE) * 1.1 + n_col
  plot <- ggplot2::ggplot() +
    ggplot2::geom_raster(data = df_plot, aes(x = lon, y = lat, fill = speed_mean)) +
    ggplot2::scale_fill_gradientn(colours = hcl.colors(120, "YlGnBu", rev = TRUE), 
                                  limits = c(0, ceiling(lmax)),
                                  na.value = "gray90",
                                  name = guide_title) + 
    ggplot2::geom_segment(data = gridded_wind,
                          aes(x = lon, xend = lon + u / 20, y = lat, yend = lat + v / 20),
                          arrow = arrow(length = unit(0.12, "cm")),
                          lineend = "round",
                          linejoin = "mitre", 
                          col = "gray15", 
                          alpha = 0.8,
                          na.rm = TRUE) +
    ggplot2::geom_sf(data = shp_sf, 
                     fill = "gray90", 
                     color = "gray10", 
                     linewidth = 0.4, 
                     alpha = 0.7) + 
    ggplot2::coord_sf(xlim = xlim, ylim = ylim, expand = FALSE) +
    #metR::scale_x_longitude(ticks = 2) +
    scale_x_longitude(ticks = 3) +
    scale_y_latitude(ticks = 0.5) + 
    ggplot2::facet_wrap(~week_name, ncol = n_col, labeller = ggplot2::labeller(week_name = labeller_vector)) +
    ggplot2::labs(x = NULL, 
                  y = NULL,
      title = "Velocidad y dirección promedio del viento",
      subtitle = glue::glue("Periodo: del {format(start_date, '%d')} al {format(end_date, '%d')} de {stringr::str_to_sentence(format(end_date, '%B'))} de {format(start_date, '%Y')}"),
      caption = "Fuente: ERA5-Copernicus Climate Data Store\nhttps://cds.climate.copernicus.eu/") +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(axis.text = element_text(color = "gray30"),
          panel.grid = element_blank(),
          panel.spacing = unit(1.5, "lines"),
          plot.title = element_text(size = 16, face = "bold", color = "#222222"),
          plot.subtitle = element_text(size = 12, color = "#444444"),
          plot.caption = element_text(size = 9, color = "gray50", hjust = 1),
          strip.text = element_text(face = "bold", size = 11),
          legend.position = "bottom",
          legend.title = element_text(size = 11, face = "bold"),
          legend.text = element_text(size = 10),
          legend.key.width = unit(2, "cm"),
          legend.key.height = unit(0.4, "cm"),
          plot.background = element_rect(fill = "white", color = NA)) +
    ggplot2::guides(fill = guide_colorbar(title.position = "top", title.hjust = 0.5,barwidth = barwidth, barheight = 0.5))
  ggplot2::ggsave(plot = plot, filename = output_name, width = width, height = height, dpi = 300)
  
}
