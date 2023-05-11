#' @title weekly_wind_plot
#' @description Función para obtener imágenes png de magnitud y velocidad de viento para datos del ERAS5 (desde archivos csv) para n semanas
#' @param list_csv lista con los archivos csv para el plot
#' @param name_plot string que indica el nombre de la imagen de salida
#' @param height altura del plot
#' @param width ancho del plot
#' @param shp_file nombre del shp utilizado para la imagen final
#' @param start_time fecha de inicio del el plot
#' @param end_time fecha de termino del plot
#' @param ncol número de columnas para el plot
#' @importFrom readr read_csv
#' @importFrom lubridate as_date
#' @importFrom purrr map map2
#' @importFrom tibble as_tibble
#' @importFrom tidyr drop_na
#' @importFrom oce oce.colorsJet
#' @importFrom stringr str_replace str_remove
#' @importFrom metR scale_x_longitude scale_y_latitude
#' @importFrom readr read_csv
#' @import ggplot2
#' @import sf
#' @import dplyr
#' @return imágenes png de velocidad y magnitud de viento
#' @export  weekly_wind_plot
#' @examples
#' \dontrun{
#' list_csv <- "list/files/csv"
#' name_plot <- "wind_plot_semana_12_complete.png"
#' height <- 11
#' width <- 8
#' shp_file <- "/media/evolecolab/PortableSSD/seguimiento_arauco_2022/viento/Chile.shp"
#' start_time <- "2023-01-23"
#' end_time <- "2023-01-29"
#' ncol <- 2
#' weekly_wind_plot(list_csv = list_csv, name_plot = name_plot, height = height, width = width, shp_file = shp_file, start_time = start_time, end_time = end_time, ncol = ncol)
#' }
weekly_wind_plot <- function(list_csv, name_plot, shp_file, start_time, end_time, height, width, ncol) {
  sf::sf_use_s2(FALSE)
  names_csv <- fs::path_file(list_csv)
  names_csv <- stringr::str_replace(string = names_csv, pattern = "data_week_", replacement = "Semana ")
  file_ext <- c("_complete.csv", "_partial.csv")
  pattern_del <- paste(file_ext, collapse = "|")
  names_csv <- stringr::str_remove(string = names_csv, pattern = pattern_del)
  fn_format_csv <- function(csv_file, name_csv) {
    csv <- readr::read_csv(csv_file,show_col_types = FALSE) %>%
      dplyr::mutate(week = name_csv) %>%
      dplyr::group_by(lon, lat, week) %>%
      dplyr::mutate(week_name = name_csv) %>%
      dplyr::filter(dplyr::between(lon, -76, -70)) %>%
      dplyr::filter(dplyr::between(lat, -44, -34)) %>%
      dplyr::group_by(lon, lat, week_name) %>%
      dplyr::summarise(
        speed_mean = mean(speed_mean, na.rm = TRUE),
        dir = mean(dir,na.rm = TRUE),
        u = mean(u, na.rm = TRUE),
        v = mean(v, na.rm = TRUE)) %>%
      #dplyr::filter(dplyr::between(date, lubridate::as_date(start_time), lubridate::as_date(end_time))) %>%
      tidyr::drop_na()
    }
  #df_plot <- map2(list_csv, names_csv, ~fn_format_csv(csv_file = .x,name_csv = .y)) %>%
  df_plot <- map2(list_csv, names_csv, ~fn_format_csv(csv_file = .x, name_csv = .y)) %>%
    bind_rows()
  #df_plot$week <- factor(df_plot$week, levels = names_csv)
  #### spatial shit####
  df_sf <- df_plot %>%
    sf::st_as_sf(coords = c("lon", "lat")) %>%
    sf::st_set_crs(4326) %>%
    dplyr::group_by(week_name) %>%
    dplyr::group_split()
  grid <- df_sf[[1]] %>%
    sf::st_make_grid(n = c(150, 150)) %>%
    sf::st_sf() %>%
    dplyr::mutate(id = dplyr::row_number())
  #### fn to crearte grid: taken from some place..####
  fn_grid <- function(grid, data_sf) {
    grid_filled <- grid %>% dplyr::mutate(
      id = dplyr::n(),
      contained = lapply(sf::st_contains(sf::st_sf(geometry), data_sf), identity),
      obs = sapply(contained, length),
      week_name = unique(data_sf$week_name),
      u = sapply(contained, function(x) {
        mean(data_sf[x, ]$u, na.rm = TRUE)
      }),
      v = sapply(contained, function(x) {
        mean(data_sf[x, ]$v, na.rm = TRUE)
      }),
      speed_mean = sapply(contained, function(x) {
        mean(data_sf[x, ]$speed_mean, na.rm = TRUE)
      })
    )
    grid_filled
  }
  grid_filled <- purrr::map(df_sf, ~ fn_grid(grid = grid, data_sf = .))
  grid_filled <- dplyr::bind_rows(grid_filled) %>%
    dplyr::select(obs, u, v, speed_mean, week_name)
  grid_points <- grid_filled %>%
    sf::st_centroid() %>%
    sf::st_coordinates() %>%
    tibble::as_tibble() %>%
    dplyr::rename(lon = X, lat = Y)
  attributes <- grid_filled
  ## remove the geometry and remain with the attributes
  sf::st_geometry(attributes) <- NULL
  gridded_wind <- grid_points %>%
    dplyr::bind_cols(attributes)
  chile <- sf::st_read(shp_file, quiet = TRUE)
  zona_sur <- chile %>% dplyr::filter(codregion %in% c(7, 8, 9, 16))
  lmax <- max(ceiling(df_plot$speed_mean))
  weekly_plot <- ggplot(data = df_plot) +
    geom_raster(aes(x = lon, y = lat, fill = speed_mean)) +
    scale_fill_gradientn(
      limits = c(0, ceiling(lmax)),
      colours = alpha(oce::oce.colorsJet(120), 0.7),
      na.value = "white",
      name = "Velocidad promedio (m/s)"
    ) +
    geom_sf(data = zona_sur, col = "white", fill = "gray70", lwd = 1) +
    coord_sf(xlim = c(-76, -70.5), ylim = c(-36.4, -38.7)) +
    geom_segment(
      data = gridded_wind,
      aes(x = lon, xend = lon + u / 20, y = lat, yend = lat + v / 20),
      arrow = arrow(length = unit(0.1, "cm")),
      lineend = "round",
      linejoin = "mitre",
      col = "black",
      na.rm = TRUE
    ) +
    metR::scale_x_longitude(ticks = 2) +
    metR::scale_y_latitude(ticks = 0.5) +
    theme_bw() +
    guides(fill = guide_colourbar(
      barwidth = 10,
      barheight = 0.5,
      title.position = "top"
    )) +
    theme(
      axis.text = element_text(size = 11, colour = "black"),
      panel.spacing = unit(1, "lines"),
      legend.position = "bottom",
      legend.box = "vertical",
    ) +
    labs(
      x = NULL, y = NULL,
      title = "Velocidad y dirección promedio del viento: VIII Región",
      subtitle = paste0(
        "Periodo :", start_time, " al ", end_time, "\n",
        "presión: 1000hPa"
      ),
      caption = "Fuente: ERA5 hourly data on pressure levels from 1959 to present; \n
          https://cds.climate.copernicus.eu/"
    ) +
    facet_wrap(~week_name, ncol = ncol)
  ggsave(filename = name_plot, plot = weekly_plot, device = "png", units = "in", dpi = 300, height = height, width = width)
  cat("Listo... \n\n")
}

