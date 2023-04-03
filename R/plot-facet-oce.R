#' @title plot_facet_oce
#' @description Función para obtener imágenes del informe mensual para cada parametro oceanográfico (chlor_a, sst y rrs_645)
#' @param lists_nc lista de archivos nc de las variables
#' @param var string que indica el nombre de la variable a graficar
#' @param name_plot string que indica el nombre de la imagen de salida
#' @param height altura del plot
#' @param width ancho del plot
#' @param shp_file nombre del shp utilizado para la imagen final
#' @param start_time fecha de inicio del el plot
#' @param end_time fecha de termino del plot
#' @importFrom purrr map
#' @importFrom tidyr pivot_longer
#' @importFrom oce oce.colorsJet
#' @importFrom metR scale_x_longitude scale_y_latitude
#' @importFrom patchwork plot_annotation
#' @importFrom oce oce.colorsJet
#' @importFrom RColorBrewer brewer.pal
#' @importFrom raster raster stack as.data.frame
#' @importFrom fs path_file
#' @importFrom stringr str_replace str_remove
#' @import ggplot2
#' @import sf
#' @import dplyr
#' @import scales
#' @return imágenes png parámetros oceanográficos
#' @export plot_facet_oce
#' @examples
#' \dontrun{
#' shp_file <- "/media/evolecolab/PortableSSD/seguimiento_arauco_2022/oce_pars/Golfo_Arauco_prj2.shp"
#' height <- 5
#' width <- 14
#' lists_nc <- dir_ls(path = "/media/evolecolab/PortableSSD/seguimiento_arauco_2022/oce_pars/vars/input_plot/semana_20", regexp = ".nc$")
#' var <- "sst
#' start_time <- "2023-03-20"
#' end_time <- "2023-03-26"
#' name_plot <- "ocepars_plot_w20.png"
#' plot_facet_oce(lists_nc = lists_nc, var = var, name_plot = name_plot, height = height, width = width, shp_file = shp_file, start_time = start_time, end_time = end_time)
#' }
plot_facet_oce <- function(list_nc, var, name_plot, height, width, shp_file, start_time, end_time) {
  shp_sf <- st_read(shp_file) %>% st_geometry()
  # vars <- c("Rrs_645", "chlor_a", "sst")
  list_raster <- map(lists_nc, ~ raster::raster(., varname = var))
  names_raster <- fs::path_file(list_nc)
  names_raster <- stringr::str_replace(string = names_raster, pattern = "w_", replacement = "Semana ")
  pattern_del <- paste0("_", var, "_", "1km_L3mapped.nc")
  names_raster <- stringr::str_remove(string = names_raster, pattern = pattern_del)
  stack <- raster::stack(list_raster) %>% raster::as.data.frame(xy = TRUE)
  names(stack)[3:ncol(stack)] <- names_raster
  df_to_plot <- tidyr::pivot_longer(data = stack, cols = 3:ncol(stack), names_to = "week", values_to = "fill")
  data_sf <- sf::st_as_sf(df_to_plot, coords = c("x", "y"), crs = 4326)
  data_to_filter <- sf::st_intersects(data_sf, shp_sf)
  df_to_plot <- df_to_plot %>% dplyr::mutate("inside" = apply(data_to_filter, 1, any))
  bbox <- sf::st_bbox(shp_sf)
  data <- df_to_plot %>%
    dplyr::filter(inside == "FALSE") %>%
    dplyr::filter(dplyr::between(x, bbox[1], bbox[3])) %>%
    dplyr::filter(dplyr::between(y, bbox[2], bbox[4]))
  xlim <- c(bbox[1], bbox[3])
  ylim <- c(bbox[2], bbox[4])
  #### chlor_a
  if (var == "chlor_a") {
    plot <- ggplot(data) +
      geom_tile(aes(x, y, fill = log10(fill))) +
      scale_fill_gradientn(
        colours = oce::oce.colorsJet(120), na.value = "white",
        limits = c(-1, 2),
        oob = squish,
        breaks = c(-1, 0, 1, 2),
        labels = c(expression(10^-1), expression(10^0), expression(10^1), expression(10^2))
      ) +
      metR::scale_x_longitude(ticks = 0.1) +
      metR::scale_y_latitude(ticks = 0.1) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
      guides(fill = guide_colorbar(
        title = expression(paste("Log10", " ", Clorofila - alpha ~ (mg ~ m^{
          -3
        }))),
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = .5,
        barheight = 12,
        title.hjust = .5
      )) +
      theme_bw() +
      facet_wrap(~week, ncol = 3)
  }
  ### plot sst
  if (var == "sst") {
    blues <- rev(brewer.pal(9, "YlGnBu"))
    reds <- brewer.pal(9, "YlOrRd")
    plot <- ggplot(data) +
      geom_raster(aes(x, y, fill = fill)) +
      scale_fill_gradientn(
        colours = c(blues, reds), na.value = "white",
        n.breaks = 5
      ) +
      metR::scale_x_longitude(ticks = 0.1) +
      metR::scale_y_latitude(ticks = 0.1) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
      guides(fill = guide_colorbar(
        title = "Temperatura Superficial \n Mar (°C)",
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = .5,
        barheight = 12,
        title.hjust = .5
      )) +
      theme_bw() +
      facet_wrap(~week, ncol = 3)
  }
  if (var == "Rrs_645") {
    data <- data %>% dplyr::mutate(fill = fill * 158.9418)
    plot <- ggplot(data) +
      geom_raster(aes(x, y, fill = fill)) +
      scale_fill_gradientn(
        colours = oce.colorsJet(120), na.value = "white",
        n.breaks = 5
      ) +
      metR::scale_x_longitude(ticks = 0.1) +
      metR::scale_y_latitude(ticks = 0.1) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
      guides(fill = guide_colorbar(
        title = expression(paste(
          "nWLR 645",
          " (",
          "mW ",
          cm^-2,
          um^-1,
          sr^-1,
          ")"
        )),
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = .5,
        barheight = 12,
        title.hjust = .5
      )) +
      theme_bw() +
      facet_wrap(~week, ncol = 3)
  }
   plot_final <-  plot + plot_annotation(
    title = paste0("Parámetros oceanográficos: Golfo de Arauco: ", var),
    subtitle = paste0("Periodo :", start_time, " al ", end_time),
    caption = "Fuente:  OceanColor Data; \n
  https://oceancolor.gsfc.nasa.gov/cgi/browse.pl"
  )
  ggsave(filename = name_plot, plot = plot_final, device = "png", units = "in", dpi = 300, height = height, width = width)
  gc()
}
