#' @title get_clim
#' @description Función para generar climatología y exportarla (formato png) para una variable determinada
#' @param dir_input directorio en donde se almacenan las imágenes raster (formato .tif)
#' @param dir_output directorio en donde se almacenará la imagen png
#' @param season temporalidad para la generación de imágenes en formato raster("mes", o "year")
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean")
#' @param shp_file nombre archivo shp para el gráfico (si no esta en dir_input poner nombre con ruta completa)
#' @param n_col numero de columnas para el gráfico
#' @param n_row numero de filas para el gráfico
#' @param height altura imagen de salida
#' @param width ancho tamyear de salida
#' @import ggplot2
#' @importFrom fs dir_ls dir_create dir_exists
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr mutate distinct pull last_col across group_split group_by
#' @importFrom purrr map
#' @importFrom terra writeRaster rast
#' @importFrom raster raster stack calc rasterToPoints
#' @importFrom tidyr separate pivot_longer
#' @importFrom sf read_sf st_geometry
#' @importFrom oce oce.colorsJet oce.colorsViridis
#' @importFrom metR scale_x_longitude scale_y_latitude
#' @importFrom RColorBrewer brewer.pal
#' @importFrom stringr str_remove str_to_sentence
#' @return imágenes png climatologías raster Rdata
#' @export get_clim
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/test_satImg/raster_mensuales/resultados_raster/"
#' dir_output <- "/home/evolecolab/Escritorio/test_satImg/climatologia"
#' season <- "mes"
#' raster_function <- "median"
#' var_name <- "chlor_a"
#' n_col <- 3
#' n_row <- 4
#' name_output <- "climatologia_chlor_a.png"
#' shp_file <- "/home/evolecolab/Escritorio/test_satImg/raster_mensuales/resultados_raster/Golfo_Arauco_prj2.shp"
#' res <- 300
#' heigth <- 7
#' width <- 9
#' get_clim(dir_input = dir_input, dir_output = dir_output, season = season, raster_function = raster_function, var_name = var_name, shp_file = shp_file, n_col = n_col, n_row = n_row, name_output = name_output, res = res, heigth = heigth, width = width)
#' }
get_clim <- function(dir_input, dir_output, season, raster_function, var_name, shp_file, n_col,
                     n_row, name_output, res, heigth, width) {
  all_tif <- tibble(
    full_path = dir_ls(path = dir_input, regexp = ".tif$", recurse = T),
    archivo = basename(full_path)
  ) %>%
    separate(archivo,
      into = c("year", "month_num", "month", "var_name"), sep = "_",
      remove = FALSE, extra = "drop"
    )
  if (season == "year") {
    all_tif_split <- all_tif %>%
      group_by(year) %>%
      group_split()
    names <- all_tif %>%
      distinct(year) %>%
      pull(year)
    names(all_tif_split) <- names
  }
  if (season == "month") {
    all_tif_split <- all_tif %>%
      group_by(month_num) %>%
      group_split()
    names <- all_tif %>%
      distinct(month) %>%
      pull(month)
    names(all_tif_split) <- names
  }
  # if (season == "semana") {
  #  all_tif_split <- all_tif %>% group_by(semana) %>% group_split()
  #  names <- all_tif %>% distinct(semana) %>% pull(semana)
  #  names(all_tif_split) <- names
  # }
  cat("\n\n Calculando climatologías...\n\n")
  stack_list <- map(all_tif_split, ~ stack(.$full_path))
  if (raster_function == "median") {
    stack <- map(stack_list, ~ calc(., fun = median, na.rm = TRUE))
    stack <- stack(stack)
  }
  if (raster_function == "mean") {
    stack <- map(stack_list, ~ calc(., fun = mean, na.rm = TRUE))
    stack <- stack(stack)
  }
  # plot climatologia
  # config gral
  shp <- read_sf(shp_file) %>% st_geometry()
  df <- stack %>%
    rasterToPoints() %>%
    as_tibble() %>%
    pivot_longer(cols = 3:last_col(), names_to = "facet_var", values_to = "valor") %>%
    mutate(facet_var = str_remove(facet_var, pattern = "X"), facet_var = str_to_sentence(facet_var)) %>%
    mutate(across(facet_var, factor, levels = str_to_sentence(names(stack))))
  cat("\n\n Generando gráfico...\n\n")
  if (var_name == "chlor_a") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = log10(valor))) +
      scale_fill_gradientn(colours = oce::oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(
        title = expression(paste("Log10", " ", Clorofila - alpha ~ (mg ~ m^{
          -3
        }))),
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = unit(.5, "cm"), barheight = unit(8.5, "cm"), title.hjust = .5
      )) +
      theme_bw()
    # para tratar de poner eje y en formato 10^.x
    # scale_fill_continuous(labels = trans_format("log", math_format(10^.x)))
  }
  if (var_name == "sst") {
    blues <- rev(brewer.pal(9, "YlGnBu"))
    reds <- brewer.pal(9, "YlOrRd")
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = valor)) +
      scale_fill_gradientn(colours = c(blues, reds), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(
        title = "Temperatura Superficial Mar (°C)",
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5
      )) +
      theme_bw()
  }
  if (var_name == "Rrs_645") {
    df <- df %>% mutate(valor_corrected = valor * 158.9418)
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = valor_corrected)) +
      scale_fill_gradientn(colours = oce::oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(
        title = expression(paste(
          "nWLR 645",
          " (",
          "mW ",
          cm^-2,
          um^-1, sr^-1,
          ")"
        )),
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5
      )) +
      theme_bw()
  }
  if (var_name == "nflh") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = valor)) +
      scale_fill_gradientn(colours = oce::oce.colorsViridis(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(
        title = expression(paste(
          "nLFH",
          " (",
          "mW ",
          cm^-2,
          um^-1,
          sr^-1,
          ")"
        )),
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5
      )) +
      theme_bw()
  }
  cat("\n\n Exportando resultados...\n\n")
  ifelse(!dir_exists(dir_output), dir_create(dir_output), FALSE)
  ggsave(filename = paste0(dir_output, "/", name_output), plot = plot, device = "png", units = "in", dpi = res, height = heigth, width = width)
  stack <- rast(stack)
  name_month <- paste0(sprintf("%02d", seq(1, 12)), "_", names(stack))
  writeRaster(x = stack, filename = paste0(dir_output, "/", "raster_climatologia_", var_name, ".tif"), overwrite = TRUE)
  writeRaster(x = stack, filename = paste0(dir_output, "/", name_month, "_", var_name, ".tif"), overwrite = TRUE)
  save(df, plot, file = paste0(dir_output, "/", "plot_data_", var_name, ".RData"))
}
