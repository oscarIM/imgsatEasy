#' Genera climatología oceanográfica y la exporta como imagen PNG
#'
#' Construye un gráfico de facetas con la climatología de una variable
#' oceanográfica a partir de un data frame ya procesado, y lo exporta como
#' archivo PNG (y opcionalmente como objeto RDS para ediciones posteriores).
#'
#' @param data_plot Data frame procesado con columnas `lat`, `lon`, `fill`
#'   (valores de la variable) y `season` (factor con etiquetas de periodo).
#' @param var_name Nombre de la variable a graficar. Valores aceptados:
#'   `"chlor_a"`, `"sst"`, `"Rrs_645"`, `"Kd490"`.
#' @param sensor Nombre del sensor para el caption del gráfico. Valores
#'   aceptados: `"aqua"`, `"terra"`, `"modis_aq"`, `"sentinel3A"`,
#'   `"sentinel3B"`, `"sentinelAB"`.
#' @param zona String que identifica la zona de estudio (se incluye en el
#'   título del gráfico, e.g. `"Golfo de Arauco"`).
#' @param start_date Fecha de inicio del periodo analizado (`"YYYY-MM-DD"`).
#' @param end_date Fecha de fin del periodo analizado (`"YYYY-MM-DD"`).
#' @param shp Objeto `sf` con la geometría costera superpuesta al raster. Si
#'   es `NULL`, se descarga automáticamente el contorno de Chile desde
#'   [rnaturalearth::ne_countries()].
#' @param xlim Vector numérico de longitud 2 con los límites en x del mapa
#'   en grados decimales: `c(oeste, este)`.
#' @param ylim Vector numérico de longitud 2 con los límites en y del mapa
#'   en grados decimales: `c(norte, sur)`.
#' @param ticks_x Separación entre ticks del eje x en grados. Default `0.1`.
#' @param ticks_y Separación entre ticks del eje y en grados. Default `0.1`.
#' @param n_col Número de columnas del panel de facetas (`facet_wrap`).
#' @param output_file Nombre y ruta del archivo PNG de salida
#'   (e.g. `"output/chlor_aqua_junin.png"`).
#' @param height Altura de la imagen exportada en pulgadas. Default `8`.
#' @param width Ancho de la imagen exportada en pulgadas. Default `6`.
#' @param save_plot_obj Lógico. Si `TRUE`, exporta el objeto `ggplot` como
#'   archivo `.rds` con el mismo nombre que `output_file`. Default `TRUE`.
#'
#' @return Invisiblemente el objeto `ggplot` generado. Como efecto secundario
#'   escribe en disco el PNG y, opcionalmente, el RDS.
#'
#' @details
#' La función ejecuta los siguientes pasos:
#' \enumerate{
#'   \item Valida entradas y prepara la geometría costera (`shp`).
#'   \item Construye los textos del gráfico: título, subtítulo, caption y
#'     título de la barra de color, en función de `var_name`, `sensor` y
#'     el periodo definido por `start_date` / `end_date`.
#'   \item Determina la escala de color: logarítmica para `chlor_a`, lineal
#'     para el resto; calcula `breaks` y `labels` automáticamente.
#'   \item Construye el objeto `ggplot` con capas de raster (`geom_tile`),
#'     geometría costera (`geom_sf`), ejes geográficos y panel de facetas.
#'   \item Exporta el PNG con [ggplot2::ggsave()] y, si `save_plot_obj = TRUE`,
#'     serializa el objeto como RDS.
#' }
#'
#' @note Si `shp` es `NULL` se requiere conexión a internet y el paquete
#'   `rnaturalearth` instalado para descargar el contorno de Chile.
#'
#' @seealso [ggplot2::facet_wrap()], [ggplot2::scale_fill_gradientn()],
#'   [rnaturalearth::ne_countries()]
#'
#' @importFrom dplyr filter
#' @importFrom lubridate year
#' @importFrom glue glue
#' @importFrom stringr str_to_sentence str_replace
#' @importFrom scales log_breaks
#' @importFrom grid convertWidth stringWidth
#' @importFrom sf sf_use_s2 st_bbox st_crs st_geometry st_crop
#' @importFrom cli cli_inform
#' @importFrom magrittr %>%
#' @import ggplot2
#'
#' @export
#'
#' @examples
#' \dontrun{
#' plot_clim(
#'   data_plot = data_clean,
#'   var_name = var_name,
#'   sensor = sensor,
#'   zona = zona,
#'   start_date = start_clim,
#'   end_date = end_clim,
#'   shp = shp_mask,
#'   xlim = xlim,
#'   ylim = ylim,
#'   ticks_x = ticks_x,
#'   ticks_y = ticks_y,
#'   n_col = 4,
#'   output_file = glue::glue("sst_GA_{periodo_clim}_{sensor}_clean.png")
#' )
#' }
plot_clim <- function(
    data_plot,
    var_name,
    sensor,
    zona,
    start_date,
    end_date,
    shp = NULL,
    xlim,
    ylim,
    ticks_x = 0.1,
    ticks_y = 0.1,
    n_col,
    output_file,
    height = 8,
    width = 6,
    save_plot_obj = TRUE) {
  # 1. Validar entradas y preparar geometría costera -------------------------
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  sf::sf_use_s2(FALSE)

  if (is.null(shp)) {
    if (!requireNamespace("rnaturalearth", quietly = TRUE)) {
      stop("El paquete 'rnaturalearth' no está instalado.")
    }
    shp <- rnaturalearth::ne_countries(scale = 10, returnclass = "sf") %>%
      dplyr::filter(admin == "Chile") %>%
      sf::st_geometry()
    bbox <- sf::st_bbox(
      c(xmin = xlim[2], xmax = xlim[1], ymin = ylim[2], ymax = ylim[1]),
      crs = sf::st_crs(shp)
    )
    shp <- suppressWarnings(sf::st_crop(shp, bbox))
  }

  # 2. Construir textos del gráfico ------------------------------------------
  titulo_var <- switch(var_name,
    "chlor_a" = "Concentraci\u00f3n de clorofila-a en",
    "sst" = "Temperatura Superficial del Mar en",
    "Rrs_645" = "Radiaci\u00f3n normalizada de salida del agua (645nm) en",
    "Kd490" = "Coeficiente de atenuaci\u00f3n difusa (490 nm) en",
    stop(glue::glue("var_name '{var_name}' no reconocida."))
  )
  title_plot <- glue::glue("{titulo_var} {zona}")

  years <- seq(lubridate::year(start_date), lubridate::year(end_date))
  subtitle <- if (length(years) == 1) {
    glue::glue(
      "Periodo: del {format(start_date, '%d')} de ",
      "{stringr::str_to_sentence(format(start_date, '%B'))} al ",
      "{format(end_date, '%d')} de ",
      "{stringr::str_to_sentence(format(end_date, '%B'))} de {years}"
    )
  } else {
    glue::glue("Periodo: {years[1]} \u2013 {years[length(years)]}")
  }

  caption <- switch(sensor,
    "aqua" = "Fuente: OceanColor Data; Sensor Modis-Aqua",
    "terra" = "Fuente: OceanColor Data; Sensor Modis-Terra",
    "modis_aq" = "Fuente: OceanColor Data; Combined Aqua-Terra satellites",
    "sentinel3A" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3A",
    "sentinel3B" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3B",
    "sentinelAB" = "Fuente: OceanColor Data; Combined Sentinel3A-Sentinel3B satellites",
    "Sensor desconocido"
  )

  guide_title <- switch(var_name,
    "chlor_a" = expression("Clorofila-\u03b1 [mg" ~ m^
      {
        -3
      } * "]"),
    "sst" = "Temperatura Superficial Mar [\u00b0C]",
    "Rrs_645" = expression("nWLR 645 [mW" ~ cm^{
      -2
    } ~ mu * m^{
      -1
    } ~ sr^
      {
        -1
      } * "]"),
    "Kd490" = expression("Coef. atenuaci\u00f3n difusa 490 nm [m"^
      {
        -1
      } * "]")
  )

  # 3. Definir escala de color y breaks --------------------------------------
  if (var_name == "Rrs_645") {
    valid_range <- c(0, max(data_plot$fill, na.rm = TRUE))
    step <- signif(diff(valid_range) / 5, digits = 1)
    breaks_color <- seq(valid_range[1], valid_range[2], by = step)
    labels_color <- formatC(breaks_color, format = "f", digits = 2)
    use_log10 <- FALSE
  } else if (var_name == "chlor_a") {
    valid_range <- range(data_plot$fill[data_plot$fill > 0], na.rm = TRUE)
    breaks_color <- scales::log_breaks()(valid_range)
    labels_color <- formatC(breaks_color, format = "f", digits = 1)
    use_log10 <- TRUE
  } else {
    valid_range <- range(data_plot$fill, na.rm = TRUE)
    breaks_color <- pretty(valid_range, 4)
    labels_color <- waiver()
    use_log10 <- FALSE
  }

  cols <- if (var_name == "sst") {
    c(get_palette("blues"), get_palette("reds"))
  } else {
    get_palette("oce_jets")
  }

  # 4. Construir objeto ggplot -----------------------------------------------
  plot_obj <- ggplot2::ggplot(data = data_plot) +
    ggplot2::geom_tile(
      ggplot2::aes(x = lon, y = lat, fill = fill)
    ) +
    ggplot2::scale_fill_gradientn(
      colours = cols,
      na.value = "white",
      breaks = breaks_color,
      labels = labels_color,
      transform = if (use_log10) "log10" else "identity"
    ) +
    scale_x_longitude(ticks = ticks_x) +
    scale_y_latitude(ticks = ticks_y) +
    ggplot2::geom_sf(data = shp, fill = "grey80", col = "black") +
    ggplot2::coord_sf(xlim = xlim, ylim = ylim, expand = FALSE, clip = "on") +
    ggplot2::guides(
      fill = ggplot2::guide_colorbar(
        title = guide_title,
        title.position = "top",
        title.hjust = 0.5,
        barwidth = grid::convertWidth(
          grid::stringWidth(guide_title),
          unitTo = "lines",
          valueOnly = TRUE
        ) * 1.5 + n_col,
        barheight = 0.8
      )
    ) +
    ggplot2::labs(
      title = title_plot,
      subtitle = subtitle,
      caption = caption,
      x = NULL,
      y = NULL
    ) +
    ggplot2::facet_wrap(~season, ncol = n_col) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      axis.text = ggplot2::element_text(color = "gray30"),
      panel.grid = ggplot2::element_blank(),
      panel.spacing = ggplot2::unit(1.5, "lines"),
      plot.title = ggplot2::element_text(size = 12, face = "bold", color = "#222222"),
      plot.subtitle = ggplot2::element_text(size = 10, color = "#444444"),
      plot.caption = ggplot2::element_text(size = 9, color = "gray50", hjust = 1),
      strip.text = ggplot2::element_text(face = "bold", size = 11),
      legend.position = "bottom",
      legend.title = ggplot2::element_text(size = 10, face = "bold"),
      legend.text = ggplot2::element_text(size = 10),
      legend.key.width = ggplot2::unit(2, "cm"),
      legend.key.height = ggplot2::unit(0.4, "cm"),
      plot.background = ggplot2::element_rect(fill = "white", color = NA),
      plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = 5),
      plot.title.position = "plot"
    )

  # 5. Exportar PNG y RDS ----------------------------------------------------
  ggplot2::ggsave(
    filename = output_file,
    plot = plot_obj,
    width = width,
    height = height,
    dpi = 300
  )
  cli::cli_inform("\u2705 Gr\u00e1fico PNG guardado en: {output_file}")

  if (save_plot_obj) {
    rds_output <- stringr::str_replace(output_file, "\\.png$", ".rds")
    saveRDS(plot_obj, file = rds_output)
    cli::cli_inform("\u2705 Objeto RDS guardado en: {rds_output}")
  }

  return(invisible(plot_obj))
}
