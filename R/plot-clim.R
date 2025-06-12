#' @title plot_clim
#' @description Función para generar climatología y exportarla (formato png) para una variable determinada
#' @param dir_input directorio en donde se almacenan las tablas en csv o parquet
#' @param season temporalidad para la generación de imágenes en formato raster("mes", o "year")
#' @param stat_function función estadística para generar las imágenes raster ("median" o "mean")
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param shp_file nombre archivo shp para el gráfico (si no esta en dir_input poner nombre con ruta completa). Opcional
#' @param n_col numero de columnas para el gráfico
#' @param name_output nombre para la salidas
#' @param res resolución para la imágen png de la climatología
#' @param height altura para la imágen png de la climatología
#' @param width amplitud para la imágen png de la climatología
#' @param xlim vector numérico tamaño 2 para los limites de x en el plot
#' @param ylim vector numérico tamaño 2 para los limites de y en el plot
#' @param ticks_x vector numérico tamaño 1 indicando ticks en el eje x del plot
#' @param ticks_y vector numérico tamaño 1 indicando ticks en el eje y del plot
#' @param n_cores cores a usar
#' @param sensor nombre del sensor (solo sirve para la leyenda)
#' @param save_data opcion boleana
#' @param from_data opcion boleana
#' @param data_plot_file nombre de archivo
#' @param start_date fecha inicio (formato "YYYYY-MM-DD")
#' @param end_date fecha final
#' @param zona string que define la zona de estudio
#' @importFrom arrow read_parquet
#' @importFrom dplyr all_of across between bind_rows group_by group_split cur_group_id filter first mutate pull select summarise tibble ungroup reframe case_when collect
#' @importFrom lubridate month year isoweek as_date
#' @importFrom furrr future_walk furrr_options
#' @importFrom future plan
#' @importFrom parallel makeForkCluster stopCluster
#' @importFrom progressr progressor with_progress
#' @importFrom purrr map map2 map_chr
#' @importFrom rlang !! sym :=
#' @importFrom glue glue
#' @importFrom grid convertWidth stringWidth
#' @importFrom scales pretty_breaks
#' @importFrom sf sf_use_s2 read_sf st_as_sf st_bbox st_geometry st_crs st_crop
#' @importFrom stringr str_extract str_to_sentence str_to_title str_detect
#' @importFrom tidyr separate_wider_delim
#' @importFrom tibble deframe tibble
#' @importFrom magrittr %>%
#' @importFrom readr read_csv write_csv
#' @importFrom arrow read_parquet
#' @import ggplot2
#' @return imágenes png
#' @export plot_clim
#' @examples
#' \dontrun{
#' dir_input <- "/home/holon--oim/Dropbox/HolonSPA/PROYECTOS/Proyecto_Junin/Analisis/satelital/aqua/chlor_a/parquet_files"
#' season <- "month"
#' stat_function <- "median"
#' var_name <- "chlor_a"
#' #shp_file <- "/home/holon--oim/Dropbox/HolonSPA/PROYECTOS/Proyecto_Junin/Analisis/satelital/zona_proyecto_junin.shp"
#' n_col <- 4
#' name_output <- "chlor_aqua_proyecto_junin.png"
#' ticks_x = 0.1
#' ticks_y = 0.15
#' n_cores = 16
#' xlim <- c(west, east)
#' ylim <- c(north, south)
#' sensor = "aqua"
#' plot_clim(dir_input = dir_input,season = season, stat_function = stat_function,var_name = var_name,n_col = n_col,name_output = name_output,res = 300,height = 12,width = 9,ticks_x = ticks_x, ticks_y = ticks_y,n_cores = n_cores,xlim = xlim, ylim = ylim,save_data = FALSE,from_data = TRUE,data_plot_file = "chlor_aqua_proyecto_junin.csv", sensor = sensor)
#' }

plot_clim <- function(dir_input = NULL, season, stat_function, var_name, shp_file = NULL, start_date = NULL, end_date = NULL, n_col, name_output, height = 8, width = 6, ticks_x = 0.1, ticks_y = 0.1, n_cores = 1, xlim, ylim, save_data = TRUE, from_data = FALSE, data_plot_file = NULL, sensor, zona) {
  tic()
  sf::sf_use_s2(FALSE)
  if (is.null(shp_file)) {
    if (!requireNamespace("rnaturalearth", quietly = TRUE)) stop("El paquete 'rnaturalearth' no está instalado.")
    shp_sf <- rnaturalearth::ne_countries(scale = 10, returnclass = "sf") %>%
      dplyr::filter(admin == "Chile") %>% sf::st_geometry()
    bbox <- sf::st_bbox(c(xmin = xlim[2], xmax = xlim[1], ymin = ylim[2], ymax = ylim[1]), crs = sf::st_crs(shp_sf))
    shp_sf <- suppressWarnings(sf::st_crop(shp_sf, bbox))
  } else {
    shp_sf <- sf::read_sf(shp_file) %>% sf::st_geometry()
  }
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  func <- match.fun(stat_function)

  if (!from_data) {
    files_ext_pattern <- paste(c(".parquet$", ".csv$", ".tif$"), collapse = "|")
    all_files_tmp <- list.files(path = dir_input, full.names = TRUE, pattern = files_ext_pattern) %>%
      tibble::tibble(file = .) %>%
      dplyr::mutate(filename = basename(file),
                    date_str = stringr::str_extract(filename, "^(\\d{4}-\\d{2}-\\d{1,2}|\\d{4}-\\d{2})"),
                    has_day = stringr::str_detect(date_str, "^\\d{4}-\\d{2}-\\d{1,2}"),
                    date = as.Date(ifelse(has_day, date_str, paste0(date_str, "-01"))),
                    month = lubridate::month(date),
                    year = lubridate::year(date),
                    week = lubridate::isoweek(date)) %>%
      dplyr::filter(is.null(start_date) | is.null(end_date) | dplyr::between(date, start_date, end_date)) %>%
      dplyr::mutate(season = dplyr::case_when(
        season == "week" ~ week,
        season == "month" ~ month,
        TRUE ~ year
      ))

    path_list <- all_files_tmp %>%
      dplyr::group_by(season) %>%
      dplyr::group_split() %>%
      purrr::map(~ dplyr::pull(.x, "file"))

    ext_file <- unique(stringr::str_extract(all_files_tmp$file, files_ext_pattern))

    process_tables <- function(file) {
      dataframe <- switch(ext_file,
                          ".parquet" = arrow::read_parquet(file, as_data_frame = FALSE),
                          ".csv" = readr::read_csv(file, show_col_types = FALSE, progress = FALSE))
      dataframe %>%
        dplyr::filter(!is.na(!!rlang::sym(var_name))) %>%
        dplyr::filter(dplyr::between(lon, xlim[1], xlim[2])) %>%
        dplyr::filter(dplyr::between(lat, ylim[2], ylim[1])) %>%
        dplyr::select(lat, lon, date1, !!rlang::sym(var_name)) %>%
        dplyr::collect()
    }

    process_sublist <- function(entry_list) {
      if (n_cores <= 1) {
        progressr::with_progress({
          p <- progressr::progressor(steps = length(entry_list))
          purrr::map(entry_list, ~ { p()
            #Sys.sleep(0.1)
            process_tables(.x) })
        })
      } else {
        cl <- parallel::makeForkCluster(n_cores)
        on.exit(parallel::stopCluster(cl))
        future::plan("cluster", workers = cl)
        progressr::with_progress({
          p <- progressr::progressor(steps = length(entry_list))
          furrr::future_map(entry_list, ~ { p()
            Sys.sleep(0.1)
            process_tables(.x) },
            .options = furrr::furrr_options(seed = TRUE, packages = c("dplyr", "readr", "arrow")))
        })
      }
    }

    data_plot <- purrr::imap(path_list, ~ {
      msg <- switch(season,
                    "month" = glue::glue("Procesando mes: {month.name[as.integer(.y)]}"),
                    "week"  = glue::glue("Procesando semana: {.y}"),
                    "year"  = glue::glue("Procesando año: {.y}"),
                    glue::glue("Procesando: {.y}")
      )
      message(msg)
      sub_data_list <- process_sublist(.x)
      bind_rows(sub_data_list) %>%
        dplyr::group_by(lat, lon, date1) %>%
        dplyr::summarise(fill = func(!!sym(var_name), na.rm = TRUE), .groups = "drop") %>%
        dplyr::mutate(season = dplyr::case_when(
          season == "week"  ~ as.character(glue::glue("Semana {lubridate::isoweek(date1)}")),
          season == "month" ~ as.character(lubridate::month(date1)),
          TRUE              ~ as.character(.y)))
    }) %>%
      dplyr::bind_rows()

    if (season == "month") {
      data_plot <- data_plot %>%
        dplyr::mutate(season = month(as.integer(season), label = TRUE, abbr = FALSE),
                      season = stringr::str_to_title(season),
                      season = factor(season, levels = stringr::str_to_title(month(1:12, label = TRUE, abbr = FALSE))))
    } else if (season == "year") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season))
    } else if (season == "week") {
      data_plot <- data_plot %>% dplyr::mutate(date = as.Date(date1))
      all_dates <- seq(start_date, end_date, by = "day")
      labeller_vector <- tibble::tibble(date = all_dates) %>%
        dplyr::mutate(
          year = lubridate::year(date),
          month = lubridate::month(date),
          week = lubridate::isoweek(date),
          season = glue::glue("Semana {week}")
        ) %>%
        dplyr::group_by(year, month, season) %>%
        dplyr::summarise(start = min(date), end = max(date), .groups = "drop") %>%
        dplyr::mutate(label = glue::glue("{season} ({format(start, '%d')}–{format(end, '%d %b')})")) %>%
        dplyr::select(season, label) %>%
        tibble::deframe()
    }
  } else {
    data_plot <- readr::read_csv(data_plot_file, show_col_types = FALSE)
    if (season == "month") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season, levels = c("Enero", "Febrero", "Marzo", "Abril","Mayo","Junio","Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")))
    } else if (season == "year") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season))
    } else if (season == "week") {
      data_plot <- data_plot %>%
        dplyr::mutate(season = glue::glue("Semana {season}"))
      all_dates <- seq(start_date, end_date, by = "day")
      labeller_vector <- tibble::tibble(date = all_dates) %>%
        dplyr::mutate(week = lubridate::isoweek(date),
                      season = glue::glue("Semana {week}")) %>%
        dplyr::group_by(season) %>%
        dplyr::summarise(start = min(date), end = max(date), .groups = "drop") %>%
        dplyr::mutate(label = glue::glue("{season} ({format(start, '%d')}–{format(end, '%d %b')})")) %>%
        dplyr::select(season, label) %>%
        tibble::deframe()
    }
  }

  title_plot <- switch(var_name,
                       "chlor_a" = "Concentración de clorofila-a en",
                       "sst" = "Temperatura Superficial del Mar en",
                       "Rrs_645" = "Radiación normalizada de salida del agua (645nm) en")
  title_plot <- glue::glue("{title_plot} {zona}")

  years <- seq(lubridate::year(start_date), lubridate::year(end_date))
  subtitle <- if (length(years) == 1) {
    glue::glue("Periodo: del {format(start_date, '%d')} de {stringr::str_to_sentence(format(start_date, '%B'))} al {format(end_date, '%d')} de {stringr::str_to_sentence(format(end_date, '%B'))} de {years}")
  } else {
    glue::glue("Periodo: {years[1]} – {years[length(years)]}")
  }

  caption <- switch(sensor,
                    "aqua" = "Fuente: OceanColor Data; Sensor Modis-Aqua",
                    "terra" = "Fuente: OceanColor Data; Sensor Modis-Terra",
                    "modis_aq" = "Fuente: OceanColor Data; Combined Aqua-Terra satellites",
                    "sentinel3A" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3A",
                    "sentinel3B" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3B",
                    "sentinelAB" = "Fuente: OceanColor Data; Combined Sentinel3A-Sentinel3B satellites",
                    default = "Sensor desconocido")

  guide_title <- switch(var_name,
                        "chlor_a" = expression("Clorofila-α [mg"~m^{-3}*"]"),
                        "sst" = "Temperatura Superficial Mar [°C]",
                        "Rrs_645" = expression("nWLR 645 [mW" ~ cm^{-2} ~ mu*m^{-1} ~ sr^{-1}*"]"))

  if (var_name == "Rrs_645") {
    use_log10 <- FALSE
    #data_plot <- data_plot %>%
    #  dplyr::mutate(fill = fill * 158.9418,
    #                fill = dplyr::if_else(condition = fill<0,true = 0, false = fill))

    valid_range <- c(0, max(data_plot$fill, na.rm = TRUE))
    range_span <- diff(valid_range)
    step <- signif(range_span / 5, digits = 1)
    breaks <- seq(valid_range[1], valid_range[2], by = step)
    labels <- formatC(breaks, format = "f", digits = 2)

  } else if (var_name == "chlor_a") {
    use_log10 <- TRUE
    valid_range <- range(data_plot$fill[data_plot$fill > 0], na.rm = TRUE)
    breaks <- scales::log_breaks()(valid_range)
    labels <- formatC(breaks, format = "f", digits = 1)

  } else {
    use_log10 <- FALSE
    valid_range <- range(data_plot$fill, na.rm = TRUE)
    breaks <- pretty(valid_range, 4)
    labels <- waiver()
  }
  cols <- if (var_name == "sst") c(get_palette("blues"), get_palette("reds")) else get_palette("oce_jets")

  plot <- ggplot2::ggplot(data = data_plot) +
    ggplot2::geom_tile(aes(x = lon, y = lat, fill = fill)) +
    ggplot2::scale_fill_gradientn(
      colours = cols,
      na.value = "white",
      breaks = breaks,
      labels = labels,
      #limits = c(0,1),
      transform = if (use_log10) "log10" else "identity"
    ) +
    scale_x_longitude(ticks = ticks_x) +
    scale_y_latitude(ticks = ticks_y) +
    ggplot2::geom_sf(data = shp_sf, fill = "grey80", col = "black") +
    ggplot2::coord_sf(xlim = xlim, ylim = ylim, expand = FALSE, clip = "on") +
    ggplot2::guides(fill = guide_colorbar(
      title = guide_title,
      title.position = "top",
      barwidth = grid::convertWidth(grid::stringWidth(guide_title), unitTo = "lines", valueOnly = TRUE) * 1.5 + n_col,
      barheight = 0.8,
      title.hjust = 0.5
    )) +
    ggplot2::labs(title = title_plot, subtitle = subtitle, caption = caption, x = NULL, y = NULL) +
    ggplot2::facet_wrap(~season, ncol = n_col) +
    ggplot2::theme_minimal(base_size = 11) +
    ggplot2::theme(
      axis.text = element_text(color = "gray30"),
      panel.grid = element_blank(),
      panel.spacing = unit(1.5, "lines"),
      plot.title = element_text(size = 12, face = "bold", color = "#222222"),
      plot.subtitle = element_text(size = 10, color = "#444444"),
      plot.caption = element_text(size = 9, color = "gray50", hjust = 1),
      strip.text = element_text(face = "bold", size = 11),
      legend.position = "bottom",
      legend.title = element_text(size = 10, face = "bold"),
      legend.text = element_text(size = 10),
      legend.key.width = unit(2, "cm"),
      legend.key.height = unit(0.4, "cm"),
      plot.background = element_rect(fill = "white", color = NA),
      plot.margin = margin(t = 5, r = 5, b = 5, l = 5),
      plot.title.position = "plot"
    )
  if (season == "week") {
    plot <- plot + ggplot2::facet_wrap(~season, ncol = n_col, labeller = ggplot2::labeller(season = labeller_vector))
  } else {
    plot <- plot + ggplot2::facet_wrap(~season, ncol = n_col)
  }

  if (save_data) {
    file_out <- stringr::str_replace(name_output, "\\.png$", ".csv")
    readr::write_csv(data_plot, file_out)
  }

  ggplot2::ggsave(name_output, plot = plot, width = width, height = height, dpi = 300)
  cat("\n✅ Listo: gráfico generado y guardado.\n")
  toc()
}
