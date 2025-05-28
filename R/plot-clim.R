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
#' @importFrom dplyr all_of across between bind_rows group_by group_split cur_group_id filter first mutate pull select summarise tibble ungroup reframe
#' @importFrom lubridate month year as_date
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
#' @importFrom stringr str_extract str_to_sentence str_to_title
#' @importFrom tidyr separate_wider_delim
#' @importFrom tibble deframe
#' @importFrom magrittr %>%
#' @importFrom readr write_csv
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
plot_clim <- function(dir_input = NULL, season, stat_function, var_name, shp_file = NULL, start_date = NULL, end_date = NULL, n_col, name_output, res = 300, height = 8, width = 6, ticks_x = 0.1, ticks_y = 0.1, n_cores = 1, xlim, ylim,save_data = TRUE, from_data = FALSE, data_plot_file = NULL, sensor, zona) {
  tic()
  sf::sf_use_s2(FALSE)
  if (is.null(shp_file)) {
    if (!requireNamespace("rnaturalearth", quietly = TRUE)) {
      stop("El paquete 'rnaturalearth' no está instalado. Instálalo con install.packages('rnaturalearth')")
    }
    cat("\n\n Generando shp para la zona definida...\n\n")
    shp_sf <- rnaturalearth::ne_countries(scale = 10, returnclass = "sf") %>%
      dplyr::filter(admin == "Chile") %>%
      sf::st_geometry()
    bbox <- sf::st_bbox(c(xmin = xlim[2], xmax = xlim[1], ymin = ylim[2] , ymax = ylim[1]), crs = sf::st_crs(shp_sf))
    shp_sf <- suppressWarnings(sf::st_crop(shp_sf, bbox))
  } else {
    shp_sf <- sf::read_sf(shp_file) %>%
      sf::st_geometry()
    }

  if (from_data) {
    cat("\n\n Generando gráfico...\n\n")
    data_plot <- readr::read_csv(data_plot_file, show_col_types = FALSE)
    if (season == "month") {
      data_plot <- data_plot %>%
        dplyr::mutate(season = factor(season, levels = c(
          "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
          "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
        )))
    } else if (season == "year") {
      data_plot <- data_plot %>%
        dplyr::mutate(season = factor(season, levels = seq(
          from = min(season, na.rm = TRUE),
          to = max(season, na.rm = TRUE), by = 1
        )))
    } else if (season == "week") {
      data_plot <- data_plot %>%
        dplyr::mutate(
          isoweek = lubridate::isoweek(date),
          week_name = paste0("Semana ", isoweek),
          season = week_name)
      weeks_seq <- data.frame(week = unique(data_plot$isoweek)) %>%
        dplyr::mutate(
          week = as.integer(week),
          start = lubridate::as_date(lubridate::floor_date(as.Date(start_date), "week", week_start = 1)) + 7 * (week - min(week)),
          end   = start + 6,
          label = glue::glue("Semana {week} ({format(start, '%d')}–{format(end, '%d %b')})"),
          season = paste0("Semana ", week))

      labeller_vector <- weeks_seq %>%
        dplyr::select(season, label) %>%
        tibble::deframe()
    }

    #####plots vars####
    title_plot <- switch (var_name,
                          "chlor_a" = "Concentración de clorofila-a en",
                          "sst" = "Temperatura Superficial del Mar en",
                          "Rrs_645" = "Radiación normalizada de salida del agua (645nm) en")
    title_plot <- glue::glue("{title_plot} {zona}")

    years <- unique(lubridate::year(data_plot$date))
    start_date <- as.Date(start_date)
    end_date   <- as.Date(end_date)

    if (length(years) == 1) {
      subtitle <- glue::glue(
        "Periodo: del {format(start_date, '%d')} de {stringr::str_to_sentence(format(start_date, '%B'))} ",
        "al {format(end_date, '%d')} de {stringr::str_to_sentence(format(end_date, '%B'))} de {years}"
      )
    } else {
      subtitle <- glue::glue(
        "Periodo: {lubridate::year(start_date)} – {lubridate::year(end_date)}"
      )
    }


    cols <- switch (var_name,
                    "sst" = c(get_palette("blues"), get_palette("reds")),
                    get_palette("oce_jets"))
    caption <- switch (sensor,
                       "aqua" = "Fuente: OceanColor Data; Sensor Modis-Aqua",
                       "terra" = "Fuente: OceanColor Data; Sensor Modis-Terra",
                       "modis_aq" = "Fuente: OceanColor Data; Combined Aqua-Terra satellites",
                       "sentinel3A" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3A",
                       "sentinel3B" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3B",
                       "sentinelAB" = "Fuente: OceanColor Data; Combined Sentinel3A-Sentinel3B satellites",
                       default = "Sensor desconocido")
    guide_title <- switch(var_name,
                          "chlor_a" = expression(paste("Clorofila-α [", mg ~ m^{-3}, "]")),
                          "sst" = "Temperatura Superficial Mar [°C]",
                          "Rrs_645" = "Radiación normalizada de salida del agua (645nm)")
    barwidth <- grid::convertWidth(grid::stringWidth(guide_title), unitTo = "lines", valueOnly = TRUE) * 1.1 + n_col
    if (var_name == "chlor_a") {
      data_plot <- data_plot %>% dplyr::mutate(fill = log10(fill))
      min_value <- min(data_plot$fill, na.rm = TRUE)
      max_value <- max(data_plot$fill, na.rm = TRUE)
      limits <- c(floor(min_value), ceiling(max_value))
      breaks <- seq(limits[1], limits[2], by = 1)
      labels <- as.expression(lapply(breaks, function(x) bquote(10^.(x))))
    } else if (var_name == "sst") {
      limits <- ceiling(range(data_plot$fill, na.rm = TRUE))
      breaks <- round(seq(from = limits[[1]], to = limits[[2]],length.out = 4))
      #labels <- ggplot2::waiver()
    } else {
      limits <- ceiling(range(data_plot$fill, na.rm = TRUE))
      breaks <- seq(from = limits[[1]], to = limits[[2]],length.out = 4)
    }
    plot <- ggplot2::ggplot(data = data_plot) +
      ggplot2::geom_tile(aes(x = lon, y = lat, fill = fill)) +
      ggplot2::scale_fill_gradientn(colours = cols,
                                    na.value = "white",
                                    breaks = breaks,
                                    #labels = labels,
                                    limits = limits) +
      scale_x_longitude(ticks = ticks_x) +
      scale_y_latitude(ticks = ticks_y) +
      ggplot2::geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      ggplot2::coord_sf(xlim = xlim, ylim = ylim, expand = FALSE,clip = "on") +
      ggplot2::guides(fill = guide_colorbar(title = guide_title,
                                            title.position = "top",
                                            barwidth = barwidth,
                                            #title.theme = element_text(angle = 90),
                                            #barwidth = .5,
                                            barheight = 0.8,
                                            title.hjust = 0.5)) +
      ggplot2::labs(title = title_plot,
                    subtitle = subtitle,
                    caption = caption,
                    x = NULL,
                    y = NULL) +
      ggplot2::theme_minimal(base_size = 11) +
      ggplot2::theme(axis.text = element_text(color = "gray30"),
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
                     plot.title.position = "plot")
    if (season == "week") {
      plot <- plot + ggplot2::facet_wrap(~season, ncol = n_col, labeller = ggplot2::labeller(season = labeller_vector))
    } else {
      plot <- plot + ggplot2::facet_wrap(~season, ncol = n_col)
    }
    }  else {
      files_ext_pattern <- paste(c(".parquet$", ".csv$", ".tif$"), collapse = "|")
      all_files_tmp <- list.files(path = dir_input, full.names = TRUE, pattern = files_ext_pattern) %>%
      dplyr::tibble(file = .) %>%
      dplyr::mutate(
        tmp_col = basename(file),
        tmp = stringr::str_extract(string = tmp_col, pattern = "^\\d+-\\d+")
      ) %>%
      tidyr::separate(tmp_col,into = "date",sep = "_",remove = FALSE, extra =  "drop") %>%
      dplyr::mutate(date = as.Date(date))
      all_files_tmp <- if (is.null(start_date) || is.null(end_date)) {
      all_files_tmp
    } else {
      start_date <- as.Date(start_date)
      end_date <- as.Date(end_date)
      all_files_tmp %>% dplyr::filter(dplyr::between(date, start_date, end_date))
    }
      all_files_tmp <- all_files_tmp %>%
      tidyr::separate_wider_delim(., cols = "tmp", delim = "-", cols_remove = TRUE, names = c("year", "month"), too_many = "drop") %>%
      dplyr::select(-tmp_col)
    if(season == "week") {
      all_files_tmp <- all_files_tmp  %>%
        dplyr::mutate(week = lubridate::isoweek(date))
    }
    cat("\n\n Calculando climatología...\n\n")
    path_list <- all_files_tmp %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(season))) %>%
      dplyr::group_split() %>%
      purrr::map(~ dplyr::pull(.x, "file"))
    func <- eval(parse(text = stat_function))
    ext_file <- unique(stringr::str_extract(string = all_files_tmp$file, pattern = files_ext_pattern))
    process_tables <- function(file) {
      dataframe <- switch(ext_file,
                          ".parquet" = arrow::read_parquet(file),
                          ".csv" = readr::read_csv(file, show_col_types = FALSE, progress = FALSE)
      )
      dataframe <- dataframe %>%
        tidyr::drop_na() %>%
        dplyr::group_by(lat, lon) %>%
        dplyr::mutate(ID = dplyr::cur_group_id()) %>%
        dplyr::group_by(ID) %>%
        dplyr::summarise(
          fill = func(!!sym(var_name), na.rm = TRUE),
          date1 = dplyr::first(date1),
          date2 = dplyr::first(date2),
          lon = dplyr::first(lon),
          lat = dplyr::first(lat),
          .groups = "drop"
        ) %>%
        dplyr::select(-ID) %>%
        dplyr::filter(dplyr::between(lon, xlim[1], xlim[2])) %>%
        dplyr::filter(dplyr::between(lat, ylim[2], ylim[1])) %>%
        dplyr::mutate(season = stringr::str_to_sentence(lubridate::month(date1, abbr = FALSE, label = TRUE)))
      return(dataframe)
      #rm(list = c("data_sf", "data_to_filter"))
      gc()
    }

    process_sublist <- function(entry_list) {
      if (n_cores <= 1) {
        progressr::with_progress({
          p <- progressr::progressor(steps = length(entry_list))
          dataframe_list <- purrr::map(entry_list, ~ {
            result <- process_tables(.x)
            p()
            Sys.sleep(0.2)
            result
          })
        })
      } else {
        cl <- parallel::makeForkCluster(n_cores)
        on.exit(parallel::stopCluster(cl))
        future::plan("cluster", workers = cl)
        progressr::with_progress({
          p <- progressr::progressor(steps = length(entry_list))
          dataframe_list <- furrr::future_map(entry_list, ~ {
            result <- process_tables(.x)
            p()
            Sys.sleep(0.2)
            result
          }, .options = furrr::furrr_options(seed = TRUE,
                                             packages = c("sf", "dplyr")))
        })
      }
      return(dataframe_list)
    }
    all_results <- purrr::imap(path_list, ~ {
      print(paste("Procesando item", .y, "de", length(path_list), "con", length(.x), "archivos"))
      process_sublist(entry_list = .x)
    })

    data_plot <- dplyr::bind_rows(all_results) %>%
      dplyr::group_by(lat, lon) %>%
      dplyr::mutate(ID = dplyr::cur_group_id()) %>%
      dplyr::ungroup() %>%
      dplyr::group_by(ID, season) %>%
      dplyr::summarise(
        fill = func(fill, na.rm = TRUE),
        date1 = first(date1),
        date2 = first(date2),
        lon = first(lon),
        lat = first(lat),
        .groups = "drop"
      ) %>%
      dplyr::select(-ID) %>%
      dplyr::mutate(date = lubridate::as_date(date1))
    if (season == "month") {
      data_plot <- data_plot %>%
        dplyr::mutate(season = stringr::str_to_title(lubridate::month(date, label = TRUE, abbr = FALSE)),
                      season = factor(season, levels = c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")))
    } else if (season == "year") {
      data_plot <- data_plot %>% dplyr::mutate(season = lubridate::year(date),
                                               season =  factor(season, levels = seq(from = min(season, na.rm = TRUE), to = max(season, na.rm = TRUE), by = 1)))
    } else if (season == "week") {
      data_plot <- data_plot %>%
        dplyr::mutate(
          isoweek = lubridate::isoweek(date),
          week_name = paste0("Semana ", isoweek),
          season = week_name)
      weeks_seq <- data.frame(week = unique(data_plot$isoweek)) %>%
        dplyr::mutate(
          week = as.integer(week),
          start = lubridate::as_date(lubridate::floor_date(as.Date(start_date), "week", week_start = 1)) + 7 * (week - min(week)),
          end   = start + 6,
          label = glue::glue("Semana {week} ({format(start, '%d')}–{format(end, '%d %b')})"),
          season = paste0("Semana ", week))

      labeller_vector <- weeks_seq %>%
        dplyr::select(season, label) %>%
        tibble::deframe()
    }
  }
  #####plots vars####
  title_plot <- switch (var_name,
                        "chlor_a" = "Concentración de clorofila-a en",
                        "sst" = "Temperatura Superficial del Mar en",
                        "Rrs_645" = "Radiación normalizada de salida del agua (645nm) en")
  title_plot <- glue::glue("{title_plot} {zona}")
  years <- unique(lubridate::year(data_plot$date))
  start_date <- as.Date(start_date)
  end_date   <- as.Date(end_date)

  if (length(years) == 1) {
    subtitle <- glue::glue(
      "Periodo: del {format(start_date, '%d')} de {stringr::str_to_sentence(format(start_date, '%B'))} ",
      "al {format(end_date, '%d')} de {stringr::str_to_sentence(format(end_date, '%B'))} de {years}"
    )
  } else {
    subtitle <- glue::glue(
      "Periodo: {lubridate::year(start_date)} – {lubridate::year(end_date)}"
    )
  }
  cols <- switch (var_name,
                  "sst" = c(get_palette("blues"), get_palette("reds")),
                  get_palette("oce_jets"))
  caption <- switch (sensor,
                     "aqua" = "Fuente: OceanColor Data; Sensor Modis-Aqua",
                     "terra" = "Fuente: OceanColor Data; Sensor Modis-Terra",
                     "modis_aq" = "Fuente: OceanColor Data; Combined Aqua-Terra satellites",
                     "sentinel3A" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3A",
                     "sentinel3B" = "Fuente: OceanColor Data; Sensor OLCI-Sentinel3B",
                     "sentinelAB" = "Fuente: OceanColor Data; Combined Sentinel3A-Sentinel3B satellites",
                     default = "Sensor desconocido")
  guide_title <- switch(var_name,
                        "chlor_a" = expression(paste("Clorofila-α [", mg ~ m^{-3}, "]")),
                        "sst" = "Temperatura Superficial Mar [°C]",
                        "Rrs_645" = "Radiación normalizada de salida del agua (645nm)")
  barwidth <- grid::convertWidth(grid::stringWidth(guide_title), unitTo = "lines", valueOnly = TRUE) * 1.1 + n_col

  if (var_name == "chlor_a") {
    data_plot <- data_plot %>% dplyr::mutate(fill = log10(fill))
    min_value <- min(data_plot$fill, na.rm = TRUE)
    max_value <- max(data_plot$fill, na.rm = TRUE)
    limits <- c(floor(min_value), ceiling(max_value))
    breaks <- seq(limits[1], limits[2], by = 1)
    labels <- as.expression(lapply(breaks, function(x) bquote(10^.(x))))
  } else if (var_name == "sst") {
    limits <- ceiling(range(data_plot$fill, na.rm = TRUE))
    breaks <- round(seq(from = limits[[1]], to = limits[[2]],length.out = 4))
    #labels <- ggplot2::waiver()
  } else {
    limits <- ceiling(range(data_plot$fill, na.rm = TRUE))
    breaks <- seq(from = limits[[1]], to = limits[[2]],length.out = 4)
    }
  plot <- ggplot2::ggplot(data = data_plot) +
    ggplot2::geom_tile(aes(x = lon, y = lat, fill = fill)) +
    ggplot2::scale_fill_gradientn(colours = cols,
                                  na.value = "white",
                                  breaks = breaks,
                                  #labels = labels,
                                  limits = limits) +
    scale_x_longitude(ticks = ticks_x) +
    scale_y_latitude(ticks = ticks_y) +
    ggplot2::geom_sf(data = shp_sf, fill = "grey80", col = "black") +
    ggplot2::coord_sf(xlim = xlim, ylim = ylim, expand = FALSE,clip = "on") +
    ggplot2::guides(fill = guide_colorbar(
      title = guide_title,
      title.position = "top",
      barwidth = barwidth,
      #title.theme = element_text(angle = 90),
      #barwidth = .5,
      barheight = 0.8,
      title.hjust = 0.5)) +
    ggplot2::labs(title = title_plot,
                  subtitle = subtitle,
                  caption = caption,
                  x = NULL,
                  y = NULL) +
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
      plot.margin = margin(t=5, r= 5, b = 5, l = 5),
      plot.title.position = "plot"
    )

  if (season == "week") {
    plot <- plot + ggplot2::facet_wrap(~season, ncol = n_col, labeller = ggplot2::labeller(season = labeller_vector))
  } else {
    plot <- plot + ggplot2::facet_wrap(~season, ncol = n_col)
  }

  if (save_data) {
    if (var_name == "chlor_a") {
      data_plot <- data_plot %>% dplyr::mutate(fill = 10^fill)
    }
    filename <- stringr::str_replace(name_output, ".png", ".csv")
    readr::write_csv(data_plot, filename)
  }
  ggplot2::ggsave(filename = name_output, plot = plot, device = "png", units = "in", dpi = res, height = height, width = width)
  cat("\n\n Listo!...\n\n")
  toc()
}


