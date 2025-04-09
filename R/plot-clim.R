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
#' @importFrom arrow read_parquet
#' @importFrom dplyr all_of across between bind_rows group_by group_split cur_group_id filter first mutate pull select summarise tibble ungroup
#' @importFrom lubridate month year
#' @importFrom furrr future_walk furrr_options
#' @importFrom future plan
#' @importFrom parallel makeForkCluster stopCluster
#' @importFrom progressr progressor with_progress
#' @importFrom purrr map map2 map_chr
#' @importFrom rlang !! sym :=
#' @importFrom sf sf_use_s2 read_sf st_as_sf st_bbox st_intersects st_geometry st_crs
#' @importFrom stringr str_extract str_to_sentence
#' @importFrom tidyr separate_wider_delim
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
plot_clim <- function(dir_input=NULL, season, stat_function, var_name, shp_file = NULL, start_date = NULL, end_date = NULL, n_col, name_output, res = 300, height = 8, width = 6, ticks_x = 0.1, ticks_y = 0.1, n_cores = 1, xlim, ylim,save_data = TRUE, from_data = FALSE, data_plot_file = NULL, sensor) {
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
    if (!requireNamespace("sf", quietly = TRUE)) {
      stop("El paquete 'sf' no está instalado. Instálalo con install.packages('sf')")
    }
    shp_sf <- sf::read_sf(shp_file) %>%
      sf::st_geometry()
  }
  caption <- switch (sensor,
                     "aqua" = "Fuente: OceanColor Data; Sensor Aqua-MODIS",
                     "terra" = "Fuente: OceanColor Data; Sensor Terra-MODIS",
                     "all" = "Fuente: OceanColor Data; Combined sensor Aqua-Terra"
  )
  if(from_data) {
    data_plot <- readr::read_csv(data_plot_file,show_col_types = FALSE)
    if (season == "month") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season,
                                                               levels = c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")
      ))
    }
    if (season == "year") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season,
                                                               levels = seq(from = min(season, na.rm = TRUE), to = max(season, na.rm = TRUE), by = 1)
      ))
    }
    cat("\n\n Generando gráfico...\n\n")
    #xlim <- c(bbox[1], bbox[3])
    #ylim <- c(bbox[2], bbox[4])

    if (var_name == "sst") {
      blues <- get_palette("blues")
      reds <- get_palette("reds")
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = c(blues, reds), na.value = "white",
          n.breaks = 5
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = "Temperatura Superficial Mar (°C)",
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Temperatura Superficial del Mar durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "chlor_a") {
      oce_jets <- get_palette("oce_jets")
      min_value <- min(log10(data_plot$fill), na.rm = TRUE)
      max_value <- max(log10(data_plot$fill), na.rm = TRUE)
      limits <- c(floor(min_value), ceiling(max_value))
      breaks <- seq(limits[1], limits[2], by = 1)
      labels <- purrr::map(breaks, ~ bquote(10^.(.x))) %>%
        purrr::map_chr(deparse)
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = log10(fill))) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white",
          limits = limits,
          breaks = breaks,
          labels = parse(text = labels)
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(Clorofila - alpha ~ " "(mg ~ m^{
            -3
          }))),
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        #theme(panel.spacing = unit(2, "lines")) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Clorofila-a durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "Rrs_645") {
      oce_jets <- get_palette("oce_jets")
      data_plot <- data_plot %>%
        dplyr::mutate(fill = fill * 158.9418) #%>%
      #dplyr::filter(fill >= 0)

      min_fill <- min(data_plot$fill[data_plot$fill > 0], na.rm = TRUE)
      data_plot <- data_plot %>%
        mutate(fill = if_else(fill < 0, min_fill, fill))

      plot <- ggplot2::ggplot(data_plot) +
        geom_raster(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white"
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
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
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = "Radiación normalizada de salida del agua (645nm)",
          subtitle = paste0("Periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "Rrs_555") {
      oce_jets <- get_palette("oce_jets")
      data_plot <- data_plot %>%
        dplyr::mutate(fill = fill * 183.869) %>%
        dplyr::filter(fill >= 0)
      plot <- ggplot2::ggplot(data_plot) +
        geom_raster(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white"
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(
            "nWLR 555",
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
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Radiación normalizada de salida del agua (555 nm) durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "nflh") {
      oce_jets <- get_palette("oce_jets")
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white",
          n.breaks = 5
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(
            "nFLH",
            " (",
            "mW ",
            m^-2,
            um^-1,
            sr^-1,
            ")"
          )),
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Altura Normalizada de la Línea de Fluorescencia durnte periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )


    }
    if (var_name == "Kd_490") {
      oce_jets <- get_palette("oce_jets")
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white",
          n.breaks = 5
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(
            "Kd 490",
            " (",
            m^-1,
            ")"
          )),
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Coeficiente de atenuación difusa a 490 nm durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
  } else {
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

      all_files_tmp  # No cambia nada si alguna de las fechas es NULL
    } else {
      start_date <- as.Date(start_date)
      end_date <- as.Date(end_date)
      all_files_tmp %>% dplyr::filter(dplyr::between(date, start_date, end_date))
    }
    all_files_tmp <- all_files_tmp %>%
      {
        switch(season,
               "year" = tidyr::separate_wider_delim(., cols = "tmp", delim = "-", cols_remove = TRUE, names = "year", too_many = "drop"),
               "month" = tidyr::separate_wider_delim(., cols = "tmp", delim = "-", cols_remove = TRUE, names = c("year", "month"), too_many = "drop"),
               .
        )
      }

    files_df_list <- all_files_tmp %>%
      dplyr::group_by(dplyr::across(dplyr::all_of(season))) %>%
      dplyr::group_split() %>%
      setNames(purrr::map(., ~ {
        if (season == "year") {
          unique(.[["year"]])
        } else if (season == "month") {
          unique(.[["month"]])
        }
      }))
    cat("\n\n Calculando climatología...\n\n")
    path_list <- purrr::map(files_df_list, ~ dplyr::pull(., "file"))
    #func <- base::match.fun(summ_function)
    func <- eval(parse(text = stat_function))
    ext_file <- unique(stringr::str_extract(string = all_files_tmp$tmp_col, pattern = files_ext_pattern))
    #### filter data####

    #bbox <- sf::st_bbox(shp_sf)
    # process_table and  process_sublist should be in utils.R but does not work!
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
          }, .options = furrr::furrr_options(seed = TRUE))
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
      dplyr::select(-ID)
    if (season == "month") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season,
                                                               levels = c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")
      ))
    }
    if (season == "year") {
      data_plot <- data_plot %>% dplyr::mutate(season = factor(season,
                                                               levels = seq(from = min(season, na.rm = TRUE), to = max(season, na.rm = TRUE), by = 1)
      ))
    }
    ### Plot section####
    cat("\n\n Generando gráfico...\n\n")
    #xlim <- c(bbox[1], bbox[3])
    #ylim <- c(bbox[2], bbox[4])

    if (var_name == "sst") {
      blues <- get_palette("blues")
      reds <- get_palette("reds")
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = c(blues, reds), na.value = "white",
          n.breaks = 5
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = "Temperatura Superficial Mar (°C)",
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Temperatura Superficial del Mar durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "chlor_a") {
      oce_jets <- get_palette("oce_jets")
      min_value <- min(log10(data_plot$fill), na.rm = TRUE)
      max_value <- max(log10(data_plot$fill), na.rm = TRUE)
      limits <- c(floor(min_value), ceiling(max_value))
      breaks <- seq(limits[1], limits[2], by = 1)
      labels <- purrr::map(breaks, ~ bquote(10^.(.x))) %>%
        purrr::map_chr(deparse)
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = log10(fill))) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white",
          limits = limits,
          breaks = breaks,
          labels = parse(text = labels)
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(Clorofila - alpha ~ " "(mg ~ m^{
            -3
          }))),
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Clorofila-a durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "Rrs_645") {
      oce_jets <- get_palette("oce_jets")
      data_plot <- data_plot %>%
        dplyr::mutate(fill = fill * 158.9418) #%>%
      #dplyr::filter(fill >= 0)

      min_fill <- min(data_plot$fill[data_plot$fill > 0], na.rm = TRUE)
      data_plot <- data_plot %>%
        mutate(fill = if_else(fill < 0, min_fill, fill))

      plot <- ggplot2::ggplot(data_plot) +
        geom_raster(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white"
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
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
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = "Radiación normalizada de salida del agua (645nm)",
          subtitle = paste0("Periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "Rrs_555") {
      oce_jets <- get_palette("oce_jets")
      data_plot <- data_plot %>%
        dplyr::mutate(fill = fill * 183.869) %>%
        dplyr::filter(fill >= 0)
      plot <- ggplot2::ggplot(data_plot) +
        geom_raster(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white"
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(
            "nWLR 555",
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
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Radiación normalizada de salida del agua (555 nm) durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )

    }
    if (var_name == "nflh") {
      oce_jets <- get_palette("oce_jets")
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white",
          n.breaks = 5
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(
            "nFLH",
            " (",
            "mW ",
            m^-2,
            um^-1,
            sr^-1,
            ")"
          )),
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Altura Normalizada de la Línea de Fluorescencia durnte periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )


    }
    if (var_name == "Kd_490") {
      oce_jets <- get_palette("oce_jets")
      plot <- ggplot2::ggplot(data = data_plot) +
        geom_tile(aes(x = lon, y = lat, fill = fill)) +
        scale_fill_gradientn(
          colours = oce_jets,
          na.value = "white",
          n.breaks = 5
        ) +
        scale_x_longitude(ticks = ticks_x) +
        scale_y_latitude(ticks = ticks_y) +
        # coord_equal() +
        geom_sf(data = shp_sf, fill = "grey80", col = "black") +
        coord_sf(xlim = xlim, ylim = ylim) +
        guides(fill = guide_colorbar(
          title = expression(paste(
            "Kd 490",
            " (",
            m^-1,
            ")"
          )),
          title.position = "right",
          title.theme = element_text(angle = 90),
          barwidth = .5,
          barheight = 15,
          title.hjust = .5
        )) +
        theme_bw() +
        facet_wrap(~season, ncol = n_col) +
        theme(panel.spacing.x = unit(2, "lines"),
              panel.spacing.y = unit(1, "lines")) +
        labs(
          title = paste0("Coeficiente de atenuación difusa a 490 nm durante periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
          caption = caption
        )


    }

  }
  if(save_data) {
    filename <- stringr::str_replace(string = name_output, pattern = ".png", replacement = ".csv")
    readr::write_csv(x = data_plot,file = filename)
    ggplot2::ggsave(filename = name_output, plot = plot, device = "png", units = "in", dpi = 300, height = height, width = width)
    cat("\n\n Listo!...\n\n")
  } else {
    ggplot2::ggsave(filename = name_output, plot = plot, device = "png", units = "in", dpi = 300, height = height, width = width)
    cat("\n\n Listo!...\n\n")
  }

  toc()
}
