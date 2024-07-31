#' @title plot_clim
#' @description Función para generar climatología y exportarla (formato png) para una variable determinada
#' @param dir_input directorio en donde se almacenan las imágenes raster (formato .tif), csv o parquet
#' @param dir_output directorio en donde se almacenará la imagen png
#' @param season temporalidad para la generación de imágenes en formato raster("mes", o "year")
#' @param stat_function función estadística para generar las imágenes raster ("median" o "mean")
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param shp_file nombre archivo shp para el gráfico (si no esta en dir_input poner nombre con ruta completa)
#' @param n_col numero de columnas para el gráfico
#' @param name_output nombre para la salidas
#' @param res resolución para la imágen png de la climatología
#' @param height altura para la imágen png de la climatología
#' @param width amplitud para la imágen png de la climatología
#' @param xlim vector numérico tamaño 2 para los limites de x en el plot
#' @param ylim vector numérico tamaño 2 para los limites de y en el plot
#' @param ticks_x vector numérico tamaño 1 indicando ticks en el eje x del plot
#' @param ticks_y vector numérico tamaño 1 indicando ticks en el eje y del plot
#' @import arrow
#' @import stringr
#' @import lubridate
#' @import purrr
#' @import rlang
#' @import dplyr
#' @import raster
#' @import sf
#' @import oce
#' @import future
#' @import parallel
#' @import progressr
#' @import furrr
#' @return imágenes png
#' @export plot_clim
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input"
#' dir_output <- "/dir/to/ouput"
#' season <- "month"
#' stat_function <- "median"
#' var_name <- "chlor_a"
#' n_col <- 3
#' name_output <- "climatologia_chlor_a.png"
#' shp_file <- "/home/evolecolab/Escritorio/test_satImg/raster_mensuales/resultados_raster/Golfo_Arauco_prj2.shp"
#' res <- 300
#' height <- 7
#' width <- 9
#' plot_clim(dir_input = dir_input, dir_output = dir_output, season = season, stat_function = stat_function, var_name = var_name, shp_file = shp_file, n_col = n_col, n_cores, name_output = name_output, res = res, height = height, width = width)
#' }
plot_clim <- function(dir_input, dir_output, season, stat_function, var_name, shp_file, n_col, name_output, res = 300, height = 8, width = 6, ticks_x = 0.2, ticks_y = 0.1, n_cores = 1) {
  #agregar los errores  para no calcular todo y luego ver que solo falta un paramentro gráfico..
  #* Establish a new 'ArgCheck' object
  #Check <- ArgumentCheck::newArgCheck()
  #* Add an error if height < 0
  #if (height < 0)
  #  ArgumentCheck::addError(
  #    msg = "'height' must be >= 0",
  #    argcheck = Check
  #  )

  #* Add an error if radius < 0
  #if (radius < 0)
  #  ArgumentCheck::addError(
  #    msg = "'radius' must be >= 0",
  #    argcheck = Check
  #  )

  #* Return errors and warnings (if any)
  #ArgumentCheck::finishArgCheck(Check)
  #tic(msg = "Duración total análisis")
  #######################
  files_ext_pattern <- paste(c(".parquet$", ".csv$", ".tif$"), collapse = "|")
  all_files_tmp <- list.files(path = dir_input,full.names = TRUE, pattern = files_ext_pattern) %>%
    dplyr::tibble(file = .) %>%
    dplyr::mutate(tmp_col = basename(file),
                  tmp = stringr::str_extract_all(string = tmp_col, pattern = "^\\d+(_\\d+)*"))
  #n_count <- stringr::str_count(string = all_files_tmp$tmp[1],pattern = "_")
  #season_map <- c("year", "month", "day")
  # Asignar valor a season usando el vector de estaciones
  #season <- ifelse(n_count >= 0 && n_count <= 2, season_map[n_count + 1], NA)
  all_files_tmp <- all_files_tmp %>%
    {
      switch(
        season,
        "year" = tidyr::separate_wider_delim(., cols = "tmp", delim = "_", cols_remove = TRUE, names = "year", too_many = "drop"),
        "month" = tidyr::separate_wider_delim(., cols = "tmp", delim = "_", cols_remove = TRUE, names = c("year", "month"), too_many = "drop"),
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
  cat("\n\n Calculando climatologías...\n\n")
  path_list <- purrr::map(files_df_list, ~ dplyr::pull(., "file"))
  func <- match.fun(stat_function)
  ext_file <- unique(stringr::str_extract(string = all_files_tmp$tmp_col, pattern = files_ext_pattern))
  ####filter data####
  sf::sf_use_s2(FALSE)
  shp_sf <- sf::read_sf(shp_file) %>% sf::st_geometry()
  bbox <- sf::st_bbox(shp_sf)
  #process_table and  process_sublist should be in utils.R but does not work!
  process_tables <- function(file) {
    dataframe <- switch(ext_file,
                        ".parquet" = arrow::read_parquet(file),
                        ".csv" = readr::read_csv(file, show_col_types = FALSE, progress = FALSE))
    dataframe <- dataframe %>%
      tidyr::drop_na() %>%
      dplyr::group_by(lat, lon) %>%
      dplyr::mutate(ID = dplyr::cur_group_id()) %>%
      dplyr::ungroup() %>%
      dplyr::group_by(ID) %>%
      dplyr::summarise(fill = func(!!sym(var_name), na.rm = TRUE),
                       date1 = dplyr::first(date1),
                       date2 = dplyr::first(date2),
                       lon = dplyr::first(lon),
                       lat = dplyr::first(lat),
                       .groups = "drop") %>%
      dplyr::select(-ID)
    data_sf <- sf::st_as_sf(dataframe, coords = c("lon", "lat"), crs = 4326)
    data_to_filter <- suppressMessages(sf::st_intersects(data_sf, shp_sf))
    # just useful for marine plots
    dataframe <- dataframe %>% dplyr::mutate(inside = apply(data_to_filter, 1, any))
    dataframe <- dataframe %>%
      dplyr::filter(inside == FALSE) %>%
      dplyr::filter(dplyr::between(lon, bbox[1], bbox[3])) %>%
      dplyr::filter(dplyr::between(lat, bbox[2], bbox[4])) %>%
      dplyr::mutate(season = stringr::str_to_sentence(lubridate::month(date1, abbr = FALSE, label = TRUE))) %>%
      dplyr::select(-inside)
    return(dataframe)
    rm(list = c("data_sf", "data_to_filter"))
    gc()
  }
  process_sublist <- function(entry_list) {
    if (n_cores <= 1) {
      progressr::with_progress({
        p <- progressr::progressor(steps = length(entry_list))
        dataframe_list <- purrr::map(entry_list, ~{
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
        dataframe_list <- furrr::future_map(entry_list, ~{
          result <- process_tables(.x)
          p()
          Sys.sleep(0.2)
          result
        }, .options = furrr::furrr_options(seed = TRUE))
      })
    }
    return(dataframe_list)
  }

  index <- seq_along(path_list)
  all_results <- purrr::map2(.x = path_list, .y = index, ~ {
    print(paste("Procesando item", .y, "de", max(index), "con", length(.x), "archivos"))
    process_sublist(entry_list = .x)
  })

  data_plot <- dplyr::bind_rows(all_results) %>%
    dplyr::group_by(lat, lon) %>%
    dplyr::mutate(ID = dplyr::cur_group_id()) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(ID, season) %>%
    dplyr::summarise(fill = func(fill, na.rm = TRUE),
                     date1 = first(date1),
                     date2 = first(date2),
                     lon = first(lon),
                     lat = first(lat),
                     .groups = "drop") %>%
    dplyr::select(-ID)
  if (season == "month") {
    data_plot <- data_plot %>% dplyr::mutate(season = factor(season,
                                                             levels = c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")))
  }
  if (season == "year") {
    data_plot <- data_plot %>% dplyr::mutate(season = factor(season,
                                                             levels = seq(from = min(season, na.rm = TRUE), to = max(season, na.rm = TRUE), by = 1)))
  }
  ###Plot section####
  cat("\n\n Generando gráfico...\n\n")
  xlim <- c(bbox[1], bbox[3])
  ylim <- c(bbox[2], bbox[4])
  if (var_name == "sst") {
    blues <- get_palette("blues")
    reds <- get_palette("reds")
    plot <- ggplot(data = data_plot) +
      geom_tile(aes(x = lon, y = lat, fill = fill)) +
      scale_fill_gradientn(
        colours = c(blues, reds), na.value = "white",
        n.breaks = 5
      ) +
      scale_x_longitude(ticks = ticks_x) +
      scale_y_latitude(ticks = ticks_y) +
      coord_equal() +
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
      facet_wrap(~season, ncol = ncol) +
      labs(title = paste0("Temperatura Superficial del Mar Periodo: ", lubridate::year(min(data_plot$date1)), "-", lubridate::year(max(data_plot$date2))),
           caption = "Fuente: OceanColor Data")
    ggsave(filename = name_plot, plot = plot, device = "png", units = "in", dpi = 300, height = height, width = width)
  }
}
