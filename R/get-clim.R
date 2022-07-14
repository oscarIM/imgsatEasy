#' @title get_clim
#' @description Función para generar climatología y exportarla (formato png) para una variable determinada
#' @param dir_input directorio en donde se almacenan las imágenes raster (formato .tif)
#' @param dir_output directorio en donde se almacenará la imagen png
#' @param season temporalidad para la generación de imágenes en formato raster("mes", o "year")
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean")
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param shp_file nombre archivo shp para el gráfico (si no esta en dir_input poner nombre con ruta completa)
#' @param n_col numero de columnas para el gráfico
#' @param n_row numero de filas para el gráfico
#' @param name_output nombre para la salidas
#' @param res resolución para la imágen png de la climatología
#' @param height altura para la imágen png de la climatología
#' @param width  amplitud para la imágen png de la climatología
#' @import ggplot2
#' @importFrom fs dir_ls dir_create dir_exists
#' @importFrom stars st_as_stars write_stars read_stars st_apply
#' @importFrom raster stack
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr mutate across group_split group_by case_when bind_rows
#' @importFrom purrr map
#' @importFrom tidyr separate pivot_longer
#' @importFrom sf read_sf st_geometry
#' @importFrom raster stack
#' @importFrom oce oce.colorsJet oce.colorsViridis
#' @importFrom metR scale_x_longitude scale_y_latitude
#' @importFrom RColorBrewer brewer.pal
#' @importFrom stringr str_remove str_to_sentence str_extract str_replace
#' @importFrom tictoc tic toc
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom progressr with_progress progressor
#' @importFrom furrr future_map furrr_options
#' @importFrom methods as
#' @return imágenes png y tif de climatologías y Rdata
#' @export get_clim
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input/"
#' dir_output <- "/dir/to/ouput/"
#' season <- "month"
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
get_clim <- function(dir_input, dir_output, season, raster_function, var_name, shp_file, n_col, n_row, name_output, n_cores = 1, res = 300, height = 8,  width = 6) {
  tic(msg = "Duración total análisis")
  all_tif <- tibble(
  full_path = dir_ls(path = dir_input, regexp = ".tif$", recurse = T),
  archivo = basename(full_path)
) %>%
  mutate(week = case_when(season == "week" ~ str_extract(archivo, pattern = "w\\d+"))) %>%
  separate(archivo,
    into = c("year", "month_num", "month"), sep = "_",
    remove = FALSE, extra = "drop"
  )
  if (season == "year") {
  all_tif_split <- all_tif %>%
    group_by(year) %>%
    group_split() %>%
  setNames(unique(all_tif$year))
}
  if (season == "month") {
  all_tif_split <- all_tif %>%
    group_by(month) %>%
    group_split() %>%
  setNames(unique(all_tif$month))
}
  #####AGREGAR IF POR NUMERO DE ARCHIVOS#####
  if (season == "week") {
  all_tif_split <- all_tif %>%
    group_by(week) %>%
    group_split() %>%
    setNames(unique(all_tif$week))
}
  cat("\n\n Calculando climatologías...\n\n")
  cat("Paso 1: Generando stacks según estacionalidad seleccionada...\n\n")
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  list_stack <- with_progress({
    p <- progressor(steps = length(all_tif_split))
    future_map(all_tif_split, ~ {
      p()
      Sys.sleep(.2)
      read_stars(.$full_path, quiet = TRUE) %>% merge()
    }, .options = furrr_options(seed = TRUE))
  })
  cat("Paso 2: Generando climatologías según función seleccionada...\n\n")
      list_raster <- with_progress({
      p <- progressor(steps = length(list_stack))
      future_map(list_stack, ~ {
        p()
        Sys.sleep(.2)
        st_apply(X = ., MARGIN = 1:2, function(x) do.call(raster_function, list(x, na.rm = TRUE)))
      }, .options = furrr_options(seed = TRUE))
    })
  stopCluster(cl)
  rm(cl)
  # plot climatologia
  # config gral
  shp <- read_sf(shp_file) %>% st_geometry()
  df <- list_raster %>% map(., ~ as.data.frame(., XY = TRUE)) %>% bind_rows(.id = "season")
  if (season == "week") {
    df <- df %>% mutate(season = str_replace(season, pattern = "w", replacement = "semana "))
  }
  if (season == "month" ) {
    months <- names(list_raster)
    df <- df %>% mutate(season = str_to_sentence(season)) %>%
  mutate(across(season, factor, levels = str_to_sentence(months)))
    }
  cat("\n\n Generando gráfico...\n\n")
  if (var_name == "chlor_a") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = log10(X))) +
      scale_fill_gradientn(colours = oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~season, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(
        title = expression(paste("Log10", " ", Clorofila - alpha ~ (mg ~ m^{
          -3
        }))),
        title.position = "right",
        title.theme = element_text(angle = 90),
        barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5
      )) +
      theme_bw()
  }
  if (var_name == "sst") {
    blues <- rev(brewer.pal(9, "YlGnBu"))
    reds <- brewer.pal(9, "YlOrRd")
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = X)) +
      scale_fill_gradientn(colours = c(blues, reds), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~season, ncol = n_col, nrow = n_row, scales = "fixed") +
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
      scale_fill_gradientn(colours = oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~season, ncol = n_col, nrow = n_row, scales = "fixed") +
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
      geom_raster(aes(x, y, fill = X)) +
      scale_fill_gradientn(colours = oce::oce.colorsViridis(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~season, ncol = n_col, nrow = n_row, scales = "fixed") +
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
  dir_create(dir_output)
  #export png y tif de la climatología
  filename <-  paste0(dir_output, name_output, "_", min(all_tif$year), "_", max(all_tif$year))
  ggsave(filename =  paste0(filename, ".png"), plot = plot, device = "png", units = "in", dpi = res, height = heigth, width = width)
  #export el stack
  #coarse each layer to raster
  list <- map(list_raster, ~ as(.,"Raster"))
  stack <- stack(list) %>% st_as_stars()
  write_stars(obj = stack, dsn = paste0(filename, ".tif"))
  save(df, plot, file = paste0(dir_output, "plot_data_", var_name, "_", raster_function, ".RData"))
  toc()
}
