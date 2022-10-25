#' @title get_clim
#' @description Función para generar climatología y exportarla (formato png) para una variable determinada
#' @param dir_input directorio en donde se almacenan las imágenes raster (formato .tif)
#' @param dir_output directorio en donde se almacenará la imagen png
#' @param season temporalidad para la generación de imágenes en formato raster("mes", o "year")
#' @param stat_function función estadística para generar las imágenes raster ("median" o "mean")
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param shp_file nombre archivo shp para el gráfico (si no esta en dir_input poner nombre con ruta completa)
#' @param n_col numero de columnas para el gráfico
#' @param n_row numero de filas para el gráfico
#' @param name_output nombre para la salidas
#' @param res resolución para la imágen png de la climatología
#' @param height altura para la imágen png de la climatología
#' @param width amplitud para la imágen png de la climatología
#' @param xlim vector numérico tamaño 2 para los limites de x en el plot
#' @param ylim vector numérico tamaño 2 para los limites de y en el plot
#' @param ticks_x vector numérico tamaño 1 indicando ticks en el eje x del plot
#' @param ticks_y vector numérico tamaño 1 indicando ticks en el eje y del plot
#' @import ggplot2
#' @importFrom fs dir_ls dir_create dir_exists
#' @importFrom stars st_as_stars write_stars read_stars st_apply
#' @importFrom raster stack
#' @importFrom purrr map
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr mutate across group_split group_by case_when bind_rows
#' @importFrom purrr map
#' @importFrom tidyr separate pivot_longer
#' @importFrom sf read_sf st_geometry st_cast st_union st_as_sf st_intersects sf_use_s2 as_Spatial
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
#' dir_input <- "/dir/to/input"
#' dir_output <- "/dir/to/ouput"
#' season <- "month"
#' stat_function <- "median"
#' var_name <- "chlor_a"
#' n_col <- 3
#' n_row <- 4
#' name_output <- "climatologia_chlor_a.png"
#' shp_file <- "/home/evolecolab/Escritorio/test_satImg/raster_mensuales/resultados_raster/Golfo_Arauco_prj2.shp"
#' res <- 300
#' height <- 7
#' width <- 9
#' get_clim(dir_input = dir_input, dir_output = dir_output, season = season, stat_function = stat_function, var_name = var_name, shp_file = shp_file, n_col = n_col, n_row = n_row, name_output = name_output, res = res, height = height, width = width)
#' }
get_clim <- function(dir_input, dir_output, season, stat_function, var_name, shp_file, n_col, n_row, name_output, n_cores = 1, res = 300, height = 8, width = 6, xlim, ylim, ticks_x, ticks_y) {
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
      setNames(map(., ~ unique(.[["year"]])))
  }
  if (season == "month") {
    all_tif_split <- all_tif %>%
      group_by(month) %>%
      group_split() %>%
      setNames(map(., ~ unique(.[["month"]])))
  }
  ##### AGREGAR IF POR NUMERO DE ARCHIVOS#####
  if (season == "week") {
    all_tif_split <- all_tif %>%
      group_by(week) %>%
      group_split() %>%
      setNames(map(., ~ unique(.[["week"]])))
  }
  cat("\n\n Calculando climatologías...\n\n")
  cat("Paso 1: Generando stacks según estacionalidad seleccionada...\n\n")
  ## generar stacks
  full_path_list <- purrr::map(all_tif_split, ~ pull(., "full_path"))
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  list_stack <- with_progress({
    p <- progressor(steps = length(full_path_list))
    future_map(full_path_list, ~ {
      p()
      Sys.sleep(.2)
      # read_stars(.$full_path, quiet = TRUE) %>% merge()
      stack(.) %>%
        st_as_stars() %>%
        setNames(var_name)
    }, .options = furrr_options(seed = TRUE))
  })
  cat("Paso 2: Generando climatologías según función seleccionada...\n\n")
  list_raster <- with_progress({
    p <- progressor(steps = length(list_stack))
    future_map(list_stack, ~ {
      p()
      Sys.sleep(.2)
      st_apply(X = ., MARGIN = 1:2, function(x) do.call(stat_function, list(x, na.rm = TRUE)))
    }, .options = furrr_options(seed = TRUE))
  })
  stopCluster(cl)
  rm(cl)
  sf_use_s2(FALSE)
  shp_sf <- read_sf(shp_file) %>% st_geometry()
  #######
  shp_sp <- as_Spatial(shp_sf)
  list_raster <- map(list_raster, ~ raster::mask(., shp_sp, inverse = TRUE))
  list_df <- map(list_raster, ~ as.data.frame(., xy = TRUE))
  df <- bind_rows(list_df, .id = "season")
  rm(list_df)
  rm(list_stack)
  # plot climatologia:
  #shp_filter <- shp %>%
  #  st_cast() %>%
  #  st_union()
  #revisar en que parte se cae el ALGO
  #df_sf <- st_as_sf(x = df, coords = c("x", "y"), crs = 4326)
  #filtred_data <- st_intersects(df_sf, shp_filter, sparse = FALSE) %>% as.data.frame()
  #df <- df %>%
  #  mutate(flag = filtred_data$V1) %>%
  #  filter(flag == "FALSE")
  #rm(filtred_data)
  #rm(df_sf)
  if (season == "week") {
    df <- df %>% mutate(season = str_replace(season, pattern = "w", replacement = "semana "))
  }
  if (season == "month") {
    months_ordered <- c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")
    df <- df %>%
      mutate(season = str_to_sentence(season)) %>%
      mutate(across(season, factor, levels = str_to_sentence(months_ordered)))
  }
  cat("\n\n Generando gráfico...\n\n")
  if (var_name == "chlor_a") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = log10(chlor_a))) +
      scale_fill_gradientn(colours = oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = ticks_x) +
      scale_y_latitude(ticks = ticks_y) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
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
      geom_raster(aes(x, y, fill = sst)) +
      scale_fill_gradientn(colours = c(blues, reds), na.value = "white") +
      scale_x_longitude(ticks = ticks_x) +
      scale_y_latitude(ticks = ticks_y) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
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
    df <- df %>% mutate(valor_corrected = Rrs_645 * 158.9418)
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = valor_corrected)) +
      scale_fill_gradientn(colours = oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = ticks_x) +
      scale_y_latitude(ticks = ticks_y) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
      facet_wrap(~season, ncol = n_col, nrow = n_row, scales = "fixed") +
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
        barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5
      )) +
      theme_bw()
  }
  if (var_name == "nflh") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = nflh)) +
      scale_fill_gradientn(colours = oce::oce.colorsViridis(120), na.value = "white") +
      scale_x_longitude(ticks = ticks_x) +
      scale_y_latitude(ticks = ticks_y) +
      coord_equal() +
      geom_sf(data = shp_sf, fill = "grey80", col = "black") +
      coord_sf(xlim = xlim, ylim = ylim) +
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
  # export png y tif de la climatología
  filename <- paste0(dir_output, "/", name_output, "_", min(all_tif$year), "_", max(all_tif$year), "_", stat_function)
  ggsave(filename = paste0(filename, ".png"), plot = plot, device = "png", units = "in", dpi = res, height = height, width = width)
  # export el stack
  # coarse each layer to raster
  list <- map(list_raster, ~ as(., "Raster"))
  stack <- stack(list) %>% st_as_stars()
  write_stars(obj = stack, dsn = paste0(filename, ".tif"))
  save(df, plot, file = paste0(dir_output, "/plot_data_", var_name, "_", stat_function, ".RData"))
  rm(df)
  rm(plot)
  rm(shp)
  rm(list_raster)
  toc()
}
