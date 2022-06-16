#' @title get_filled_raster
#' @description Función basada en el paquete Gapfill para predecir valores Nas en series temporales, funciona para set de datos 4-D
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param shp_mask_file nombre del file usado como mascara (formato .shp)
#' @param season temporalidad para la generación de imágenes en formato raster ("week", "month"). Actualmente, solo funciona con "month"
#' @param n_cores vector tamyear 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull mutate group_split group_by
#' @importFrom stringr str_split
#' @importFrom purrr map map_chr map_dbl
#' @importFrom terra writeRaster rast
#' @importFrom raster stack brick cellStats calc mask extent
#' @importFrom furrr future_walk
#' @importFrom future plan cluster makeClusterPSOCK
#' @importFrom parallel stopCluster
#' @importFrom tidyr separate
#' @importFrom gapfill Gapfill
#' @importFrom sf read_sf st_geometry as_Spatial
#' @return imágenes raster sin Nas
#' @export get_filled_raster
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecol/Escritorio/R_package/test_package/rasters/resultados_raster/"
#' dir_output <- paste0("/home/evolecol/Escritorio/R_package/test_package", "/", "gapfill")
#' season <- "month"
#' shp_mask_file <- "/home/evolecol/Escritorio/R_package/test_package/Golfo_Arauco_prj2.shp"
#' n_cores <- 4
#' get_filled_raster(dir_input = dir_input, dir_output = dir_output, shp_mask_file = shp_mask_file, season = season, n_cores = n_cores)
#' }
get_filled_raster <- function(dir_input, dir_output, shp_mask_file, season = "month", n_cores = 1) {
  # agregar mensajes y barra de progreso
  all_tif <- tibble(
    full_path = dir_ls(path = dir_input, regexp = ".tif$", recurse = T),
    file = basename(full_path)
  ) %>%
    separate(file,
      into = c("year", "month_num", "month"), sep = "_",
      remove = FALSE, extra = "drop"
    )
  all_tif_list <- all_tif %>%
    group_by(month) %>%
    group_split()
  month_names <- map_chr(all_tif_list, ~ unique(.$month))
  layer_names <- map(all_tif_list, ~ paste0(.$month, "_", .$year))
  names(all_tif_list) <- month_names
  stack_raw_list <- map(all_tif_list, ~ stack(.$full_path))
  names(stack_raw_list) <- month_names
  n_years <- length(unique(all_tif$year))
  n_month_year <- 1L
  n_col <- dim(stack_raw_list[[1]])[2]
  n_row <- dim(stack_raw_list[[1]])[1]
  cat("\n\n Generando inputs...\n\n")
  tmp_list <- map(stack_raw_list, ~ array(., dim = c(
    n_col,
    n_row,
    n_month_year,
    n_years
  )))
  input_array_list <- map(tmp_list, ~ aperm(., c(2, 1, 3, 4)))
  min <- min(map_dbl(stack_raw_list, ~ min(cellStats(., min, na.rm = TRUE)))
  %>% min() %>% as.numeric())
  max <- min(map_dbl(stack_raw_list, ~ max(cellStats(., max, na.rm = TRUE)))
  %>% max() %>% as.numeric())
  cat("\n\n Generando predición para valores faltantes...\n\n")
  cl <- makeForkCluster(n_cores)
  registerDoParallel(cl)
  output_list <- map(input_array_list, ~ Gapfill(data = ., dopar = TRUE, clipRange = c(min, max), verbose = FALSE))
  stopCluster(cl)
  cat("\n\n Generando resultados...\n\n")
  output_array_list <- map(output_list, "fill")
  n_raster <- n_month_year * n_years
  # este objeto contiene todos los monthes rellenos
  output_stack_fill_list <- map(output_array_list, ~ stack(brick(array(., c(n_row, n_col, n_raster)))))
  median_stack_fill_list <- map(output_stack_fill_list, ~ calc(., fun = median, na.rm = TRUE))
  final_stack <- stack(median_stack_fill_list)
  raster::extent(final_stack) <- raster::extent(stack_raw_list[[1]])
  shp <- read_sf(shp_mask_file) %>%
    st_geometry() %>%
    as_Spatial()
  final_stack <- mask(final_stack, shp, inverse = TRUE)
  final_stack <- rast(final_stack)
  ifelse(!dir_exists(dir_output), dir_create(dir_output), FALSE)
  writeRaster(x = final_stack, filename = paste0(dir_output, "/", "all_month_median_filled.tif"), overwrite = TRUE)
  # traducir a map para consistencia del código
  for (i in 1:length(output_stack_fill_list)) {
    names(output_stack_fill_list[[i]]) <- layer_names[[i]]
    raster::extent(output_stack_fill_list[[i]]) <- raster::extent(stack_raw_list[[1]])
    output_stack_fill_list[[i]] <- mask(output_stack_fill_list[[i]], shp, inverse = TRUE)
    output_stack_fill_list[[i]] <- rast(output_stack_fill_list[[i]])
  }
  walk2(output_stack_fill_list, month_names, ~ writeRaster(.x, paste0(dir_output, "/", .y, "_filled.tif"), overwrite = TRUE))
}
