#' @title get_raster_fix
#' @description Función para generar imágenes raster a partir de imágenes satelitales L3 (formato aaajulianday)
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param season temporalidad para la generación de imágenes en formato raster ("semana", "mes", año).
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean").  Por defecto, median
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return Imágenes raster
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move file_copy path_wd
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull case_when
#' @importFrom stringr str_split
#' @importFrom purrr walk walk2
#' @importFrom terra writeRaster rast
#' @importFrom raster raster stack calc
#' @importFrom furrr future_walk
#' @importFrom future plan multisession
#' @importFrom doParallel stopImplicitCluster
#' @export get_raster_fix
#' @examples
#'\dontrun{
#' dir_input <- "/home/evolecol/Escritorio/R_package/test_package/chlor_A"
#' dir_output <- paste0(dir_input, "/", "rasters")
#' season <- "mes"
#' raster_function <- "median"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' get_raster_fix(dir_input = dir_input, dir_output = dir_output, season = season,raster_function = raster_function,var_name = var_name,n_cores = n_cores)
#' }
get_raster_fix <- function(dir_input, dir_output, season = "mes", raster_function = "median", var_name, n_cores = 1) {
  cat("\n\n Configurando sistema de archivos...\n\n")
  all_nc <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE),
                   archivo = basename(ruta_completa),
                   fecha = as_date(archivo, format = "%Y%j"),
                   año = year(fecha),
                   mes = month(fecha),
                   mes_num = sprintf("%02d", mes),
                   mes_ch = month(fecha, label = TRUE, abbr = FALSE),
                   semana = week(fecha),
                   semana_num = sprintf("%02d", semana),
                   nombre_semana = paste0("s_", semana_num),
                   nombre_dir = case_when(season == "año" ~paste0(dir_output, "/", año),
                                          season == "mes" ~paste0(dir_output, "/", año,"/", mes_num, "_", mes_ch),
                                          season == "semana" ~paste0(dir_output, "/", año,"/", mes_num, "_", mes_ch, "/", nombre_semana)))
  nombre_dir <- all_nc %>% distinct(nombre_dir) %>% pull(nombre_dir)
  walk(nombre_dir, ~dir_create(., recurse = T))
  walk2(all_nc[, 1], all_nc[, 11], ~file_copy(.x, .y, overwrite = TRUE))
  cat("\n\n Listo...\n\n")
  #función interna para crear un raster a cada dir a cual se mueva
  cat("\n\n Iniciando creación de rasters...\n\n")
  internal_raster <- function(dir, raster_function) {
    setwd(dir)
    files <- dir_ls(regexp = ".nc$", recurse = T)
    tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
    n_files <- length(files)
    if (season == "año") {
      name_year <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", var_name)
      if (n_files <= 1) {
        raster <- raster(files, varname = var_name)
        raster <- rast(raster)
        writeRaster(x = raster, filename = paste0(name_file,".tif"), overwrite = TRUE)
      } else {
        #pasar nombre de las funciones a quoted
        raster <- stack(files, varname = var_name)
        if (raster_function == "median") {
          stack <- raster::calc(raster, fun = median, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
        if (raster_function == "mean") {
          stack <- raster::calc(raster, fun = mean, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
      }
    }
    if (season == "mes") {
      name_year <- tmp[length(tmp) - 1]
      name_month <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", name_month, "_", var_name)
      if (n_files <= 1) {
        raster <- raster(files, varname = var_name)
        raster <- rast(raster)
        writeRaster(x = raster, filename = paste0(name_file,".tif"), overwrite = TRUE)
      } else {

        raster <- stack(files, varname = var_name)
        if (raster_function == "median") {
          stack <- raster::calc(raster, fun = median, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
        if (raster_function == "mean") {
          stack <- raster::calc(raster, fun = mean, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
      }
    }
    if (season == "semana") {
      name_year <- tmp[length(tmp) - 2]
      name_month <- tmp[length(tmp) - 1]
      name_week <- tmp[length(tmp)]
      name_file <- paste0(name_year, "_", name_month, "_", name_week, "_",var_name)
      if (n_files <= 1) {
        raster <- raster(files, varname = var_name)
        raster <- rast(raster)
        writeRaster(x = raster, filename = paste0(name_file,".tif"), overwrite = TRUE)
      } else {
        raster <- stack(files, varname = var_name)
        if (raster_function == "median") {
          stack <- raster::calc(raster, fun = median, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
        if (raster_function == "mean") {
          stack <- raster::calc(raster, fun = mean, na.rm = TRUE)
          name_file <- paste0(name_file, "_", raster_function)
          stack <- rast(stack)
          writeRaster(x = stack, filename = paste0(name_file,".tif"), overwrite = TRUE)
        }
      }
    }
    setwd(dir_output)
  }
  #add progess bar
  if (n_cores == 1) {
    walk(nombre_dir, ~internal_raster(dir = ., raster_function = raster_function))
  } else {
    plan(multisession, workers = n_cores)
    future_walk(nombre_dir, ~internal_raster(dir = ., raster_function = raster_function), verbose = FALSE)
    stopImplicitCluster()
  }
  cat("\n\n Generación de rasters finalizada...\n\n")
  dir_create(path = paste0(dir_output,"/", "resultados_raster"))
  res_path <- paste0(dir_output,"/", "resultados_raster")
  all_tif <- dir_ls(path = dir_output, regexp = ".tif", type = "file", recurse = TRUE)
  walk(all_tif, ~file_move(path = ., new_path = res_path))
}
#' @title get_csv_fix
#' @description Función para generar imágenes raster a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return data.frame csv
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move file_copy path_wd
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull mutate
#' @importFrom stringr str_split
#' @importFrom purrr map map2
#' @importFrom terra writeRaster rast
#' @importFrom raster raster as.data.frame
#' @importFrom furrr future_walk
#' @importFrom future plan multisession
#' @importFrom doParallel stopImplicitCluster
#' @export get_csv_fix
#' @examples
#'\dontrun{
#' dir_input <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/chlor_a/"
#' dir_output <- "/home/evolecolab/Escritorio/test_satImg/test_get_L3/test_raster_fix"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' get_csv_fix(dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores)
#' }
get_csv_fix <- function(dir_input, dir_output, var_name, n_cores = 1) {
  cat("\n\n Configurando sistema de archivos...\n\n")
  all_nc <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE),
                   archivo = basename(ruta_completa),
                   fecha = as_date(archivo, format = "%Y%j"),
                   año = year(fecha),
                   mes = month(fecha),
                   mes_num = sprintf("%02d", mes),
                   mes_ch = month(fecha, label = TRUE, abbr = FALSE),
                   semana = week(fecha),
                   semana_num = sprintf("%02d", semana),
                   nombre_semana = paste0("s_", semana_num),
                   nombre_dir = paste0(dir_output, "/", año,"/", mes_num, "_", mes_ch))
  nombre_dir <- all_nc %>% distinct(nombre_dir) %>% pull(nombre_dir)
  walk(nombre_dir, ~dir_create(., recurse = T))
  walk2(all_nc[, 1], all_nc[, 11], ~file_copy(.x, .y, overwrite = TRUE))
  cat("\n\n Listo...\n\n")
  cat("\n\n Iniciando creación de csv...\n\n")
  internal_csv <- function(dir) {
    setwd(dir)
    files <- dir_ls(regexp = ".nc$", recurse = T)
    tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
    n_files <- length(files)
    name_year <- tmp[length(tmp) - 1]
    name_month <- tmp[length(tmp)]
    name_out <- paste0(name_year, "_",name_month, "_", var_name, ".csv")
    fecha <- all_nc %>% filter(archivo %in% files) %>% pull(fecha)
    raster_list <- map(files, ~raster::raster(., varname = var_name) %>% raster::as.data.frame(., xy = T))
    raster_list <- map2(raster_list, fecha, ~mutate(.x, fecha =.y))
    df <- bind_rows(raster_list)
    names(df)[1:3] <- c("longitud", "latitud", var_name)
    write_csv(df, file = paste0(name_year, "_", name_month, "_", var_name, ".csv"))
    setwd(dir_output)
  }
  #add progess bar
  if (n_cores == 1) {
    walk(nombre_dir, ~internal_csv(dir = .))
  } else {
    plan(multisession, workers = n_cores)
    future_walk(nombre_dir, ~internal_csv(dir = .), verbose = FALSE)
    stopImplicitCluster()
  }
  #mover todas las salidas a una carpeta llamada output
  dir_create(path = paste0(dir_output,"/", "resultados_csv"))
  res_path <- paste0(dir_output, "/", "resultados_csv")
  all_csv <- dir_ls(path = dir_output, regexp = ".csv$", type = "file", recurse = TRUE)
  walk(all_csv, ~file_move(path = ., new_path = res_path))
}
#' @title get_filled_raster
#' @description Función basada en el paquete Gapfill para predecir valores Nas en series temporales, funciona para set de datos 4-D
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran las imágenes en formato raster
#' @param shp_mask_file nombre del archivo usado como mascara (formato .shp)
#' @param season temporalidad para la generación de imágenes en formato raster ("semana", "mes"). Actualmente, solo funciona con "mes"
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @importFrom fs dir_ls dir_create dir_exists dir_delete file_move
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week
#' @importFrom dplyr distinct pull mutate group_split group_by
#' @importFrom stringr str_split
#' @importFrom purrr map map2 map_chr map_dbl
#' @importFrom terra writeRaster rast
#' @importFrom raster raster stack brick as.data.frame cellStats calc mask extent
#' @importFrom furrr future_walk
#' @importFrom future plan multisession
#' @importFrom doParallel stopImplicitCluster registerDoParallel
#' @importFrom tidyr separate
#' @importFrom gapfill Gapfill
#' @importFrom sf read_sf st_geometry as_Spatial
#' @return imágenes raster sin Na
#' @export get_filled_raster
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecol/Escritorio/R_package/test_package/rasters/resultados_raster/"
#' dir_output <- paste0("/home/evolecol/Escritorio/R_package/test_package", "/", "gapfill")
#' season <- "mes"
#' shp_mask_file <- "/home/evolecol/Escritorio/R_package/test_package/Golfo_Arauco_prj2.shp"
#' n_cores <- 4
#' get_filled_raster(dir_input = dir_input, dir_output = dir_output, shp_mask_file = shp_mask_file,season = season, n_cores = n_cores)
#' }
get_filled_raster <- function(dir_input, dir_output, shp_mask_file, season = "mes", n_cores = 1) {
  #agregar mensajes y barra de progreso
  all_tif <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".tif$", recurse = T),
                    archivo = basename(ruta_completa)) %>%
    separate(archivo, into = c("año", "numero_mes", "mes"), sep = "_",
             remove = FALSE, extra = "drop")
  all_tif_list <- all_tif %>%  group_by(mes) %>% group_split()
  month_names <- map_chr(all_tif_list, ~unique(.$mes))
  layer_names <- map(all_tif_list, ~paste0(.$mes, "_", .$año))
  names(all_tif_list) <- month_names
  stack_raw_list <- map(all_tif_list, ~stack(.$ruta_completa))
  names(stack_raw_list) <- month_names
  n_años <- length(unique(all_tif$año))
  n_mes_año <- 1L
  n_col <- dim(stack_raw_list[[1]])[2]
  n_row <- dim(stack_raw_list[[1]])[1]
  cat("\n\n Generando inputs...\n\n")
  tmp_list <- map(stack_raw_list, ~array(., dim = c(n_col,
                                                           n_row,
                                                           n_mes_año,
                                                           n_años)))
  input_array_list <- map(tmp_list, ~aperm(., c(2, 1, 3, 4)))
  min <- min(map_dbl(stack_raw_list, ~min(cellStats(., min, na.rm = TRUE)))
             %>% min() %>% as.numeric())
  max <- min(map_dbl(stack_raw_list, ~max(cellStats(., max, na.rm = TRUE)))
             %>% max() %>% as.numeric())
  cat("\n\n Generando predición para valores NAS...\n\n")
  registerDoParallel(n_cores)
  output_list <- map(input_array_list, ~Gapfill(data = ., dopar = TRUE, clipRange = c(min, max),verbose = FALSE))
  stopImplicitCluster()
  cat("\n\n Generando resultados...\n\n")
  output_array_list <- map(output_list, "fill")
  n_raster <- n_mes_año * n_años
  #este objeto contiene todos los meses rellenos
  output_stack_fill_list <- map(output_array_list, ~stack(brick(array(., c(n_row, n_col, n_raster)))))
  median_stack_fill_list <- map(output_stack_fill_list, ~calc(., fun = median, na.rm = TRUE))
  final_stack <- stack(median_stack_fill_list)
  raster::extent(final_stack) <- raster::extent(stack_raw_list[[1]])
  shp <- read_sf(shp_mask_file) %>% st_geometry() %>% as_Spatial()
  final_stack <- mask(final_stack, shp, inverse = TRUE)
  final_stack <- rast(final_stack)
  ifelse(!dir_exists(dir_output), dir_create(dir_output), FALSE)
  terra::writeRaster(x = final_stack, filename = paste0(dir_output, "/", "all_month_median_filled.tif"), overwrite = TRUE)
  #traducir a map para consistencia del código
  for (i in 1:length(output_stack_fill_list)) {
    names(output_stack_fill_list[[i]]) <- layer_names[[i]]
    raster::extent(output_stack_fill_list[[i]]) <- raster::extent(stack_raw_list[[1]])
    output_stack_fill_list[[i]] <- mask(output_stack_fill_list[[i]], shp, inverse = TRUE)
    output_stack_fill_list[[i]] <- rast(output_stack_fill_list[[i]])
  }
  walk2(output_stack_fill_list, month_names, ~writeRaster(.x, paste0(dir_output, "/", .y, "_filled.tif"), overwrite = TRUE) )
}
#' @title get_raster_ct
#' @description Función para generar imágenes raster a partir de imágenes L3 para un intervalo de tiempo determinado (desde intervalos diarios en adelante)
#' @param dir_input directorio en donde se almacenas las imágenes L3
#' @param dir_output directorio en donde se almacenaras las imágenes en formato raster
#' @param date_1 fecha inicio del tiempo personalizado (formato: AAAA-MM-DD)
#' @param date_2 fecha termino del tiempo personalizado (formato: AAAA-MM-DD)
#' @param name_time vector tamaño 1 que indica el nombre del intervalo temporal ("eg. "semana 1")
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean")
#' @importFrom fs dir_ls dir_create file_copy
#' @importFrom tibble tibble
#' @importFrom lubridate as_date
#' @importFrom dplyr filter between
#' @importFrom purrr walk
#' @importFrom terra writeRaster rast
#' @importFrom raster raster stack calc extent crs resample
#' @return imágenes raster
#' @export get_raster_ct
#' @examples
#' \dontrun{
#' dir_input <- "/home/evolecol/Escritorio/R_package/test_package/chlor_A/"
#' dir_output <- paste0( "/home/evolecol/Escritorio/R_package/test_package/test_ct")
#' date_1 <- "2010-01-01"
#' date_2 <- "2010-01-29"
#' name_time <- "enero_2010"
#' var_name <- "chlor_a"
#' raster_function <- "median"
#' get_raster_ct(dir_input = dir_input, dir_output = dir_output, date_1 = date_1, date_2 = date_2, name_time = name_time,var_name = var_name, raster_function = raster_function)
#' }
get_raster_ct <- function(dir_input, dir_output, date_1, date_2, name_time, var_name, raster_function = "median") {
  range_time <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".nc$", recurse = TRUE),
                   archivo = basename(ruta_completa),
                   fecha = as_date(archivo, format = "%Y%j"),
                   name_folder = paste0(dir_output, "/", name_time)) %>%
    filter(between(fecha, as_date(date_1), as_date(date_2)))
  name_folder <- paste0(dir_output, "/", name_time)
  dir_create(name_folder)
  walk(range_time[, 1], ~file_copy(path = ., new_path = name_folder, overwrite = TRUE))
  setwd(name_folder)
  files <- dir_ls(regexp = ".nc$", recurse = T)
  n_files <- length(files)
  if (n_files <= 1) {
    raster <- raster(files, varname = var_name)
    raster::crs(raster) <- "+proj=longlat"
    name_file <- paste0(var_name, "_" , name_time, "_1file", ".tif")
    raster <- rast(raster)
    writeRaster(x = raster, filename = name_file, overwrite = TRUE)
    } else {
      raster <- stack(files, varname = var_name)
      ext_df <- raster::extent(raster)
      ext_mask <- raster(ext_df,  resolution = 0.01)
      raster <- resample(raster, ext_mask, method = "bilinear")
      #pasar nombre a quoted#
      if (raster_function == "median") {
       stack <- calc(raster, fun = median, na.rm = TRUE)
       raster::crs(stack) <- "+proj=longlat"
       stack <- rast(stack)
       writeRaster(x = stack, filename = paste0(var_name, "_", name_time, "_" ,raster_function, ".tif"), overwrite = TRUE)
     }
      if (raster_function == "mean") {
      stack <- calc(raster, fun = mean, na.rm = TRUE)
      raster::crs(stack) <- "+proj=longlat"
      stack <- rast(stack)
      writeRaster(x = stack, filename = paste0(var_name, "_", name_time, "_" ,raster_function, ".tif"), overwrite = TRUE)
      }
    }
  setwd(dir_input)
}
#' @title get_clim
#' @description Función para generar climatología y exportarla (formato png) para una variable determinada
#' @param dir_input directorio en donde se almacenan las imágenes raster (formato .tif)
#' @param dir_output directorio en donde se almacenará la imagen png
#' @param season temporalidad para la generación de imágenes en formato raster("mes", o "año")
#' @param raster_function función estadística para generar las imágenes raster ("median" o "mean")
#' @param shp_file nombre archivo shp para el gráfico (si no esta en dir_input poner nombre con ruta completa)
#' @param n_col numero de columnas para el gráfico
#' @param n_row numero de filas para el gráfico
#' @param height altura imagen de salida
#' @param width ancho tamaño de salida
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
#' @importFrom stringr str_remove
#' @return imágenes raster
#' @export get_clim
#' @examples
#' \dontrun{
#'dir_input <- "/home/evolecolab/Escritorio/test_satImg/raster_mensuales/resultados_raster/"
#'dir_output <- "/home/evolecolab/Escritorio/test_satImg/climatologia"
#'season <- "mes"
#'raster_function <- "median"
#'var_name <-  "chlor_a"
#'n_col <- 3
#'n_row <- 4
#'name_output <- "climatologia_chlor_a.png"
#'shp_file <- "/home/evolecolab/Escritorio/test_satImg/raster_mensuales/resultados_raster/Golfo_Arauco_prj2.shp"
#'res <- 300
#'heigth <- 7
#'width <- 9
#'get_clim(dir_input = dir_input, dir_output = dir_output, season = season ,raster_function = raster_function , var_name = var_name, shp_file = shp_file, n_col = n_col, n_row = n_row, name_output = name_output,res = res, heigth = heigth, width = width)
#'}
get_clim <- function(dir_input, dir_output, season, raster_function, var_name, shp_file, n_col,
                     n_row, name_output, res, heigth, width) {
  all_tif <- tibble(ruta_completa = dir_ls(path = dir_input, regexp = ".tif$", recurse = T),
                    archivo = basename(ruta_completa)) %>%
    separate(archivo, into = c("año", "numero_mes", "mes", "var_name"), sep = "_",
           remove = FALSE, extra = "drop")
  if (season == "año") {
    all_tif_split <- all_tif %>% group_by(año) %>% group_split()
    names <- all_tif %>% distinct(año) %>% pull(año)
    names(all_tif_split) <- names
  }
  if (season == "mes") {
  all_tif_split <- all_tif %>% group_by(numero_mes) %>% group_split()
  names <- all_tif %>% distinct(mes) %>% pull(mes)
  names(all_tif_split) <- names
  }
  #if (season == "semana") {
  #  all_tif_split <- all_tif %>% group_by(semana) %>% group_split()
  #  names <- all_tif %>% distinct(semana) %>% pull(semana)
  #  names(all_tif_split) <- names
  #}
  stack_list <- map(all_tif_split, ~stack(.$ruta_completa))
  if (raster_function == "median") {
    stack <- map(stack_list, ~calc(., fun = median, na.rm = TRUE))
    stack <- stack(stack)
    }
  if (raster_function == "mean") {
    stack <- map(stack_list, ~calc(., fun = mean, na.rm = TRUE))
    stack <- stack(stack)
  }
  names(stack) <- names
  #plot climatologia
  #config gral
  shp <- read_sf(shp_file) %>% st_geometry()
  df <- stack %>% rasterToPoints() %>%
    as_tibble() %>%
    pivot_longer(cols = 3:last_col(), names_to = "facet_var", values_to = "valor") %>%
    mutate(across(facet_var, factor, levels = names(stack))) %>%
    mutate(facet_var = str_remove(facet_var, pattern = "X"))
  if (var_name == "chlor_a") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = log(valor))) +
      scale_fill_gradientn(colours = oce::oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(title = expression(paste("Log"," ", Clorofila-alpha~(mg~m^{-3}))),
                                   title.position = "right",
                                   title.theme = element_text(angle = 90),
                                   barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5)) +
      theme_bw()
    #para tratar de poner eje y en formato 10^.x
    #scale_fill_continuous(labels = trans_format("log", math_format(10^.x)))
  }
  if (var_name == "sst") {
    blues <-  rev(brewer.pal(9, "YlGnBu"))
    reds <-  brewer.pal(9, "YlOrRd")
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = valor)) +
      scale_fill_gradientn(colours = c(blues, reds), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(title = "Temperatura Superficial Mar (°C)",
                                   title.position = "right",
                                   title.theme = element_text(angle = 90),
                                   barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5)) +
      theme_bw()
  }
  if (var_name == "Rrs_645") {
    plot <- ggplot(df) +
      geom_raster(aes(x, y, fill = valor)) +
      scale_fill_gradientn(colours = oce::oce.colorsJet(120), na.value = "white") +
      scale_x_longitude(ticks = .2) +
      scale_y_latitude(ticks = .2) +
      coord_equal() +
      geom_sf(data = shp, fill = "grey80", col = "black") +
      facet_wrap(~facet_var, ncol = n_col, nrow = n_row, scales = "fixed") +
      guides(fill = guide_colorbar(title = expression(paste("nWLR 645" ,
                                                            " (",
                                                            "mW ",
                                                            cm^-2,
                                                            um^-1,sr^-1,
                                                            ")" )),
                                   title.position = "right",
                                   title.theme = element_text(angle = 90),
                                   barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5)) +
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
      guides(fill = guide_colorbar(title = expression(paste("nLFH" ,
                                                            " (",
                                                            "mW ",
                                                            cm^-2,
                                                            um^-1,
                                                            sr^-1,
                                                            ")" )),
                                   title.position = "right",
                                   title.theme = element_text(angle = 90),
                                   barwidth = unit(.5, "cm"), barheight = unit(7.5, "cm"), title.hjust = .5)) +
      theme_bw()
  }
  ifelse(!dir_exists(dir_output), dir_create(dir_output), FALSE)
  ggsave(filename = paste0(dir_output, "/", name_output), plot = plot, device = "png", units = "in", dpi = res, height = heigth, width = width)
  stack <- rast(stack)
  writeRaster(x = stack, filename = paste0(dir_output, "/", "raster_climatologia.tif"), overwrite = TRUE)
  save(df, plot, file = paste0(dir_output, "/", "plot_data.RData"))
}
