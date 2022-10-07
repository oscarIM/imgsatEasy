#' @title get_raster
#' @description Función para generar imágenes raster o archivos csv a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3 (formato .nc)
#' @param dir_output directorio de destino
#' @param season temporalidad para la generación de imágenes en formato raster. Opciones "year", "month", "week"
#' @param stat_function función estadística para generar los compuestos ("median", "mean", etc).Por defecto, "median"
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @param custom_time se necesita un intervalo particular de tiempo?. TRUE/FALSE. Si TRUE hay que indicar begin_day y end_day. Si FALSE, se considerara el intervalo de tiempo comprendido por todas las imágenes
#' @param begin_day fecha inicio (formato aaaa-mm-dd)
#' @param end_day fecha de termino (formato aaaa-mm-dd)
#' @return imágenes raster o archivos parquet
#' @importFrom fs dir_ls dir_create path_wd path_file fs_path
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week day
#' @importFrom dplyr case_when group_by group_split rename mutate between filter pull
#' @importFrom purrr map map_int walk2 keep
#' @importFrom stars write_stars st_as_stars
#' @importFrom raster stack calc
#' @importFrom furrr furrr_options future_walk2
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom tictoc tic toc
#' @importFrom progressr with_progress progressor
#' @export get_raster
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input/"
#' dir_output <- "/dir/to/output/"
#' season <- "month"
#' stat_function <- "median"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' custom_time <- FALSE
#' fecha1 <- "2022-04-18"
#' fecha2 <- "2022-04-24"
#' get_raster_csv_ct(dir_input = dir_input, dir_output = dir_output, season = season, stat_function = stat_function, var_name = var_name, n_cores = n_cores)
#' }
get_raster <- function(dir_input, dir_output, season = "months", var_name, n_cores = 1, stat_function = "median", custom_time = FALSE, begin_day = NULL, end_day = NULL) {
  current_wd <- path_wd()
  tic(msg = "Duración total análisis")
  # setwd(dir_input)
  cat("Leyendo imágenes L3... \n\n")
  files <- dir_ls(path = dir_input, regexp = "_L3mapped.nc$", recurse = FALSE) %>%
    tibble(
      mapped_files = .,
      date = case_when(
        var_name == "sst" ~ as_date(path_file(mapped_files), format = "%Y%m%d"),
        TRUE ~ as_date(path_file(mapped_files), format = "%Y%m%d")
      ),
      year = year(date),
      day = day(date),
      month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE)),
      week_name = paste0("w", sprintf("%02d", week(date)))
    )
  if (custom_time) {
    files <- files %>%
      filter(between(date, as_date(begin_day), as_date(end_day)))
  }
  #### definir la lista y los nombres de salida según temporalidad seleccionada
  dir <- dir_create(path = paste0(dir_output, "/all_rasters")) %>% fs_path()
  #### fn para obtener un raster según función seleccionada#####################
  get_raster <- function(files, file_out) {
    stack <- raster::stack(files, varname = var_name)
    stack <- raster::calc(stack, fun = function(x) do.call(stat_function, list(x, na.rm = TRUE))) %>% st_as_stars()
    write_stars(obj = stack, dsn = file_out)
  }
  ##############################################################################
  if (season == "year") {
    cat("Generando y exportando rasters según estacionalidad y función estadística seleccionada...\n\n")
    list_files <- files %>%
      group_by(year) %>%
      group_split()
    name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", var_name, "_", stat_function, ".tif"))
    files <- map(list_files, ~ pull(., "mapped_files"))
    cl <- makeForkCluster(n_cores)
    plan(cluster, workers = cl)
    with_progress({
      p <- progressor(steps = length(files))
      future_walk2(files, name_out, ~ {
        p()
        Sys.sleep(.2)
        get_raster(files = .x, file_out = .y)
      }, .options = furrr_options(seed = TRUE))
    })
    stopCluster(cl)
    rm(cl)
    cat("Fin...\n\n")
  }
  if (season == "month") {
    cat("Generando y exportando rasters según estacionalidad y función estadística seleccionada...\n\n")
    list_files <- files %>%
      group_by(year, month_name) %>%
      group_split()
    name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", var_name, "_", stat_function, ".tif"))
    files <- map(list_files, ~ pull(., "mapped_files"))
    if (n_cores >= 2) {
      cl <- makeForkCluster(n_cores)
      plan(cluster, workers = cl)
      with_progress({
        p <- progressor(steps = length(files))
        future_walk2(files, name_out, ~ {
          p()
          Sys.sleep(.2)
          get_raster(files = .x, file_out = .y)
        }, .options = furrr_options(seed = TRUE))
      })
      stopCluster(cl)
      rm(cl)
      cat("Fin...\n\n")
    }
    if (n_cores == 1) {
      with_progress({
        p <- progressor(steps = length(files))
        walk2(files, name_out, ~ {
          p()
          Sys.sleep(.2)
          get_raster(files = .x, file_out = .y)
        })
      })
    }
  }
  if (season == "week") {
    # arreglar
    cat("Generando y exportando rasters según estacionalidad y función estadística seleccionada...\n\n")
    list_files <- files %>%
      group_by(year, month_name, week_name) %>%
      group_split()
    nfiles_by_week <- map_int(list_files, ~ nrow(.))
    if (any(nfiles_by_week) < 2) {
      raster_list <- list_files %>% keep(~ nrow(.) < 2)
      raster_to_write <- map(raster_list, ~ read_stars(.$mapped_files, quiet = TRUE, sub = var_name, proxy = TRUE))
      name_out_raster <- map_chr(raster_to_write, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", unique(.["week_name"]), "_", unique(.["day"]), "_unique", "_", var_name, "_", stat_function, ".tif"))
      walk2(raster_to_write, name_out_raster, ~ write_stars(obj = .x, dsn = .y))
    }
    list_files <- list_files %>% keep(~ nrow(.) < 2)
    name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", unique(.["week_name"]), "_", var_name, "_", stat_function, ".tif"))
    files <- map(list_files, ~ pull(., "mapped_files"))
    cl <- makeForkCluster(n_cores)
    plan(cluster, workers = cl)
    with_progress({
      p <- progressor(steps = length(files))
      future_walk2(files, name_out, ~ {
        p()
        Sys.sleep(.2)
        get_raster(files = .x, file_out = .y)
      }, .options = furrr_options(seed = TRUE))
    })
    stopCluster(cl)
    rm(cl)
    cat("Fin...\n\n")
  }
  setwd(current_wd)
  toc()
}
