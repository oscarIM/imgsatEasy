#' @title get_raster_csv
#' @description Función para generar imágenes raster o archivos csv a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3 (formato .nc)
#' @param dir_output directorio de destino
#' @param season temporalidad para la generación de imágenes en formato raster. Opciones "year", "month", "week"
#' @param raster_function función estadística para generar los compuestos ("median" o "mean").  Por defecto, median
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @param result_type tipo de salida requerida. Si result_type == "raster", se generaran imágenes raster según la estacionalidad y función para los compuestos definidos. Si result_type =="csv", se generar archivos año/mes en fomato csv
#' @return imágenes raster o archivos csv
#' @importFrom fs dir_ls dir_create path_wd path_file fs_path
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week day
#' @importFrom dplyr case_when group_by group_split rename mutate
#' @importFrom tidyr drop_na separate
#' @importFrom purrr map map_int walk map_chr keep walk2 map2
#' @importFrom stars read_stars st_apply write_stars st_set_dimensions
#' @importFrom furrr future_walk future_map furrr_options
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom tictoc tic toc
#' @importFrom progressr with_progress progressor
#' @importFrom readr write_csv
#' @export get_raster_csv
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input"
#' dir_output <- "/dir/to/output"
#' season <- "month"
#' raster_function <- "median"
#' var_name <- "chlor_a"
#' result_type <- "raster"
#' n_cores <- 4
#' get_raster_csv(dir_input = dir_input, dir_output = dir_output, season = season, raster_function = raster_function, var_name = var_name, n_cores = n_cores, result_type = "raster")
#' }
get_raster_csv <- function(dir_input, dir_output, season = "month", raster_function = "median", result_type, var_name, n_cores = 1) {
  current_wd <- path_wd()
  tic(msg = "Duración total análisis")
  setwd(dir_input)
  cat("Leyendo imágenes L3... \n\n")
  files <- dir_ls(path = dir_input, regexp = "_L3mapped.nc$", recurse = TRUE) %>%
    tibble(
      mapped_files = .,
      date = case_when(
        var_name == "sst" ~ as_date(path_file(mapped_files), format = "%Y%m%d"),
        TRUE ~ as_date(path_file(mapped_files), format = "%Y%j")
      ),
      year = year(date),
      day = day(date),
      month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE)),
      week_name = paste0("w", sprintf("%02d", week(date)))
    )
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  if (result_type == "raster") {
    if (season == "year") {
      list_files <- files %>%
        group_by(year) %>%
        group_split()
      cat("Paso 1: Generando stacks según estacionalidad seleccionada...\n\n")
      list_stack <- with_progress({
        p <- progressor(steps = length(list_files))
        future_map(list_files, ~ {
          p()
          Sys.sleep(.2)
          read_stars(.$mapped_files, quiet = TRUE, sub = var_name) %>% merge()
        }, .options = furrr_options(seed = TRUE))
      })
      cat("Paso 2: Generando compuestos según función seleccionada...\n\n")
      if (raster_function == "median") {
        list_raster <- with_progress({
          p <- progressor(steps = length(list_stack))
          future_map(list_stack, ~ {
            p()
            Sys.sleep(.2)
            st_apply(X = ., MARGIN = 1:2, function(x) median(x, na.rm = T))
          }, .options = furrr_options(seed = TRUE))
        })
      }
      if (raster_function == "mean") {
        list_raster <- with_progress({
          p <- progressor(steps = length(list_stack))
          future_map(list_stack, ~ {
            p()
            Sys.sleep(.2)
            st_apply(X = ., MARGIN = 1:2, function(x) mean(x, na.rm = T))
          }, .options = furrr_options(seed = TRUE))
        })
      }
      cat("Exportando rasters anuales...\n\n")
      dir <- dir_create(path = paste0(dir_output, "/", "all_rasters")) %>% fs_path()
      name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", raster_function, ".tif"))
      walk2(list_raster, name_out, ~ write_stars(obj = .x, dsn = .y))
      ### agregar función de mover todo a carpetas separadas
      toc()
      stopCluster(cl)
      rm(cl)
      setwd(current_wd)
      cat("Fin")
    }
    if (season == "month") {
      list_files <- files %>%
        group_by(year, month_name) %>%
        group_split()
      cat("Paso 1: Generando stacks según estacionalidad seleccionada...\n\n")
      list_stack <- with_progress({
        p <- progressor(steps = length(list_files))
        future_map(list_files, ~ {
          p()
          Sys.sleep(.2)
          read_stars(.$mapped_files, quiet = TRUE, sub = var_name) %>% merge()
        }, .options = furrr_options(seed = TRUE))
      })
      cat("Paso 2: Generando compuestos según función seleccionada...\n\n")
      if (raster_function == "median") {
        list_raster <- with_progress({
          p <- progressor(steps = length(list_stack))
          future_map(list_stack, ~ {
            p()
            Sys.sleep(.2)
            st_apply(X = ., MARGIN = 1:2, function(x) median(x, na.rm = T))
          }, .options = furrr_options(seed = TRUE))
        })
      }
      if (raster_function == "mean") {
        list_raster <- with_progress({
          p <- progressor(steps = length(list_stack))
          future_map(list_stack, ~ {
            p()
            Sys.sleep(.2)
            st_apply(X = ., MARGIN = 1:2, function(x) mean(x, na.rm = T))
          }, .options = furrr_options(seed = TRUE))
        })
      }
      cat("Exportando archivos rasters mensuales...\n\n")
      dir <- dir_create(path = paste0(dir_output, "/", "all_rasters")) %>% fs_path()
      name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", raster_function, ".tif"))
      walk2(list_raster, name_out, ~ write_stars(obj = .x, dsn = .y))
      ### agregar función de mover todo a carpetas separadas
      toc()
      stopCluster(cl)
      rm(cl)
      setwd(current_wd)
      cat("Fin")
    }
    if (season == "week") {
      list <- files %>%
        group_by(year, month_name, week_name) %>%
        group_split()
      # cosas especiales para casos en donde solo hay un archivo por temporalidad
      files_by_season <- map_int(list, ~ nrow(.))
      to_write <- list %>% keep(~ nrow(.) < 2)
      stack_to_write <- map(to_write, ~ read_stars(.$mapped_files, quiet = TRUE, sub = var_name))
      dir <- dir_create(path = paste0(dir_output, "/", "all_rasters")) %>% fs_path()
      name_out_w <- map_chr(to_write, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", unique(.["week_name"]), "_", unique(.["day"]), "_unique", ".tif"))
      list_files <- list %>% keep(~ nrow(.) > 1)
      cat("Paso 1: Generando stacks según estacionalidad seleccionada...\n\n")
      list_stack <- with_progress({
        p <- progressor(steps = length(list_files))
        future_map(list_files, ~ {
          p()
          Sys.sleep(.2)
          read_stars(.$mapped_files, quiet = TRUE, sub = var_name) %>% merge()
        }, .options = furrr_options(seed = TRUE))
      })
      cat("Paso 2: Generando compuestos según función seleccionada...\n\n")
      if (raster_function == "median") {
        list_raster <- with_progress({
          p <- progressor(steps = length(list_stack))
          future_map(list_stack, ~ {
            p()
            Sys.sleep(.2)
            st_apply(X = ., MARGIN = 1:2, function(x) median(x, na.rm = T))
          }, .options = furrr_options(seed = TRUE))
        })
      }
      if (raster_function == "mean") {
        list_raster <- with_progress({
          p <- progressor(steps = length(list_stack))
          future_map(list_stack, ~ {
            p()
            Sys.sleep(.2)
            st_apply(X = ., MARGIN = 1:2, function(x) mean(x, na.rm = T))
          }, .options = furrr_options(seed = TRUE))
        })
      }
      cat("Exportando archivos rasters semanales...\n\n")
      name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", unique(.["week_name"]), "_", raster_function, ".tif"))
      walk2(stack_to_write, name_out_w, ~ write_stars(obj = .x, dsn = .y))
      walk2(list_raster, name_out, ~ write_stars(obj = .x, dsn = .y))
      toc()
      stopCluster(cl)
      rm(cl)
      setwd(current_wd)
      cat("Fin")
    }
  }
  if (result_type == "csv") {
    list_files <- files %>%
      group_by(year, month_name) %>%
      group_split()
    tic(msg = "Duración análisis")
    list_raw <- with_progress({
      p <- progressor(steps = length(list_files))
      future_map(list_files, ~ {
        p()
        Sys.sleep(.2)
        read_stars(.$mapped_files, quiet = TRUE, sub = var_name) %>%
          merge()
      }, .options = furrr_options(seed = TRUE))
    })
    cat("Transformando imágenes L3 a data.frames...\n\n")
    Sys.sleep(.2)
    list_csv_tmp <- with_progress({
      p <- progressor(steps = length(list_raw))
      future_map(list_raw, ~ {
        p()
        Sys.sleep(.2)
        st_set_dimensions(., names = c("x", "y", "date")) %>%
          setNames(var_name) %>%
          as.data.frame()
      }, .options = furrr_options(seed = TRUE))
    })

    cat("Formateando data.frames...\n\n")
    list_csv <- with_progress({
      p <- progressor(steps = length(list_csv_tmp))
      future_map(list_csv_tmp, ~ {
        p()
        Sys.sleep(.2)
        separate(., col = date, into = "date", sep = "_", extra = "drop") %>%
          drop_na(all_of(var_name)) %>%
          mutate(date = case_when(
            var_name == "sst" ~ as_date(date, format = "%Y%m%d"),
            TRUE ~ as_date(date, format = "%Y%j")
          ))
      }, .options = furrr_options(seed = TRUE))
    })
    cat("Exportando archivos csv mensuales...\n\n")
    dir <- dir_create(path = paste0(dir_output, "/", "all_csv")) %>% fs_path()
    name_out <- map_chr(list_files, ~ paste0(dir, "/", unique(.["year"]), "_", unique(.["month_name"]), "_", var_name, ".csv"))
    walk2(list_csv, name_out, ~ write_csv(x = .x, file = .y))
    toc()
    stopCluster(cl)
    rm(cl)
    setwd(current_wd)
    cat("Fin")
  }
}
