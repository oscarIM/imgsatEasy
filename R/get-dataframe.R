#' @title get_dataframe
#' @description Función para generar dataframes en formato .parquet a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3 (formato .nc)
#' @param dir_output directorio de destino
#' @param stat_function función estadística para generar los compuestos diarios ("median", "mean", etc).Por defecto, "median"
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param custom_time se necesita un intervalo particular de tiempo?. TRUE/FALSE. Si TRUE hay que indicar begin_day y end_day. Si FALSE, se considerara el intervalo de tiempo comprendido por todas las imágenes
#' @param begin_day fecha inicio (formato aaaa-mm-dd)
#' @param end_day fecha de termino (formato aaaa-mm-dd)
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return archivos parquet
#' @importFrom fs dir_ls dir_create path_wd path_file fs_path file_delete
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week day
#' @importFrom dplyr case_when group_by group_split filter pull bind_rows mutate
#' @importFrom tidyr drop_na
#' @importFrom purrr map walk
#' @importFrom raster stack calc
#' @importFrom furrr furrr_options future_walk2
#' @importFrom future plan cluster
#' @importFrom parallel stopCluster makeForkCluster
#' @importFrom tictoc tic toc
#' @importFrom progressr with_progress progressor
#' @importFrom arrow write_parquet read_parquet
#' @export get_dataframe
#' @examples
#' \dontrun{
#' dir_input <- "/dir/to/input/"
#' dir_output <- "/dir/to/output/"
#' stat_function <- "median"
#' var_name <- "chlor_a"
#' n_cores <- 4
#' fecha1 <- "2022-04-18"
#' fecha2 <- "2022-04-24"
#' get_dataframe(dir_input = dir_input, dir_output = dir_output, stat_function = stat_function, var_name = var_name, n_cores = n_cores)
#' }
get_dataframe <- function(dir_input, dir_output, var_name, stat_function = "median", custom_time = FALSE, begin_day = NULL, end_day = NULL, n_cores = 1) {
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
  dir <- dir_create(path = paste0(dir_output, "/all_dataframe")) %>% fs_path()
  #### funciones necesarias: get_data_frame_init y get_data_frame_fin##########
  get_data_frame_init <- function(files, dates, dir) {
    stack_avg <- raster::stack(files, varname = var_name)
    df <- raster::calc(stack_avg, fun = function(x) do.call(stat_function, list(x, na.rm = TRUE))) %>%
      setNames(var_name) %>%
      raster::as.data.frame(xy = TRUE) %>%
      drop_na(all_of(var_name))
    df <- df %>% mutate(date = unique(dates))
    tmp_name <- paste0(unique(dates), "_", stat_function, "_tmp.parquet")
    out <- paste0(dir, "/", tmp_name)
    write_parquet(x = df, sink = out)
    rm(list = c("stack_avg", "df", "tmp_name", "out"))
  }
  get_data_frame_fin <- function(files) {
    df <- map(files, ~ read_parquet(.)) %>%
      bind_rows() %>%
      mutate(
        year = year(date),
        month = month(date),
        month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE))
      )
    tmp_name <- paste0(unique(df$year), "_", unique(df$month_name), ".parquet")
    out <- paste0(dir, "/", tmp_name)
    write_parquet(x = df, sink = out)
    rm(list = c("df", "tmp_name", "out"))
  }
  ##############################################################################
  ##### PRIMER PASO####
  list_files <- files %>%
    group_by(date) %>%
    group_split()
  files <- map(list_files, ~ pull(., "mapped_files"))
  dates <- map(list_files, ~ pull(., "date"))
  cat("Generando dataframes mensuales...\n\n")
  cat("Etapa 1: Generación dataframes diarios...\n\n")
  cl <- makeForkCluster(n_cores)
  plan(cluster, workers = cl)
  tic(msg = "Duración etapa 1")
  with_progress({
    p <- progressor(steps = length(files))
    future_walk2(files, dates, ~ {
      p()
      Sys.sleep(.2)
      get_data_frame_init(files = .x, dates = .y, dir = dir)
    }, .options = furrr_options(seed = TRUE))
  })
  toc()
  stopCluster(cl)
  rm(cl)
  cat("Etapa 2: Generando archivos mensuales y exportando...\n\n")
  #### Agrupando archivos por temporalidad seleccionada####
  files <- dir_ls(path = dir, regexp = "_tmp.parquet$", recurse = FALSE) %>%
    tibble(
      data_frame_files = .,
      date = as_date(path_file(data_frame_files), format = "%Y-%m-%d"),
      year = year(date),
      day = day(date),
      month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE)),
      week_name = paste0("w", sprintf("%02d", week(date)))
    )
  list_files <- files %>%
    group_by(year, month_name) %>%
    group_split() %>%
    map(., ~ pull(., "data_frame_files"))
  # cl <- makeForkCluster(n_cores)
  # plan(cluster, workers = cl)
  tic(msg = "Duración etapa 2")
  ## version multicore##
  # with_progress({
  #  p <- progressor(steps = length(list_files))
  #  future_walk(list_files, ~ {
  #    p()
  #    Sys.sleep(.2)
  #    get_data_frame_fin(files = .)
  #    },.options = furrr_options(seed = TRUE))
  #  })
  # stopCluster(cl)
  # rm(cl)
  ## version 1 core
  with_progress({
    p <- progressor(steps = length(list_files))
    walk(list_files, ~ {
      p()
      Sys.sleep(.2)
      get_data_frame_fin(files = .)
    })
  })
  walk(list_files, ~ file_delete(.))
  toc()
  cat("Fin... \n\n")
  setwd(current_wd)
  toc()
}
