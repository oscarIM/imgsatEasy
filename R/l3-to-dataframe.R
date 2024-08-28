#' @title l3_to_dataframe
#' @description Función para obtener tablas de datos en parquet o csv a partir de imágenes L3 ya procesadas por l2_to_dataframe. Útil cuando se quiere obtener solo una parte (temporal y/o espacial) de los datos
#' @param dir_input directorio en donde se almacenan las imágenes L3
#' @param dir_output directorio en donde se almacenaran los archivos resultantes
#' @param format_output formato de los archivos resultantes. Toma valores "parquet" o "csv. Por defecto parquet.
#' @param var_name nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh", etc)
#' @param season temporalidad para la generación de las tablas de datos ("day",  "month", "year" por ahora). Depende la la temporalidad de las imágenes L3
#' @param stat_function string con el nombre de la función para calcular el valor de una variables a través de la temporalidad (season).
#' @param north latitud norte. Si null, se toma la latitud norte de los datos de entrada
#' @param south latitud sur. Si null, se toma la latitud sur de los datos de entrada
#' @param west latitud oeste. Si null, se toma la longitud oeste de los datos de entrada
#' @param east latitud este. Si null, se toma la longitud este de los datos de entrada
#' @param n_cores número de núcleos a usar. Por defecto, n_cores = 1 (corrida secuencial). La parelelización es respecto de la cantidad de dataframe procesados simultáneamente
#' @return tabla de datos en csv o parquet
#' @importFrom arrow write_parquet
#' @importFrom dplyr across all_of between filter group_by group_split mutate tibble
#' @importFrom lubridate year month ymd
#' @importFrom ncdf4 nc_open ncatt_get ncvar_get nc_close
#' @importFrom furrr future_walk furrr_options
#' @importFrom purrr map map_dfr walk
#' @importFrom readr write_csv
#' @importFrom rlang !! sym :=
#' @importFrom magrittr %>%
#' @importFrom parallel makeForkCluster stopCluster
#' @importFrom terra app rast as.data.frame
#' @importFrom future plan
#' @importFrom progressr progressor with_progress
#' @export l3_to_dataframe
#' @examples
#' \dontrun{
#' dir_input <- "vars_oce/sst/L3"
#' dir_output <- getwd()
#' season <- "month"
#' start_date <- NULL
#' end_date <- NULL
#' var_name <- "sst"
#' north <- -36
#' south <- -38
#' west <- -73
#' east <- -71
#' stat_function <- "median"
#' format_output <- "parquet"
#' l3_to_dataframe(dir_input = dir_input, dir_output = dir_output, season = season, format_ouput = format_ouput, var_name = var_name,  north = north, south = south, west = west, east = east, stat_function = stat_function)
#' }
l3_to_dataframe <- function(dir_input, dir_output, season, var_name, star_date = NULL, end_date = NULL, north = NULL, south = NULL, west = NULL, east = NULL, stat_function = "mean", format_output, n_cores = 1) {
  tic()
  all_files <- list.files(path = dir_input, pattern = "_L3mapped.nc$", full.names = TRUE)
  #aux fn
  get_nc_dates <- function(nc_file) {
    nc <- ncdf4::nc_open(nc_file)
    on.exit(ncdf4::nc_close(nc))
    times <- c(ncdf4::ncatt_get(nc, 0, "time_coverage_start")$value,
               ncdf4::ncatt_get(nc, 0, "time_coverage_end")$value)
  }
  nc_to_df <- function(df_input) {
    stack <- terra::rast(df_input$file, subds = var_name)
    raster <- terra::app(stack, fun = match.fun(stat_function), na.rm = TRUE)
    dates <- purrr::map(df_input$file, ~get_nc_dates(nc_file = .))
    df_fechas <- purrr::map_dfr(dates, ~ dplyr::tibble(fecha_inicio = .x[1],
                                                       fecha_termino = .x[2]))
    date1 <- min(df_fechas$fecha_inicio)
    date2 <- max(df_fechas$fecha_termino)
    df <-  terra::as.data.frame(raster, xy = TRUE) %>%
      dplyr::rename(!!sym(var_name) := 3,
                    lon = x,
                    lat = y) %>%
      dplyr::mutate(date1 = date1, date2 = date2)
    coords <- c(north, south, west, east)
    if (any(is.null(coords))) {
      df <- df
    } else {
      ylim <- sort(c(coords[1], coords[2]))
      xlim <- sort(c(coords[3], coords[4]))
      df <- df %>%
        dplyr::filter(dplyr::between(round(lat, 2), ylim[1], ylim[2])) %>%
        dplyr::filter(dplyr::between(round(lon, 2), xlim[1], xlim[2]))
    }
    base_name <- paste0(lubridate::ymd(as.Date(date1)), "_", lubridate::ymd(as.Date(date2)), "_", season)
    if (format_output  == "parquet") {
      file_name <- paste0(dir_output,"/", base_name, ".parquet")
      arrow::write_parquet(x = df, sink = file_name)
    } else if (format_output == "csv") {
      file_name <- paste0(dir_output,"/", base_name, ".csv")
      write_csv(x = df, file = file_name, progress = FALSE)
    } else {
      stop("Formato no soportado: ", format_output)
    }
  }
  all_dates_df <- purrr::map(all_files, ~get_nc_dates(nc_file = .)) %>%
    purrr::map_df(~dplyr::tibble(fecha_inicio = .x[1], fecha_termino = .x[2])) %>%
    dplyr::mutate(file = all_files,
                  year = lubridate::year(fecha_inicio),
                  month = lubridate::month(fecha_inicio,abbr = FALSE, label = TRUE))
  dates_filter <- c(start_date, end_date)
  if (any(is.null(dates_filter))) {
    dates_filter <- c(as.Date(start_date), as.Date(end_date))
    all_dates_df <- all_dates_df
  } else {
    tmp <- all_dates_df %>%
      dplyr::filter(as.Date(fecha_inicio) >= start_date,
                    as.Date(fecha_termino) <= end_date)
  }
  group_vars <- switch(season,
                       "year"  = "year",
                       "month" = "month",
                       "day"   = "fecha_inicio")
  files_df_list <- all_dates_df %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(group_vars))) %>%
    dplyr::group_split() %>%
    setNames(purrr::map(., ~ {
      if (season == "year") {
        unique(.[["year"]])
      } else if (season == "month") {
        unique(as.character(.[["month"]]))
      } else if (season == "day") {
        unique(.[["fecha_inicio"]])
      }
    }))

  if (n_cores <= 1) {
    progressr::with_progress({
      p <- progressr::progressor(steps = length(files_df_list))
      purrr::walk(files_df_list, function(df_input) {
        Sys.sleep(.2)
        nc_to_df(df_input)
        p()
      })
    }) } else {
      cl <- parallel::makeForkCluster(n_cores)
      on.exit(parallel::stopCluster(cl))
      future::plan("cluster", workers = cl)
      progressr::with_progress({
        p <- progressr::progressor(steps = length(files_df_list))
        furrr::future_walk(files_df_list, function(df_input) {
          Sys.sleep(.2)
          nc_to_df(df_input)
          p()
        }, .options = furrr::furrr_options(seed = TRUE))
      })
    }
  toc()
  cat("Fin \n\n")
}
