#' @title get_wind_data
#' @description Función para descarcar datos de viento desde eras5 (u y v) para 1000 hpa (formato .grib), por medio del paquete ecmwfr y transformarlos a un dataframe (csv) diario. Es necesario tener una cuenta del CDS.
#' @param lat_max latitud máxima para la zona de descarga
#' @param lat_min latitud mínima para la zona de descarga
#' @param long_max longitud máxima para la zona de descarga
#' @param long_min latitud mínima para la zona de descarga
#' @param start_time tiempo inicial para la descarga
#' @param end_time tiempo dinal para la descarga
#' @param product_time tipo de producto. Por ahora 2: "pressure-levels" o "single-levels"
#' @param dir_output directorio de descarga de los datos. Por defecto(getwd())
#' @param prefix_outfile archivo a descargar
#' @return dataframe en .csv
#' @importFrom lubridate year month day ymd as_date ymd_hms
#' @importFrom purrr map map_depth
#' @importFrom ncdf4 nc_open ncatt_get ncvar_get
#' @importFrom tictoc tic toc
#' @importFrom fs dir_create fs_path file_delete
#' @importFrom ecmwfr wf_request
#' @importFrom tidyr drop_na as_tibble
#' @importFrom readr write_csv
#' @importFrom dplyr mutate between filter group_by summarise bind_cols

#' @export get_wind_data
#' @examples
#' \dontrun{
#' long_max <- -60
#' long_min <- -80
#' lat_max <- -18
#' lat_min <- -56
#' start_time <- "2023-02-20"
#' end_time <- "2023-02-24"
#' dir_output <- dir_output
#' prefix_outfile <- prefix_outfile
#' uid <- "XXXXX"
#' cds_api_key <- "XXXXXX"
#' ecmwfr::wf_set_key(
#'   user = uid,
#'   key = cds_api_key,
#'   service = "cds"
#' )
#' get_wind_data(
#'   lat_min = lat_min, lat_max = lat_max, long_min = long_max, star_time = star_time,
#'   end_time = end_time, dir_output = dir_output, prefix_outfile = prefix_outfile
#' )
#' }
get_wind_data <- function(lat_min, lat_max, long_min, long_max, start_time,
                          end_time, dir_output = getwd(), product_name, prefix_outfile) {
  tictoc::tic("Tiempo total descarga y transformación de datos")
  #### input checks ####
  if (missing(long_min)) {
    stop("se debe especificar long_min")
  }
  if (missing(long_max)) {
    stop("se debe especificar long_max")
  }
  if (missing(lat_min)) {
    stop("se debe especificar lat_min ")
  }
  if (missing(lat_max)) {
    stop("se debe especificar lat_max ")
  }
  if (missing(start_time)) {
    stop("se debe especificar start_time ")
  }
  if (missing(end_time)) {
    stop("se debe especificar end_time")
  }
  #### setting inputs for request ####
  lat_max <- ceiling(lat_max)
  long_max <- ceiling(long_max)
  lat_min <- floor(lat_min)
  long_min <- floor(long_min)
  area <- paste0(lat_max, "/", long_min, "/", lat_min, "/", long_max)
  dates <- seq.Date(from = lubridate::ymd(start_time), to = lubridate::ymd(end_time), by = "day")
  days <- as.character(lubridate::day(dates))
  ####
  #### request ####
  cat("Generando la socilitud de datos para el area y tiempo definidos... \n")
  if(product_name == "pressure_levels") {
    request <- list(
      "dataset_short_name" = "reanalysis-era5-pressure-levels",
      "pressure_level" = "1000",
      "product_type" = "reanalysis",
      "variable" = c("u_component_of_wind", "v_component_of_wind"),
      "year" = as.character(lubridate::year(start_time)),
      "month" = paste0(sprintf("%02d", lubridate::month(start_time))),
      "day" = days,
      "time" = c(
        "00:00", "01:00", "02:00", "03:00", "04:00",
        "05:00", "06:00", "07:00", "08:00", "09:00",
        "10:00", "11:00", "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00", "18:00", "19:00",
        "20:00", "21:00", "22:00", "23:00"
      ),
      "area" = area,
      "format" = "grib",
      "target" = "tmp.grib"
    )
}
  if (product_name == "single-levels") {
    request <- list(
      "dataset_short_name" = "reanalysis-era5-single-levels",
      #"pressure_level" = "1000",
      "product_type" = "reanalysis",
      "variable" = c("10m_u_component_of_wind", "10m_v_component_of_wind", "mean_sea_level_pressure"),
      "year" = as.character(lubridate::year(start_time)),
      "month" = paste0(sprintf("%02d", lubridate::month(start_time))),
      "day" = days,
      "time" = c(
        "00:00", "01:00", "02:00", "03:00", "04:00",
        "05:00", "06:00", "07:00", "08:00", "09:00",
        "10:00", "11:00", "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00", "18:00", "19:00",
        "20:00", "21:00", "22:00", "23:00"
      ),
      "area" = area,
      "format" = "grib",
      "target" = "tmp.grib"
    )
  }
  #### download####
  dir_output <- fs::dir_create(path = dir_output) %>%
    fs::fs_path()
  cat("Iniciando la descarga de datos...\n")
  ecmwfr::wf_request(
    request = request,
    user = uid,
    transfer = TRUE,
    path = dir_output,
    verbose = TRUE,
    time_out = 18000
  )
  cat("Descarga de datos finalizada...\n")
  #### transformación a nc#####
  cat("Transformación de grib a nc...\n")
  file_grb <- paste0(dir_output, "/tmp.grib")
  file_ncdf <- paste0(dir_output, "/", prefix_outfile, "_tmp.nc")
  system(paste("cdo -f nc copy ", file_grb, file_ncdf, sep = (" ")))
  fs::file_delete(path = paste0(dir_output, "/tmp.grib"))
  #### transformación a csv#####
  cat("Transformación de nc a csv...\n")
  nc_data <- ncdf4::nc_open(file_ncdf)
  # get lon lat, time
  dims <- c("lon", "lat", "time")
  dims_list <- purrr::map(dims, ~ ncdf4::ncvar_get(nc = nc_data, varid = .))
  names(dims_list) <- dims
  tunits <- ncdf4::ncatt_get(nc_data, "time", "units")
  time_ini <- tunits$value
  time_ini <- lubridate::ymd_hms(time_ini)
  time_step <- paste0(1, " ", "hours")
  time_range <- seq(
    from = time_ini,
    length.out = length(dims_list$time),
    by = time_step
  )
  dims_list$time_range <- time_range
  fields <- c("u", "v")
  arrays_list <- purrr::map(1:length(fields), ~ ncdf4::ncvar_get(nc = nc_data, varid = nc_data$var[[.]]$name))
  names(arrays_list) <- fields
  fillvalues_list <- purrr::map(1:length(fields), ~ ncdf4::ncatt_get(nc = nc_data, varid = nc_data$var[[.]]$name, attname = "_FillValue"))
  arrays_list <- purrr::map_depth(arrays_list, 1, ~ ifelse(. == fillvalues_list[[1]]$value, NA, .x))
  tmp <- expand.grid(dims_list$lon, dims_list$lat, dims_list$time_range) %>%
    tidyr::as_tibble()
  colnames(tmp) <- c("lon", "lat", "date")
  tmp <- tmp %>%
    dplyr::mutate(
      lon = as.vector(lon),
      lat = as.vector(lat),
      u = as.vector(arrays_list[[1]]),
      v = as.vector(arrays_list[[2]]),
      date = as_date(date)
    ) %>%
    tidyr::drop_na() %>%
    dplyr::filter(dplyr::between(date, lubridate::as_date(start_time), lubridate::as_date(end_time))) %>%
    tidyr::as_tibble()
  # remove cosas
  rm(list = c(
    "time_range", "arrays_list", "fillvalues_list", "dims_list",
    "nc_data", "tunits"
  ))
  speed_dir <- tibble::as_tibble(rWind::uv2ds(tmp$u, tmp$v))
  df <- dplyr::bind_cols(tmp, speed_dir) %>%
    dplyr::mutate(date = lubridate::as_date(date))
  rm(list = c("tmp", "speed_dir"))
  # We compute the arithmetic mean for the wind speed. The direction as the circular mean
  # fn to calculate circular mean for direcction
  ####### iniicio funciones trigonometricas tomadas de Rwind#####
  circ_mean <- function(deg) {
    rad_m <- (deg * pi) / (180)
    mean_cos <- mean(cos(rad_m))
    mean_sin <- mean(sin(rad_m))
    theta <- rad2deg(atan(mean_sin / mean_cos))
    if (mean_cos < 0) theta <- theta + 180
    if ((mean_sin < 0) & (mean_cos > 0)) theta <- theta + 360
    theta
  }
  rad2deg <- function(rad) {
    (rad * 180) / (pi)
  }
  deg2rad <- function(deg) {
    (deg * pi) / (180)
  }
  #### dataframe by season ####
  df_final <- df %>%
    dplyr::group_by(lon, lat, date) %>%
    dplyr::summarise(
      speed_mean = mean(speed, na.rm = TRUE),
      dir = circ_mean(dir),
      u = mean(u, na.rm = TRUE),
      v = mean(v, na.rm = TRUE)
    )
  #### export final df ####
  file <- paste0(dir_output, "/", prefix_outfile, ".csv")
  readr::write_csv(x = df_final, file = file)
  fs::file_delete(path = file_ncdf)
  tictoc::toc()
}
