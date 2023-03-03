#' @title get_wind_data
#' @description Función para descarcar datos de viento desde eras5 (u y v) para 1000 hpa (formato .grib), por medio del paquete ecmwfr. Es necesario tener una cuenta del CDS.
#' @param lat_max latitud máxima para la zona de descarga
#' @param lat_min latitud mínima para la zona de descarga
#' @param long_max longitud máxima para la zona de descarga
#' @param long_min latitud mínima para la zona de descarga
#' @param start_time tiempo inicial para la descarga
#' @param end_time tiempo dinal para la descarga
#' @param dir_output directorio de descarga de los datos. Por defecto(getwd())
#' @param name_outfile nombre archivo a descargar
#' @return archivos .grib
#' @importFrom lubridate day ymd
#' @importFrom tictoc tic toc
#' @export get_wind_data
#' @examples
#' \dontrun{
#' long_max <- -60
#' long_min <- -80
#' lat_max <- -18
#' lat_min <- -56
#' start_time <- "2023-02-20"
#' end_time <- "2023-02-24"
#' out_path <- getwd()
#' name_outfile <- "data_feb_2023_w15c_w16p.grib"
#' uid <- "XXXXX"
#' cds_api_key <- "XXXXXX"
#' ecmwfr::wf_set_key(
#'   user = uid,
#'   key = cds_api_key,
#'   service = "cds"
#' )
#' get_wind_data(
#'   lat_min = lat_min, lat_max = lat_max, long_min = long_max, star_time = star_time,
#'   end_time = end_time, dir_output = dir_output, name_outfile = name_outfile
#' )
#' }
get_wind_data <- function(lat_min, lat_max, long_min, long_max, start_time,
                          end_time, dir_output, name_outfile) {
  setwd(dir_output)
  tic("Tiempo total descarga y transformación de datos")
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
  request <- list(
    "dataset_short_name" = "reanalysis-era5-pressure-levels",
    "pressure_level" = "1000",
    "product_type" = "reanalysis",
    "variable" = c("u_component_of_wind", "v_component_of_wind"),
    "year" = as.character(year(start_time)),
    "month" = paste0(sprintf("%02d", month(start_time))),
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
  ####
  #### download####
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
  ####
  #### transformación a nc
  cat("Transformación de grib a nc...\n")
  file_grb = "tmp.grib"
  file_ncdf = name_outfile
  system(paste("cdo -f nc copy ", file_grb, file_ncdf,sep=(" ")))
  fs::file_delete(path = paste0(dir_output,"/tmp.grib"))
  toc()
}
