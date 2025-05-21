#' Descarga y procesamiento de datos de viento desde ERA5
#'
#' Esta función descarga datos diarios de viento del reanálisis ERA5 (Copernicus) para un área y período definidos. Puede trabajar con datos de niveles de presión o de superficie. Los archivos se descargan en formato NetCDF y se procesan para obtener valores diarios promedio de velocidad y dirección del viento.
#'
#' @param long_min Longitud mínima del área de interés (decimal).
#' @param long_max Longitud máxima del área de interés (decimal).
#' @param lat_min Latitud mínima del área de interés (decimal).
#' @param lat_max Latitud máxima del área de interés (decimal).
#' @param start_time Fecha de inicio en formato `"YYYY-MM-DD"`.
#' @param end_time Fecha de término en formato `"YYYY-MM-DD"`.
#' @param dir_output Ruta del directorio donde se guardarán los archivos descargados y procesados.
#' @param prefix_outfile Prefijo que se usará para nombrar los archivos de salida (`.nc` y `.csv`).
#' @param product_name Nombre del producto a solicitar: `"pressure_levels"` para viento a 1000 hPa o `"single-levels"` para viento a 10 metros de altura.
#'
#' @return Exporta un archivo CSV con los datos diarios promedio de viento (velocidad y dirección), y guarda el NetCDF descargado. No retorna ningún objeto en R.
#' @details Esta función requiere que el usuario tenga configurada una cuenta en el CDS (Climate Data Store) con su clave de API, la cual puede configurarse mediante `ecmwfr::wf_set_key()`.
#'
#' @importFrom glue glue
#' @importFrom purrr walk
#' @importFrom rlang sym is_missing
#' @importFrom lubridate ymd day year month as_datetime as_date
#' @importFrom dplyr mutate summarise group_by bind_cols rename select
#' @importFrom tidyr pivot_longer pivot_wider separate
#' @importFrom terra rast
#' @importFrom readr write_csv
#' @importFrom rWind uv2ds
#' @importFrom ecmwfr wf_request
#' 
#' @examples
#' \dontrun{
#' get_wind_data(
#'   long_min = -75, long_max = -70,
#'   lat_min = -40, lat_max = -35,
#'   start_time = "2023-01-01",
#'   end_time = "2023-01-10",
#'   dir_output = tempdir(),
#'   prefix_outfile = "wind_data",
#'   product_name = "single-levels"
#' )
#' }
#'
#' @export
get_wind_data <- function(long_min, long_max, lat_min, lat_max,
                          start_time, end_time,
                          dir_output, prefix_outfile, product_name) {
  
  if (!dir.exists(dir_output)) dir.create(dir_output, recursive = TRUE)
  ### Funciones auxiliares internas ###
  circ_mean <- function(deg) {
    rad_m <- (deg * pi) / 180
    mean_cos <- mean(cos(rad_m))
    mean_sin <- mean(sin(rad_m))
    theta <- rad2deg(atan(mean_sin / mean_cos))
    if (mean_cos < 0) theta <- theta + 180
    if ((mean_sin < 0) & (mean_cos > 0)) theta <- theta + 360
    theta
  }
  
  rad2deg <- function(rad) {
    (rad * 180) / pi
  }
  
  deg2rad <- function(deg) {
    (deg * pi) / 180
  }
  
  cat("Generando la solicitud de datos para el área y tiempo definidos...\n")
  
  ### Validación de argumentos ###
  purrr::walk(
    c("long_min", "long_max", "lat_min", "lat_max", "start_time", "end_time"),
    ~ if (rlang::is_missing(eval(rlang::sym(.x))))
      stop(glue::glue("Se debe especificar `{.x}`"))
  )
  
  ### Formateo del área y fechas ###
  lat_max <- ceiling(lat_max)
  long_max <- ceiling(long_max)
  lat_min <- floor(lat_min)
  long_min <- floor(long_min)
  area <- paste0(lat_max, "/", long_min, "/", lat_min, "/", long_max)
  dates <- seq.Date(from = lubridate::ymd(start_time),
                    to = lubridate::ymd(end_time), by = "day")
  days <- as.character(lubridate::day(dates))
  target <- glue::glue("{prefix_outfile}.nc")
  
  ### Construcción de la solicitud ###
  if (product_name == "pressure_levels") {
    request <- list(
      dataset_short_name = "reanalysis-era5-pressure-levels",
      pressure_level     = "1000",
      product_type       = "reanalysis",
      variable           = c("u_component_of_wind", "v_component_of_wind"),
      year               = as.character(lubridate::year(start_time)),
      month              = sprintf("%02d", lubridate::month(start_time)),
      day                = days,
      time               = sprintf("%02d:00", 0:23),
      data_format        = "netcdf",
      download_format    = "unarchived",
      target             = target
    )
  }
  
  if (product_name == "single-levels") {
    request <- list(
      dataset_short_name = "reanalysis-era5-single-levels",
      product_type       = "reanalysis",
      variable           = c("10m_u_component_of_wind", "10m_v_component_of_wind", "mean_sea_level_pressure"),
      year               = as.character(lubridate::year(start_time)),
      month              = as.character(unique(lubridate::month(dates))),
      day                = days,
      time               = sprintf("%02d:00", 0:23),
      area               = area,
      data_format        = "netcdf",
      download_format    = "unarchived",
      target             = target
    )
  }
  
  cat("Descargando los datos...\n")
  ecmwfr::wf_request(request = request, path = dir_output)
  cat("Descarga finalizada.\n")
  
  cat("Procesando datos...\n")
  file_ncdf <- glue::glue("{dir_output}/{target}")
  
  df <- terra::rast(file_ncdf) %>%
    as.data.frame(xy = TRUE) %>%
    tidyr::pivot_longer(cols = 3:last_col(), names_to = "variables_tmp", values_to = "values_tmp") %>%
    tidyr::separate(col = variables_tmp, into = c("full_var", "timestamp"), sep = "=", extra = "drop") %>%
    tidyr::separate(full_var, into = c("var_name"), sep = "_", extra = "drop") %>%
    tidyr::pivot_wider(names_from = var_name, values_from = values_tmp) %>%
    dplyr::mutate(time = lubridate::as_datetime(as.integer(timestamp), tz = "UTC")) %>%
    dplyr::bind_cols(rWind::uv2ds(.$u10, .$v10))
  
  df_final <- df %>%
    dplyr::mutate(date = lubridate::as_date(time)) %>%
    dplyr::group_by(x, y, date) %>%
    dplyr::summarise(
      speed_mean = mean(speed, na.rm = TRUE),
      dir        = circ_mean(dir),
      u          = mean(u10, na.rm = TRUE),
      v          = mean(v10, na.rm = TRUE),
      .groups    = "drop"
    ) %>%
    dplyr::rename(lon = x, lat = y)
  
  ### Exportar archivo CSV final ###
  file <- glue::glue("{dir_output}/{prefix_outfile}.csv")
  readr::write_csv(df_final, file = file)
}