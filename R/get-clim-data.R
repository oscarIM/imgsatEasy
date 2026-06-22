#' @title get_clim_data
#' @description Procesa archivos satelitales (parquet o csv) y genera un
#'   data frame con la estadística resumen por temporada (mes, semana o año)
#'   para una variable oceanográfica dada. El resultado puede guardarse como
#'   CSV y usarse directamente en \code{\link{plot_clim}}.
#'
#' @param dir_input Ruta al directorio que contiene los archivos de entrada
#'   (.parquet o .csv). Los nombres de archivo deben comenzar con la fecha
#'   en formato \code{YYYY-MM-DD} o \code{YYYY-MM}.
#' @param season Temporalidad de agregación. Opciones: \code{"month"},
#'   \code{"week"} o \code{"year"}.
#' @param stat_function Nombre de la función estadística a aplicar como
#'   string. Ejemplos: \code{"median"}, \code{"mean"}.
#' @param var_name Nombre de la variable a procesar tal como aparece en los
#'   archivos de entrada. Ejemplos: \code{"chlor_a"}, \code{"sst"},
#'   \code{"Rrs_645"}, \code{"Kd490"}.
#' @param start_date Fecha de inicio del periodo a procesar en formato
#'   \code{"YYYY-MM-DD"}. Si es \code{NULL} se usan todos los archivos
#'   disponibles.
#' @param end_date Fecha de término del periodo a procesar en formato
#'   \code{"YYYY-MM-DD"}. Si es \code{NULL} se usan todos los archivos
#'   disponibles.
#' @param xlim Vector numérico de longitud 2 con los límites de longitud
#'   \code{c(lon_min, lon_max)}.
#' @param ylim Vector numérico de longitud 2 con los límites de latitud
#'   \code{c(lat_min, lat_max)}.
#' @param n_cores Número de núcleos a utilizar para el procesamiento
#'   paralelo. Por defecto \code{1} (procesamiento secuencial). El
#'   paralelismo usa \code{parallel::makeForkCluster} y solo está
#'   disponible en sistemas Unix/macOS.
#' @param name_output Ruta y nombre del archivo CSV de salida. Si es
#'   \code{NULL} (por defecto) no se guarda ningún archivo y la función
#'   retorna el data frame de forma invisible.
#'
#' @return Data frame con las columnas \code{lat}, \code{lon}, \code{fill}
#'   (valor de la estadística calculada) y \code{season} (factor ordenado
#'   con el nombre del periodo: nombre del mes en español, \emph{Semana N}
#'   o el año). Si \code{name_output} no es \code{NULL}, el data frame se
#'   guarda en disco como CSV y se retorna de forma invisible.
#'
#' @importFrom arrow read_parquet
#' @importFrom dplyr between bind_rows collect filter group_by group_split if_else mutate pull select summarise
#' @importFrom furrr future_map furrr_options
#' @importFrom future plan
#' @importFrom glue glue
#' @importFrom lubridate isoweek month year
#' @importFrom parallel makeForkCluster stopCluster
#' @importFrom progressr progressor with_progress
#' @importFrom purrr imap map
#' @importFrom readr read_csv write_csv
#' @importFrom rlang sym
#' @importFrom stringr str_detect str_extract
#' @importFrom tibble tibble
#'
#' @seealso \code{\link{plot_clim}} para graficar el resultado de esta función.
#'
#' @export get_clim_data
#'
#' @examples
#' \dontrun{
#' get_clim_data(
#'   dir_input     = "/datos/satelital/sst/parquet",
#'   season        = "month",
#'   stat_function = "median",
#'   var_name      = "sst",
#'   start_date    = "2020-01-01",
#'   end_date      = "2023-12-31",
#'   xlim          = c(-76, -70),
#'   ylim          = c(-40, -35),
#'   n_cores       = 8,
#'   name_output   = "sst_mensual.csv"
#' )
#' }
get_clim_data <- function(
  dir_input,
  season,
  stat_function,
  var_name,
  start_date = NULL,
  end_date = NULL,
  xlim,
  ylim,
  n_cores = 1,
  name_output = NULL
) {
  # 1. Configuración inicial -----------------------------------------------

  meses_es <- c(
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
  )

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  func <- match.fun(stat_function)
  season_param <- season

  lon_min <- min(xlim)
  lon_max <- max(xlim)
  lat_min <- min(ylim)
  lat_max <- max(ylim)


  # 2. Inventario y filtrado de archivos -----------------------------------

  files_ext_pattern <- paste(c(".parquet$", ".csv$", ".tif$"), collapse = "|")

  all_files_tmp <- list.files(
    path       = dir_input,
    full.names = TRUE,
    pattern    = files_ext_pattern
  ) %>%
    tibble::tibble(file = .) %>%
    dplyr::mutate(
      filename = basename(file),
      date_str = stringr::str_extract(filename, "^(\\d{4}-\\d{2}-\\d{1,2}|\\d{4}-\\d{2})"),
      has_day = stringr::str_detect(date_str, "^\\d{4}-\\d{2}-\\d{1,2}"),
      date_str = as.character(date_str),
      date = as.Date(ifelse(has_day, date_str, paste0(date_str, "-01"))),
      month = lubridate::month(date),
      year = lubridate::year(date),
      week = lubridate::isoweek(date),
      season_val = if (season_param == "week") {
        week
      } else if (season_param == "month") {
        month
      } else {
        year
      }
    ) %>%
    dplyr::filter(
      is.null(start_date) | is.null(end_date) |
        dplyr::between(date, start_date, end_date)
    )

  ext_file <- unique(stringr::str_extract(all_files_tmp$file, files_ext_pattern))

  path_list <- all_files_tmp %>%
    dplyr::group_by(season_val) %>%
    dplyr::group_split() %>%
    purrr::map(~ dplyr::pull(.x, "file"))


  # 3. Lectura y filtrado espacial de cada archivo -------------------------

  process_tables <- function(file) {
    switch(ext_file,
      ".parquet" = arrow::read_parquet(file, as_data_frame = FALSE),
      ".csv"     = readr::read_csv(file, show_col_types = FALSE, progress = FALSE)
    ) %>%
      dplyr::filter(!is.na(!!rlang::sym(var_name))) %>%
      dplyr::filter(
        lon >= lon_min, lon <= lon_max,
        lat >= lat_min, lat <= lat_max
      ) %>%
      dplyr::select(lat, lon, date1, !!rlang::sym(var_name)) %>%
      dplyr::collect()
  }


  # 4. Despacho secuencial o paralelo --------------------------------------

  process_sublist <- function(entry_list) {
    if (n_cores <= 1) {
      progressr::with_progress({
        p <- progressr::progressor(steps = length(entry_list))
        purrr::map(entry_list, ~ {
          p()
          process_tables(.x)
        })
      })
    } else {
      cl <- parallel::makeForkCluster(n_cores)
      on.exit(parallel::stopCluster(cl))
      future::plan("cluster", workers = cl)
      progressr::with_progress({
        p <- progressr::progressor(steps = length(entry_list))
        furrr::future_map(
          entry_list,
          ~ {
            p()
            Sys.sleep(0.1)
            process_tables(.x)
          },
          .options = furrr::furrr_options(
            seed     = TRUE,
            packages = c("dplyr", "readr", "arrow")
          )
        )
      })
    }
  }


  # 5. Agregación estadística por temporada --------------------------------

  data_plot <- purrr::imap(path_list, ~ {
    message(switch(season_param,
      "month" = glue::glue("Procesando mes: {meses_es[as.integer(.y)]}"),
      "week"  = glue::glue("Procesando semana: {.y}"),
      "year"  = glue::glue("Procesando año: {.y}"),
      glue::glue("Procesando: {.y}")
    ))

    dplyr::bind_rows(process_sublist(.x)) %>%
      dplyr::mutate(
        year  = lubridate::year(date1),
        month = lubridate::month(date1),
        week  = lubridate::isoweek(date1)
      ) %>%
      dplyr::group_by(
        lat, lon,
        season_num = if (season_param == "week") {
          week
        } else if (season_param == "month") {
          month
        } else {
          year
        }
      ) %>%
      dplyr::summarise(
        fill = func(!!rlang::sym(var_name), na.rm = TRUE),
        .groups = "drop"
      )
  }) %>%
    dplyr::bind_rows()


  # 6. Etiquetado y ordenamiento de la columna season ----------------------

  data_plot <- data_plot %>%
    dplyr::mutate(
      season = if (season_param == "month") {
        factor(meses_es[season_num], levels = meses_es)
      } else if (season_param == "week") {
        factor(
          paste0("Semana ", season_num),
          levels = paste0("Semana ", sort(unique(season_num)))
        )
      } else {
        factor(
          as.character(season_num),
          levels = sort(unique(as.character(season_num)))
        )
      }
    ) %>%
    dplyr::select(-season_num)


  # 7. Corrección específica para Rrs_645 ----------------------------------

  if (var_name == "Rrs_645") {
    data_plot <- data_plot %>%
      dplyr::mutate(
        fill = fill * 158.9418,
        fill = dplyr::if_else(fill < 0, 0, fill)
      )
  }


  # 8. Exportación opcional ------------------------------------------------

  if (!is.null(name_output)) {
    readr::write_csv(data_plot, name_output)
    message(glue::glue("Datos procesados guardados en: {name_output}"))
  }

  invisible(data_plot)
}
