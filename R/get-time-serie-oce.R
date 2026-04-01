#' Generar serie de tiempo a partir de datos satelitales
#'
#' Lee archivos parquet o CSV con datos georreferenciados, elimina los puntos
#' terrestres mediante una máscara costera y resume la var_name indicada por
#' fecha, calculando media (u otra stat_functionión), mínimo, máximo y desviación
#' estándar.
#'
#' @param files Character vector. Rutas a los archivos de entrada
#'   (`.parquet` o `.csv`).
#' @param stat_function Character. Nombre de la stat_functionión de agregación principal
#'   (e.g. `"mean"`, `"median"`).
#' @param file_type Character. Tipo de archivo: `"parquet"` o `"csv"`.
#' @param var_name Character. Nombre de la var_name objetivo presente en los
#'   archivos (e.g. `"sst"`, `"chlor_a"`).
#' @param shp_mask Objeto `sf` (opcional). Polígono(s) de tierra para filtrar
#'   puntos terrestres. Si es `NULL` (por defecto), se descarga la capa
#'   `ne_countries(scale = "large")` de \pkg{rnaturalearth}.
#' @param n_cores Integer. Número de núcleos para procesamiento paralelo.
#'   Si `n_cores = 1` (por defecto), se ejecuta en modo secuencial.
#' @param output_name Character. Ruta completa del archivo CSV de salida
#'   (e.g. `"output/serie_sst.csv"`). Se crean las carpetas intermedias si
#'   no existen.
#'
#' @return Invisible. Escribe un CSV en \code{output_name} con las columnas:
#'   \code{fecha}, \code{<var_name>_<stat_function>}, \code{<var_name>_min},
#'   \code{<var_name>_max}, \code{<var_name>_sd}.
#'
#' @details
#' La lectura de archivos utiliza \pkg{arrow} para ambos formatos (parquet y
#' CSV), aprovechando la evaluación lazy (pushdown de `select` y `filter`)
#' para minimizar el uso de memoria. Solo se cargan las columnas necesarias
#' (`lat`, `lon`, `date1` y la var_name objetivo) y se descartan filas con
#' `NA` en la var_name antes de traer los datos a R.
#'
#' @importFrom arrow read_parquet read_csv_arrow
#' @importFrom dplyr select filter collect mutate group_by summarise bind_cols
#'   rename bind_rows
#' @importFrom sf st_as_sf st_transform st_crs st_intersects st_coordinates
#'   st_drop_geometry st_make_valid st_union st_buffer
#' @importFrom rlang sym !! :=
#' @importFrom purrr map compact
#' @importFrom readr write_csv
#' @importFrom rnaturalearth ne_countries
#' @importFrom parallel makeForkCluster stopCluster
#' @importFrom future plan
#' @importFrom furrr future_map furrr_options
#' @importFrom cli cli_inform
#'
#' @examples
#' \dontrun{
#' files <- list.files("data/sst/", pattern = "\\.parquet$", full.names = TRUE)
#'
#' get_time_serie(
#' files = files_parquet,
#' stat_function = "median",
#'file_type = "parquet",
#' var_name = var_name,
#' shp_mask = shp_mask,
#' n_cores = 2,
#' output_name = glue::glue("sst_GA_{periodo_clim}_{sensor}_ts_clean.csv"))
#' )
#' }
#' @export get_time_serie
get_time_serie <- function(files, stat_function, file_type, var_name,
                           shp_mask = NULL, n_cores = 1, output_name) {
  sf::sf_use_s2(TRUE)
  # 1. Validar entradas -------------------------------------------------------

  stopifnot(
    is.character(files),
    length(files) > 0,
    file_type %in% c("parquet", "csv"),
    is.character(var_name),
    is.character(stat_function),
    is.numeric(n_cores) && n_cores >= 1
  )

  var_sym <- rlang::sym(var_name)

  # 2. Preparar máscara costera -----------------------------------------------

  if (is.null(shp_mask)) {
    cli::cli_inform("Descargando máscara de tierra (ne_countries, scale = 'large')...")
    shp_mask <- rnaturalearth::ne_countries(scale = "large", returnclass = "sf") %>%
      sf::st_make_valid() %>%
      sf::st_union()
  } else {
    shp_mask <- shp_mask %>%
      sf::st_make_valid() %>%
      sf::st_union()
  }

  # 3. Asegurar carpeta de salida ---------------------------------------------

  dir_out <- dirname(output_name)
  if (!dir.exists(dir_out)) dir.create(dir_out, recursive = TRUE)

  # 4. Definir stat_functionión interna de procesamiento -------------------------------

  fn_process <- function(file) {
    # 4a. Lectura lazy (Arrow pushdown) · · · · · · · · · · · · · · · · · · · ·

    tbl <- switch(file_type,
                  "parquet" = arrow::read_parquet(file, as_data_frame = FALSE),
                  "csv"     = arrow::read_csv_arrow(file, as_data_frame = FALSE)
    )

    df <- tbl %>%
      dplyr::select(lat, lon, date1, !!var_sym) %>%
      dplyr::filter(!is.na(!!var_sym)) %>%
      dplyr::collect() %>%
      dplyr::mutate(fecha = as.Date(date1))

    if (nrow(df) == 0L) {
      return(NULL)
    }

    # 4b. Filtrar puntos terrestres · · · · · · · · · · · · · · · · · · · · · ·

    df_sf <- sf::st_as_sf(df, coords = c("lon", "lat"), crs = 4326) %>%
      sf::st_transform(crs = sf::st_crs(shp_mask))

    en_tierra <- as.vector(sf::st_intersects(df_sf, shp_mask, sparse = FALSE))

    df_sf <- df_sf[!en_tierra, ]

    if (nrow(df_sf) == 0L) {
      return(NULL)
    }

    # 4c. Recuperar coordenadas y limpiar geometría · · · · · · · · · · · · · ·

    df_clean <- sf::st_transform(df_sf, crs = 4326) %>%
      {
        dplyr::bind_cols(
          sf::st_drop_geometry(.),
          as.data.frame(sf::st_coordinates(.))
        )
      } %>%
      dplyr::rename(lon = X, lat = Y)

    # 4d. Resumir por fecha · · · · · · · · · · · · · · · · · · · · · · · · · ·

    df_clean %>%
      dplyr::group_by(fecha) %>%
      dplyr::summarise(
        !!paste0(var_name, "_", stat_function) := match.fun(stat_function)(!!var_sym, na.rm = TRUE),
        !!paste0(var_name, "_min") := min(!!var_sym, na.rm = TRUE),
        !!paste0(var_name, "_max") := max(!!var_sym, na.rm = TRUE),
        !!paste0(var_name, "_sd") := sd(!!var_sym, na.rm = TRUE),
        .groups = "drop"
      )
  }

  # 5. Ejecución: paralela o secuencial ---------------------------------------

  cli::cli_inform("Procesando {length(files)} archivos con {n_cores} núcleo(s)...")

  if (n_cores > 1) {
    cl <- parallel::makeForkCluster(n_cores)
    on.exit(parallel::stopCluster(cl), add = TRUE)
    future::plan("cluster", workers = cl)

    df_list <- furrr::future_map(
      files, fn_process,
      .options = furrr::furrr_options(seed = TRUE)
    )
  } else {
    df_list <- purrr::map(files, fn_process)
  }

  # 6. Combinar resultados y exportar -----------------------------------------

  df_final <- df_list %>%
    purrr::compact() %>%
    dplyr::bind_rows()

  readr::write_csv(df_final, file = output_name)
  cli::cli_inform("Guardado en {output_name}")

  invisible(df_final)
}
