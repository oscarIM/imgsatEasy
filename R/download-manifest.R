#' @title download_manifest

#' @description función para descargar archivos a partir de un manifiesto o varios manifiestos. Esta función descarga archivos desde enlaces proporcionados en archivos .txt. Los enlaces deben estar en formato HTTP(s) y la función generará nombres de archivo únicos basados en la ruta de los enlaces para evitar conflictos. se necesitan credecianles almacenadas en .urs_cookies
#' @param folder_txts Carpeta que contiene los archivos .txt con los enlaces de descarga.
#' @param output_file Nombre del archivo de salida donde se guardarán todos los enlaces.
#' @param download_dir Directorio donde se descargarán los archivos.
#' @return archivos tar
#' @importFrom purrr map_dfr pwalk
#' @importFrom readr read_lines write_lines
#' @importFrom stringr str_subset str_match str_extract
#' @importFrom glue glue
#' @export download_manifest
#'
#' @examples
#' \dontrun{
#' download_manifest("/ruta/a/tus/archivos/txt")
#' }

download_manifest <- function(folder_txts, output_file = "todos_los_links.txt", download_dir = "descargas") {

  # Crear carpeta de descarga si no existe
  if (!dir.exists(download_dir)) {
    dir.create(download_dir)
  }

  # Listar archivos .txt en la carpeta
  files <- list.files(folder_txts, pattern = "\\.txt$", full.names = TRUE)

  # Extraer links y construir nombres únicos más limpios
  links_con_nombre <- files %>%
    purrr::map_dfr(~ {
      links <- readr::read_lines(.x) %>%
        stringr::str_subset("^https?://")
      if (length(links) == 0) return(NULL)

      rutas_unicas <- stringr::str_match(links, "p=(/data\\d+/[\\w]+)")[, 2]

      tibble::tibble(link = links, ruta = rutas_unicas) %>%
        dplyr::mutate(
          id = stringr::str_extract(ruta, "([\\w]+)$"),
          sufijo = stringr::str_extract(link, "_files_\\d+"),
          nombre_destino = glue::glue("request_files_{id}{sufijo}.tar")
        ) %>%
        dplyr::select(link, nombre_destino)
    })




  # Guardar todos los links en un archivo
  readr::write_lines(links_con_nombre$link, output_file)

  # Descargar con wget cada archivo con nombre único
  purrr::pwalk(links_con_nombre, function(link, nombre_destino) {
    destino_final <- file.path(download_dir, nombre_destino)
    cmd <- glue::glue(
      'wget --load-cookies ~/.urs_cookies ',
      '--save-cookies ~/.urs_cookies ',
      '--auth-no-challenge=on ',
      '--no-check-certificate ',
      '--content-disposition ',
      '-O "{destino_final}" "{link}"'
    )
    message("Descargando: ", nombre_destino)
    system(cmd)
  })

  message("✅ Todos los archivos han sido descargados en: ", download_dir)
}
