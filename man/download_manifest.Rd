% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/download-manifest.R
\name{download_manifest}
\alias{download_manifest}
\title{download_manifest}
\usage{
download_manifest(
  folder_txts,
  output_file = "todos_los_links.txt",
  download_dir = "descargas"
)
}
\arguments{
\item{folder_txts}{Carpeta que contiene los archivos .txt con los enlaces de descarga.}

\item{output_file}{Nombre del archivo de salida donde se guardarán todos los enlaces.}

\item{download_dir}{Directorio donde se descargarán los archivos.}
}
\value{
archivos tar
}
\description{
función para descargar archivos a partir de un manifiesto o varios manifiestos. Esta función descarga archivos desde enlaces proporcionados en archivos .txt. Los enlaces deben estar en formato HTTP(s) y la función generará nombres de archivo únicos basados en la ruta de los enlaces para evitar conflictos. se necesitan credecianles almacenadas en .urs_cookies
}
\examples{
\dontrun{
download_manifest("/ruta/a/tus/archivos/txt")
}
}
