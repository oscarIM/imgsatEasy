#colors
blues <- c("#081D58", "#253494", "#225EA8", "#1D91C0", "#41B6C4", "#7FCDBB", "#C7E9B4", "#EDF8B1", "#FFFFD9")
reds <- c("#FFFFCC", "#FFEDA0", "#FED976", "#FEB24C", "#FD8D3C", "#FC4E2A", "#E31A1C", "#BD0026", "#800026")

# Función para acceder a las paletas
#' @title get_palette
#' @rdname get_palette
#' @keywords internal
#' @param name as input
get_palette <- function(name) {
  switch(name,
         "blues" = blues,
         "reds" = reds,
         stop("Unknown palette name"))
}