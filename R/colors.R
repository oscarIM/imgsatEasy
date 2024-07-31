#colors
blues <- c("#081D58", "#253494", "#225EA8", "#1D91C0", "#41B6C4", "#7FCDBB", "#C7E9B4", "#EDF8B1", "#FFFFD9")
reds <- c("#FFFFCC", "#FFEDA0", "#FED976", "#FEB24C", "#FD8D3C", "#FC4E2A", "#E31A1C", "#BD0026", "#800026")
oce_jets <- c("#00007F", "#000087", "#000090", "#000098", "#0000A1", "#0000AA", "#0000B2", "#0000BB", "#0000C3", 
  "#0000CC", "#0000D5", "#0000DD", "#0000E6", "#0000EE", "#0000F7", "#0001FF", "#0009FF", "#0012FF", "#001AFF", 
  "#0023FF", "#002BFF", "#0034FF", "#003CFF", "#0045FF", "#004DFF", "#0056FF", "#005EFF", "#0067FF", "#0070FF", 
  "#0078FF", "#0081FF", "#0089FF", "#0092FF", "#009AFF", "#00A3FF", "#00ACFF", "#00B4FF", "#00BDFF", "#00C5FF", 
  "#00CEFF", "#00D7FF", "#00DFFF", "#00E8FF", "#00F1FF", "#00F9FF", "#03FFFB", "#0BFFF3", "#14FFEA", "#1CFFE1", 
  "#25FFD9", "#2DFFD0", "#36FFC8", "#3EFFBF", "#47FFB6", "#50FFAE", "#58FFA5", "#61FF9D", "#69FF94", "#72FF8B",
  "#7AFF83", "#83FF7A", "#8BFF72", "#94FF69", "#9DFF61", "#A5FF58", "#AEFF50", "#B6FF47", "#BFFF3E", "#C8FF36",
  "#D0FF2D", "#D9FF25", "#E1FF1C", "#EAFF14", "#F3FF0B", "#FBFF03", "#FFF900", "#FFF100", "#FFE800", "#FFDF00",
  "#FFD700", "#FFCE00", "#FFC500", "#FFBD00", "#FFB400", "#FFAC00", "#FFA300", "#FF9A00", "#FF9200", "#FF8900", 
  "#FF8100", "#FF7800", "#FF7000", "#FF6700", "#FF5E00", "#FF5600", "#FF4D00", "#FF4500", "#FF3C00", "#FF3400", 
  "#FF2B00", "#FF2300", "#FF1A00", "#FF1200", "#FF0900", "#FF0100", "#F70000", "#EE0000", "#E60000", "#DD0000",
  "#D50000", "#CC0000", "#C30000", "#BB0000", "#B20000", "#AA0000", "#A10000", "#980000", "#900000", "#870000", 
  "#7F0000")

# Función para acceder a las paletas
#' @title get_palette
#' @rdname get_palette
#' @keywords internal
#' @param name as input
get_palette <- function(name) {
  switch(name,
         "blues" = blues,
         "reds" = reds,
         "oce_jets" = oce_jets,
         stop("Unknown palette name"))
}