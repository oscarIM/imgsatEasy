devtools::install_github("oscarIM/imgsatEasy")
library(imgsatEasy)
dir_input <- "/home/evolecolab/Escritorio/Analisis_Arauco"
dir_output <- paste0(dir_input, "/", "imagenes_L3")
var_name <- "chlor_a"
n_cores <- 8
res_l2 <- "1"
res_l3 <- "1km"
north <- -36.77544
south <- -37.29073
west <- -73.67165
east <- -73.11573
get_L3(dir_input = dir_input, dir_output = dir_output, var_name = var_name, n_cores = n_cores, res_l2 = res_l2, res_l3 = res_l3, north = north, south = south, west = west, east = east)
################################################################################
dir_input <- "/media/evolecol/Transcend/sat_img/Proyecto_ARAUCO/analisis_imagenes/OC/CHLOR_A/all/input/nc_files/requested_files"
dir_output <-  paste0("/home/evolecol/Escritorio/R_package/test_package", "/", "raster_all")
var_name <- "chlor_a"
n_cores <- 12
season = "mes"
raster_function <- "median"
get_raster_fix(dir_input = dir_input, dir_output = dir_output, season = season,raster_function = raster_function,var_name = var_name,n_cores = n_cores)
#aagregar el get_csv_fix
################################################################################
dir_input <- "/home/evolecol/Escritorio/R_package/test_package/rasters/resultados_raster/"
dir_output <- paste0("/home/evolecol/Escritorio/R_package/test_package", "/", "gapfill")
season <- "mes"
shp_mask_file <- "/home/evolecol/Escritorio/R_package/test_package/Golfo_Arauco_prj2.shp"
n_cores <- 12
get_filled_raster(dir_input = dir_input, dir_output = dir_output, shp_mask_file = shp_mask_file,season = season, n_cores = n_cores)
################################################################################
dir_input <- "/home/evolecol/Escritorio/R_package/test_package/chlor_A/"
dir_output <- paste0( "/home/evolecol/Escritorio/R_package/test_package/test_ct")
date_1 <- "2010-01-01"
date_2 <- "2010-01-29"
name_time <- "enero_2010"
var_name <- "chlor_a"
raster_function <- "median"
get_raster_ct(dir_input = dir_input, dir_output = dir_output, date_1 = date_1, date_2 = date_2, name_time = name_time,var_name = var_name, raster_function = raster_function)
################################################################################
dir_input <- "/home/evolecol/Escritorio/R_package/test_package/rasters/resultados_raster"
dir_output <- "/home/evolecol/Escritorio/R_package/test_package/climatologia"
season <- "año"
raster_function <- "median"
var_name <-  "chlor_a"
n_col <- 3
n_row <- 4
name_output <- "climatologia_chlor_a.png"
shp_file <- "/home/evolecol/Escritorio/R_package/test_package/Golfo_Arauco_prj2.shp"
res <- 300
heigth <- 7
width <- 9
get_clim(dir_input = dir_input, dir_output = dir_output, season = season ,raster_function = raster_function , var_name = var_name, shp_file = shp_file, n_col = n_col, n_row = n_row, name_output = name_output,res = res, heigth = heigth, width = width)
################################################################################


