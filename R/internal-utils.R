#  función interna de imgsatEasy
#
# \code{.internal_csv} es una función interna
#
.internal_csv <- function(dir) {
  setwd(dir)
  files <- dir_ls(regexp = ".nc$", recurse = T)
  tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
  name_year <- tmp[length(tmp) - 1]
  name_month <- tmp[length(tmp)]
  name_out <- paste0(name_year, "_", name_month, "_", var_name, ".csv")
  possible_tidync <- possibly(.f = tidync, otherwise = NULL)
  tmp <- map(files, ~ possible_tidync(.)) %>% keep(~ !is.null(.))
  tmp <- map(tmp, ~ hyper_tibble(.))
  dates <- case_when(
    var_type == "sst" ~ as_date(names(tmp), format = "%Y%m%d"),
    var_type == "oc" ~ as_date(names(tmp), format = "%Y%j")
  )
  tmp <- map2(tmp, dates, ~ mutate(.x, date = .y))
  final <- bind_rows(tmp)
  write_csv(final, file = name_out)
  setwd(dir_raiz)
}
#  función interna de imgsatEasy
#
# \code{.internal_raster} es una función interna
#
.internal_raster <- function(dir, raster_function) {
  setwd(dir)
  files <- dir_ls(regexp = ".nc$", recurse = T)
  # configurando nombre del stack final
  tmp <- str_split(string = path_wd(), pattern = "/", simplify = T)
  if (season == "year") {
    name_year <- tmp[length(tmp)]
    name_file <- paste0(name_year, "_", var_name)
  }
  if (season == "month") {
    name_year <- tmp[length(tmp) - 1]
    name_month <- tmp[length(tmp)]
    name_file <- paste0(name_year, "_", name_month, "_", var_name)
  }
  if (season == "week") {
    name_year <- tmp[length(tmp) - 2]
    name_month <- tmp[length(tmp) - 1]
    name_week <- tmp[length(tmp)]
    name_file <- paste0(name_year, "_", name_month, "_", name_week, "_", var_name)
  }
  # proceso de los archivos
  possible_nc_open <- possibly(.f = nc_open, otherwise = NULL)
  possible_nc2raster <- possibly(.f = nc2raster, otherwise = NULL)
  nc_files_tmp <- map(files, ~ possible_nc_open(.))
  nc_file_tmp <- nc_files_tmp %>% keep(~ !is.null(.))
  nc_raster_tmp <- map(nc_files_tmp, ~ possible_nc2raster(., var_name, lonname = "lon", latname = "lat"))
  nc_raster_tmp <- nc_raster_tmp %>% keep(~ !is.null(.))
  nc_raster_flip_tmp <- map(nc_raster_tmp, ~ flip(., "y"))
  rasters <- map(nc_raster_flip_tmp, ~ rotate(.))
  # ext_df <- purrr::map(rasters, ~raster::extent(.))
  # xmin <- purrr::map(ext_df, "xmin") %>% bind_rows()
  # xmax <- purrr::map(ext_df, "xmax") %>% bind_rows()
  # ymin <- purrr::map(ext_df, "ymin") %>% bind_rows()
  # ymax <- purrr::map(ext_df, "ymax") %>% bind_rows()
  # final_ext <- data.frame("xmin" = t(xmin), "xmax" = t(xmax), "ymin" = t(ymin), "ymax" = t(ymax))
  # ext_mask <- raster(ymx = max(final_ext$ymax), ymn = min(final_ext$ymin), xmn = min(final_ext$xmin), xmx = max(final_ext$xmax), resolution = 0.01)
  # rasters <- purrr::map(rasters, ~raster::resample(., ext_mask, method = "bilinear"))
  rasters <- stack(rasters)
  if (raster_function == "median") {
    stack <- raster::calc(rasters, fun = median, na.rm = TRUE)
    name_file <- paste0(name_file, "_", raster_function)
    stack <- rast(stack)
    writeRaster(x = stack, filename = paste0(name_file, ".tif"), overwrite = TRUE)
  }
  if (raster_function == "mean") {
    stack <- raster::calc(rasters, fun = mean, na.rm = TRUE)
    name_file <- paste0(name_file, "_", raster_function)
    stack <- rast(stack)
    writeRaster(x = stack, filename = paste0(name_file, ".tif"), overwrite = TRUE)
  }
  setwd(dir_output)
}
