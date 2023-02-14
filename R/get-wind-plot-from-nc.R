#' @title get_wind_plot
#' @description Función para obtener imágenes png de magnitud y velocidad de viento para datos del ERAS5 (formato .nc)
#' @param nc_file nombre archivo entrada 
#' @param time_step parametro que indica la temporalidad de los datos de ERAS 5 (defecto = 1; cada una hora)
#' @param week_number string que indica la semana a graficar
#' @param name_plot string que indica el nombre de la imagen de salida
#' @param height altura del plot
#' @param width ancho del plot
#' @param shp_file nombre del shp utilizado para la imagen final
#' @param fecha_1 fecha de inicio del el plot
#' @param fecha_2 fecha de termino del plot
#' @import ggplot2
#' @import sf
#' @import dplyr
#' @import rWind
#' @importFrom ncdf4 nc_open ncatt_get ncvar_get
#' @importFrom lubridate ymd_hms day wday isoweek month as_date
#' @importFrom purrr map map_depth
#' @importFrom tibble tibble as_tibble
#' @importFrom oce oce.colorsJet
#' @return imágenes png de velocidad y magnitud de viento
#' @export get_wind_plot
#' @examples
#' \dontrun{
#' nc_file <- "wind_20023_ene_w12c.nc"
#' time_step <- 1
#' week_number <- 12
#' name_plot <- "wind_plot_semana_12_complete.png" 
#' height = 11
#' width = 8
#' shp_file <- "/media/evolecolab/PortableSSD/seguimiento_arauco_2022/viento/Chile.shp"
#' fecha_1 <- "2023-01-23"
#' fecha_2 <- "2023-01-29"
#'get_wind_plot(nc_file = nc_file,time_step = time_step, week_number = week_number,name_plot = name_plot, height = height, width = width,shp_file = shp_file, fecha_1 = fecha_1, fecha_2 = fecha_2)
#' }
get_wind_plot <- function(nc_file, time_step, week_number, name_plot, height, width, shp_file, fecha_1, fecha_2) {
  sf_use_s2(FALSE)
  nc_data <- nc_open(nc_file)
  # get lon lat, time
  dims <- c("lon", "lat", "time")
  dims_list <- map(dims, ~ ncvar_get(nc = nc_data, varid = .))
  names(dims_list) <- dims
  tunits <- ncatt_get(nc_data, "time", "units")
  time_ini <- tunits$value
  time_ini <- ymd_hms(time_ini)
  time_step <- paste0(time_step, " ", "hours")
  time_range <- seq(
    from = time_ini,
    length.out = length(dims_list$time),
    by = time_step
  )
  dims_list$time_range <- time_range
  fields <- c("u", "v")
  arrays_list <- map(1:length(fields), ~ ncvar_get(nc = nc_data, varid = nc_data$var[[.]]$name))
  names(arrays_list) <- fields
  fillvalues_list <- map(1:length(fields), ~ ncatt_get(nc = nc_data, varid = nc_data$var[[.]]$name, attname = "_FillValue"))
  arrays_list <- map_depth(arrays_list, 1, ~ ifelse(. == fillvalues_list[[1]]$value, NA, .x))
  lonlattime <- as.matrix(expand.grid(dims_list$lon, dims_list$lat,dims_list$time_range))
  ### dataframe final###
  u_vec_long <- as.vector(arrays_list[[1]])
  v_vec_long <- as.vector(arrays_list[[2]])
  tmp <- bind_cols(lonlattime, u_vec_long, v_vec_long)
  colnames(tmp) <- c("lon", "lat", "date", "u", "v")
  #target_week <- paste0("semana ", week_number)
  tmp <- tmp %>%
    mutate(
      date = as.Date(date),
      day = day(date),
      day_name = wday(date, label = TRUE, abbr = FALSE),
      day_name = str_to_sentence(day_name),
      semana = isoweek(date),
      month = month(date),
      year = year(date),
      lon = as.numeric(lon),
      lat = as.numeric(lat)
    ) %>%
    drop_na() %>%
    filter(between(date, as_date(fecha_1), as_date(fecha_2)))
  # remove cosas
  rm(list = c(
    "v_vec_long", "u_vec_long", "time_range", "arrays_list", "fillvalues_list", "dims_list",
    "nc_data", "tunits"
  ))
  
  speed_dir <- as_tibble(uv2ds(tmp$u, tmp$v))
  df <- bind_cols(tmp, speed_dir) %>%
    mutate(date = as_date(date))
  rm(list = c("tmp", "speed_dir"))
  # We compute the arithmetic mean for the wind speed. The direction as the circular mean
  # fn to calculate circular mean for direcction
  ####### inicio funciones trigonometricas tomadas de Rwind#####
  circ_mean <- function(deg) {
    rad_m <- (deg * pi) / (180)
    mean_cos <- mean(cos(rad_m))
    mean_sin <- mean(sin(rad_m))
    theta <- rad2deg(atan(mean_sin / mean_cos))
    if (mean_cos < 0) theta <- theta + 180
    if ((mean_sin < 0) & (mean_cos > 0)) theta <- theta + 360
    theta
  }
  rad2deg <- function(rad) {
    (rad * 180) / (pi)
  }
  deg2rad <- function(deg) {
    (deg * pi) / (180)
  }
  ##############################################################################
  df_final <- df %>%
    group_by(lon, lat, day_name, semana, date) %>%
    summarise(
      speed_mean = mean(speed, na.rm = TRUE),
      dir = circ_mean(dir),
      u = mean(u, na.rm = TRUE),
      v = mean(v, na.rm = TRUE)
    )
  # filtro para la region
  df_plot <- df_final %>%
    filter(between(lon, -76, -70)) %>%
    filter(between(lat, -44, -34))
  ######### seleccionar####
  days_ord <- c("Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo")
  indx <- days_ord %in% unique(df_plot$day_name)
  days_ord <- days_ord[indx]
  df_plot$day_name <- factor(df_plot$day_name, levels = days_ord)
  df_sf <- df_plot %>%
    st_as_sf(coords = c("lon", "lat")) %>%
    st_set_crs(4326) %>%
    group_by(day_name) %>%
    group_split()
  grid <- df_sf[[1]] %>%
    st_make_grid(n = c(100, 100)) %>%
    st_sf() %>%
    mutate(id = row_number())
  fn_grid <- function(grid, data_sf) {
    grid_filled <- grid %>% mutate(
      id = n(),
      contained = lapply(st_contains(st_sf(geometry), data_sf), identity),
      obs = sapply(contained, length),
      day_name = unique(data_sf$day_name),
      u = sapply(contained, function(x) {
        mean(data_sf[x, ]$u, na.rm = TRUE)
      }),
      v = sapply(contained, function(x) {
        mean(data_sf[x, ]$v, na.rm = TRUE)
      }),
      speed_mean = sapply(contained, function(x) {
        mean(data_sf[x, ]$speed_mean, na.rm = TRUE)
      })
    )
    grid_filled
  }
  grid_filled <- map(df_sf, ~ fn_grid(grid = grid, data_sf = .))
  grid_filled <- bind_rows(grid_filled)
  grid_filled <- grid_filled %>% dplyr::select(obs, u, v, speed_mean, day_name)
  grid_points <- grid_filled %>%
    st_centroid() %>%
    st_coordinates() %>%
    as_tibble() %>%
    rename(lon = X, lat = Y)
  attributes <- grid_filled
  ## remove the geometry and remain with the attributes
  st_geometry(attributes) <- NULL
  gridded_wind <- grid_points %>% bind_cols(attributes)
  chile <- st_read(shp_file)
  zona_sur <- chile %>% filter(codregion %in% c(7,8,9,16))
  lmax <- max(ceiling(df_plot$speed_mean))
   daily_plot <- ggplot(data = df_plot) +
    geom_raster(aes(x = lon, y = lat, fill = speed_mean)) +
    scale_fill_gradientn(
      limits = c(0, ceiling(lmax)),
      colours = alpha(oce.colorsJet(120), 0.7),
      na.value = "white",
      name = "Velocidad promedio (m/s)"
    ) + 
    geom_sf(data = zona_sur, col = "white", fill = "gray70", lwd = 1) +
    coord_sf(xlim = c(-76, -70.5), ylim = c(-36.4, -38.7)) +
    geom_segment(
      data = gridded_wind,
      aes(x = lon, xend = lon + u / 20, y = lat, yend = lat + v / 20),
      arrow = arrow(length = unit(0.1, "cm")),
      lineend = "round", 
      linejoin = "mitre",
      col = "black"
    ) +
    scale_x_longitude(ticks = 2) +
    scale_y_latitude(ticks = 0.5) +
    theme_bw() +
    guides(fill = guide_colourbar(barwidth = 10, 
                                  barheight = 0.5,
                                  title.position = "top")) +
    theme(axis.text = element_text(size = 11, colour = "black"),
          panel.spacing = unit(1, "lines"),
          legend.position = "bottom",
          legend.box = "vertical",
          ) +
    labs(
      x = NULL, y = NULL,
      title = "Velocidad y dirección promedio del viento: VIII Región",
      subtitle = paste0("Semana ",  week_number, ": ", min(df_plot$date), " al ",  max(df_plot$date),"\n",
      "presión: 1000hPa"),
      caption = "Fuente: ERA5 hourly data on pressure levels from 1959 to present; \n
          https://cds.climate.copernicus.eu/"
    ) +
      facet_wrap(~day_name, ncol = 2)
  #ggview(plot = daily_plot,device = "png",width = width, height = height)
  ggsave(filename = name_plot, plot = daily_plot, device = "png", units = "in", dpi = 300, height = height, width = width)
}

