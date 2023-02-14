#' @title get_dataframe
#' @description Función para generar dataframes en formato .parquet a partir de imágenes satelitales L3
#' @param dir_input directorio en donde se almacenan las imágenes L3 (formato .nc)
#' @param dir_output directorio de destino
#' @param type tipo de dataframe(to_plot o full_df)
#' @param var_name vector de tamaño 1 con el nombre de la variable a analizar ("chlor_a", "sst", "Rrs_645", "pic", "poc", "nflh")
#' @param season vector de tamaño 1 con la temporalidad deseada para el dataframe("year" or "month" por ahora)
#' @param n_cores vector tamaño 1 que indique el numero de núcleos a usar. Por defecto, n_cores = 1
#' @return archivos parquet
#' @importFrom fs dir_ls dir_create path_file file_delete
#' @importFrom tibble tibble
#' @importFrom lubridate as_date year month week day
#' @importFrom dplyr case_when group_by group_split filter pull bind_rows mutate
#' @importFrom tidyr drop_na
#' @importFrom purrr map walk
#' @importFrom raster stack calc clusterR endCluster
#' @importFrom tictoc tic toc
#' @importFrom arrow write_parquet read_parquet
#' @export get_dataframe
#' @examples
#' \dontrun{
#' dir_input <- "/media/evolecolab/PortableSSD/Proyecto_Packard/chlor_a/join_l3"
#' dir_output <- "/home/evolecolab/Escritorio/proyecto_packard/chlor_a/to_plot"
#' type <- "to_plot"
#' var_name <- "chlor_a"
#' stat_function <- "median"
#' season <- "month"
#' n_cores <- 8
#'get_dataframe(dir_input = dir_input, dir_output = dir_output, type = "to_plot",var_name = var_name, stat_function = stat_function, season = season,n_cores = n_cores)
#' }
get_dataframe <- function(dir_input, dir_output, type, var_name, stat_function = "median", season, n_cores = 4) {
  ####mensajes de error####
  tic(msg = "Duración total análisis")
  files <- dir_ls(path = dir_input, regexp = "_L3mapped.nc$", recurse = TRUE) %>%
    tibble(
      mapped_files = .,
      files = path_file(mapped_files)) %>%
    separate(data = ., into = c("year", "month_number", "month_name", "day") , col = "files",  sep = "_", remove = FALSE, extra = "drop") %>%
    mutate(month_number_1 = as.numeric(month_number),
           date = as_date(paste0(year, "-", month_number_1, "-", day)))
 
  if (type == "full_df") {
    dir <- dir_create(path = paste0(dir_output, "/all_dataframe")) %>% 
      fs_path()
     list_day_files <- files %>% 
      group_by(date) %>% 
      group_split()
    input_files <- map(list_day_files, ~ pull(., "mapped_files"))
    input_dates <- map(list_day_files, ~ pull(., "date"))
    single_day_to_df <- function(input_files, dates){
      ####para archivos únicos####
      if (length(input_files) >= 2) {
        stack <- raster::stack(input_files, varname = var_name)
        df_day <- raster::calc(stack, fun = function(x) do.call(mean, list(x, na.rm = TRUE))) %>%
          setNames(var_name) %>% 
          raster::as.data.frame(x = ., xy = TRUE)
        df_day <- df_day %>% 
          mutate(date = unique(dates)) %>%
          drop_na() %>%
          tmp_name <- paste0(unique(dates), "_tmp.parquet")
        out <- paste0(dir, "/", tmp_name)
        write_parquet(x = df_day, sink = out)
        rm(list = c("stack", "df_day", "tmp_name", "out")) 
      } else {
        df_day <- raster::raster(input_files, varname = var_name) %>% 
          raster::as.data.frame(x = ., xy = TRUE) %>% 
          drop_na() %>%
          dplyr::rename(!!var_name := 3)  %>% 
          dplyr::mutate(date = dates)
        tmp_name <- paste0(dates, "_tmp.parquet")
        out <- paste0(dir, "/", tmp_name)
        write_parquet(x = df_day, sink = out)
        rm(list = c("df_day", "tmp_name", "out"))
      }
    }
    purrr::walk2(input_files, input_dates, ~single_day_to_df(input_file = .x, date = .y), .progress = TRUE)
    ####crear archivos mensuales y exportar####
    files <- dir_ls(path = dir, regexp = "_tmp.parquet$", recurse = FALSE) %>%
      tibble(
        data_frame_files = .,
        date = as_date(path_file(data_frame_files), format = "%Y-%m-%d"),
        year = year(date),
        day = day(date),
        month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE))
        )
    list_files <- files %>%
      group_by(year, month_name) %>%
      group_split() %>%
      map(., ~ pull(., "data_frame_files"))
    get_final_df <- function(files) {
      df <- map(files, ~ read_parquet(.)) %>%
        bind_rows() %>%
        mutate(
          year = year(date),
          month = month(date),
          month_name = paste0(sprintf("%02d", month(date)), "_", month(date, label = TRUE, abbr = FALSE))
        )
      tmp_name <- paste0(unique(df$year), "_", unique(df$month_name),"_", var_name, ".parquet")
      out <- paste0(dir, "/", tmp_name)
      write_parquet(x = df, sink = out)
      rm(list = c("df", "tmp_name", "out"))
      file_delete(files)
    }
    purrr::walk(list_files , ~get_final_df(files = .), .progress = TRUE)
  }
  if (type == "to_plot") {
    dir <- dir_create(path = paste0(dir_output, "/to_plot")) %>% 
      fs_path()
     if (season == "year") {
      df_files <- files %>%
        group_by(year) %>%
        group_split() %>%
        setNames(map(., ~ unique(.[["year"]])))
    }
    if (season == "month") {
      df_files <- files %>%
        group_by(month_number_1) %>%
        group_split() %>%
        setNames(map(., ~(paste0(unique(.[["month_number"]]), "_", unique(.[["month_name"]])))))
         }
      get_season_raster <- function(df_input) {
      tmp <- df_input$mapped_files
      name <- paste0(unique(df_input$month_number),"_", unique(df_input$month_name), "_", min(df_input$year),"_",max(df_input$year))
      stack <- raster::stack(x = tmp, varname = var_name)
      beginCluster(n_cores)
      if (stat_function == "median") {
        tmp <- clusterR(stack, calc, args = list(fun = median, na.rm = TRUE))  
        }
      if (stat_function == "mean") {
        tmp <- clusterR(stack, calc, args = list(fun = mean, na.rm = TRUE))
        }
      endCluster()
     df <- tmp %>% setNames(var_name) %>% 
       raster::as.data.frame(x = ., xy = TRUE)
     tmp_name <- paste0(name,"_",var_name, "_", stat_function, ".parquet")
     out <- paste0(dir, "/", tmp_name)
     write_parquet(x = df, sink = out)
     rm(list = c("stack", "df","tmp", "tmp_name", "out"))
      }
    #get_season_raster(input_list = input_files[1])
    walk(df_files, ~ get_season_raster(df_input = .), .progress = TRUE)
    #tratar de cambiar a map
    cat("Listo... \n\n")
  }
  toc()
}



