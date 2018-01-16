library(magrittr)
library(stringr)
library(geocode)
library(dplyr)
library(dtplyr)
library(data.table)
library(codebase)
options(datatable.print.class = TRUE)


# Read data ---------------------------------------------------------------

dt <- rbindlist(list(
  readr::read_csv("data/2017S1.csv"),
  readr::read_csv("data/2017S2.csv")
))  

# ETL ---------------------------------------------------------------------

dt[!str_detect(土地區段位置或建物區門牌, '^.{2}[縣市]'), 
   土地區段位置或建物區門牌 := str_c(county, 土地區段位置或建物區門牌)]

dt <- dt[str_detect(交易標的, "建物")]
dt <- dt[str_detect(土地區段位置或建物區門牌, "\\d+號$")]

get_mid_addr <- function(addr) {
  # addr = c("臺北市文山區保儀路109巷23弄1~30號",
  #          "臺北市文山區保儀路109巷23弄1~30",
  #          "臺北市文山區保儀路109巷23弄30號")
  m <- addr %>% str_match("(\\d+)[~](\\d+)號$")
  if (is.na(m[1])) return(addr)
  
  new_no <- ((as.integer(m[,2]) + as.integer(m[,3])) / 2) %>% as.integer()
  
  new_addr <- addr %>% str_replace("(\\d+[~]\\d+)號$", sprintf("%s號", new_no))
  new_addr
}

dt[, 土地區段位置或建物區門牌 := get_mid_addr(土地區段位置或建物區門牌)]

# Add geocode -------------------------------------------------------------

dt_geo <- dt %>% add_geocode(addr_var = "土地區段位置或建物區門牌",
                                   precise = TRUE, n_cpu = -1L)
dt_geo[, c("lat_y", "lon_x") := lapply(.SD, as.numeric), .SDcols = c("lat_y", "lon_x")]
# elapsed = 03h 54m 09s

# dt_geo %>% readr::write_csv("data/df_all_2017S1S2_geocode.csv")
# dt_geo <- readr::read_csv("data/df_all_2017S1S2_geocode.csv")
dt_geo_codebase <- add_codebase(dt_geo %>% filter(!is.na(lat_y)),
                                lng = "lon_x", lat = "lat_y")
# Time elapsed: 1.842773 mins

# Export ------------------------------------------------------------------

dt_geo_codebase %>% readr::write_csv("data/df_all_2017S1S2_geocode_codebase.csv")


