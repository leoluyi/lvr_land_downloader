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
  fread("data/2017S1.csv", colClasses = "character"),
  fread("data/2017S2.csv", colClasses = "character"),
  fread("data/2017S3.csv", colClasses = "character")
))  

# dt <- dt[, .(編號, county, 土地區段位置或建物區門牌, 交易標的)]

dt_geo_codebase_1 <- 
  fread("data/df_all_2017S1S2_geocode_codebase.csv", colClasses = "character") %>% 
  data.table() %>% 
  .[, .(編號, addr, lat = lat_y, lng = lon_x, addr_norm, CODEBASE)]
dt_geo_codebase_2 <- 
  fread("data/dt_2017S3_geo_codebase.csv", colClasses = "character") %>% 
  data.table() %>% 
  .[, .(編號, addr, lat = lat_y, lng = lon_x, addr_norm, CODEBASE)]

dt_geo_codebase_3 <- fread("data/dt_left_geocode.csv")
dt_geo_codebase_3 <- dt_geo_codebase_3[!is.na(lat),
                                       .(編號, addr = 土地區段位置或建物區門牌,
                                           lat, lng, addr_norm)]

dt_geo_codebase_4 <- fread("data/dt_left_geocode2.csv")
dt_geo_codebase_4 <- dt_geo_codebase_4[!is.na(lat),
                                       .(編號, addr = 土地區段位置或建物區門牌,
                                           lat, lng, addr_norm)]


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


# Re-geocode ---------------------------------------------------------------

# dt_combine <- rbind(
#   dt[, .(編號, county, 土地區段位置或建物區門牌, 交易標的)] %>% merge(dt_geo_codebase_1, by = "編號"),
#   dt[, .(編號, county, 土地區段位置或建物區門牌, 交易標的)] %>% merge(dt_geo_codebase_2, by = "編號"),
#   dt[, .(編號, county, 土地區段位置或建物區門牌, 交易標的)] %>% merge(dt_geo_codebase_3, by = "編號"),
#   fill = TRUE
# )
# dt_left <- dt[!編號 %in% dt_combine$編號]
# 
# dt_left_geocode <- dt_left %>% 
#   add_geocode(addr_var = "土地區段位置或建物區門牌", source = "t", n_cpu = 3)
# 
# dt_left_geocode %>% fwrite("data/dt_left_geocode2.csv")


# # Add geocode -------------------------------------------------------------
# 
# dt_geo <- dt %>% add_geocode(addr_var = "土地區段位置或建物區門牌",
#                              precise = TRUE, n_cpu = -1L)
# dt_geo[, c("lat_y", "lon_x") := lapply(.SD, as.numeric), .SDcols = c("lat_y", "lon_x")]
# # elapsed = 03h 54m 09s
# 
# # dt_geo %>% readr::write_csv("data/df_all_2017S1S2_geocode.csv")
# # dt_geo <- readr::read_csv("data/df_all_2017S1S2_geocode.csv")
# dt_geo_codebase <- add_codebase(dt_geo %>% filter(!is.na(lat_y)),
#                                 lng = "lon_x", lat = "lat_y")
# # Time elapsed: 1.842773 mins
# 
# 
# # Export ------------------------------------------------------------------
# 
# dt_geo_codebase %>% readr::write_csv("data/df_all_2017S1S2_geocode_codebase.csv")
# 


# Add codebase ------------------------------------------------------------

dt_combine <- rbind(
  dt %>% merge(dt_geo_codebase_1, by = "編號"),
  dt %>% merge(dt_geo_codebase_2, by = "編號"),
  dt %>% merge(dt_geo_codebase_3, by = "編號"),
  dt %>% merge(dt_geo_codebase_4, by = "編號"),
  fill = TRUE
)
dt_combine[, `:=`(lng = as.numeric(lng), lat = as.numeric(lat))] %>% 
  .[!is.na(lat) & is.na(CODEBASE)] %>% 
  .[, CODEBASE := codebase::codebase(lng, lat)]

dt_combine <- dt_combine[!is.na(CODEBASE)]
dt_combine %>% fwrite("result/dt_2017S1S2S3_geo_codebase.csv")
