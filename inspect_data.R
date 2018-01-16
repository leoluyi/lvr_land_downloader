library(data.table)

dt <- fread("data/df_all_2017S1S2_geocode.csv")

dt[is.na(lat_y), .(addr)]
