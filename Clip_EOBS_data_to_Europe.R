## clip ISIMIP 3B data to EUROPE

library(ncdf4)
library(raster)
library(dplyr)
library(foreach)
library(doParallel)
library(rgeos)
library(dismo)
library(reshape2)
library(sf)
library(stringr)
library(rgdal)
library(ggplot2)
library(lubridate)

base_nc_path<-"./Input_files/"
base_nc_path_EU<-"./Outputs/EU_files/"
path.expand(base_nc_path)

eu_NUTSregions<-readOGR("./Shapes_files/NUTS_RG_20M_2021_4326/NUTS_RG_20M_2021_4326.shp")
eu_NUTSregions_sf<-st_as_sf(eu_NUTSregions)



ggplot()+
  geom_sf(data=eu_NUTSregions_sf)

## Clip to EU using NCO command ncks
path_NC<-paste0(getwd(),'/Input_files/')
path_NC_EU<-paste0(getwd(),'/Outputs/EU_files/')

## the EOBS v25 is daily for the period 1950-01-01 - 2021-12-31
# we clip mean temp ncdf tg_ens_mean_0.25deg_reg_v25.0e.nc to EU using bounding box lat 18 to 83 North, long -45 to 62 East but e-OBS
# we also just keep data for for example 2020 - 2021
## we need to find indices for the years 2020 to 2021 
date_beg<-as.Date("1950-01-01")
date_end<-as.Date("2021-12-31")

data_dates<-data.frame(date=seq.Date(date_beg,date_end,by="day")) %>% 
  dplyr::mutate(year=year(date),N=1:n())

time_beg<-min(which(data_dates$year==2020))
time_end<-max(which(data_dates$year==2021))

oufile_Name<-paste0(path_NC_EU,'EU_tg_ens_mean_0.25deg_reg_v25.0e_2020_2021.nc')

clip_cms<-paste0("time ncks  -F -d time,25568,26298  -d latitude,18.,83. -d longitude,-45.,62. -p ",path_NC, ' tg_ens_mean_0.25deg_reg_v25.0e.nc',' ',oufile_Name,' -O --no_tmp_fl -4 -L 1')

unlink(oufile_Name)
system(clip_cms)

op<-nc_open(oufile_Name)
op1<-nc_open(paste0(getwd(),'/Input_files/tg_ens_mean_0.25deg_reg_v25.0e.nc'))

365*2
tmean<-ncvar_get(op,"tg")[,,1:2]

dim(tmean)

lat<-ncvar_get(op,"latitude")
lon<-ncvar_get(op,"longitude")

dimnames(tmean)<-list(lon,lat,1:2)

tmean_brick<-melt(tmean) %>% 
  dplyr::rename(x=Var1,y=Var2,time=Var3) %>% 
  dplyr::group_by(x,y) %>% 
  tidyr::spread(time,value) %>% 
  rasterFromXYZ()

plot(tmean_brick$X1)

## to compute Biovars you need monthly summaries
## again you can use nco to compute the monthly summaries



