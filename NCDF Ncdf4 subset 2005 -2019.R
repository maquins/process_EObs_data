library(ncdf4)
library(raster)
library(rgdal)
library(rgeos)
library(stringr)
library(foreach)
library(dplyr)
library(doParallel)
library(lubridate)
library(reshape2)

cl<-makeCluster(6)
registerDoParallel(cl)

setwd("~/Climate_data_Input")


#all_files<-list.files(ndvi_path,recursive =T,'.nc')


ncdf_pth<-"~/Climate_data_Input/ncdf_files/"

sub_path_05_19<-"~/Climate_data_Input/ncdf_2005_2019/"

inp_fls<-data.frame(ncdf_file=list.files(ncdf_pth,pattern='.nc$',ignore.case =T),
                    path=list.files(ncdf_pth,pattern='.nc$',ignore.case =T,full.names =T),
                    stringsAsFactors =F) %>% 
  dplyr::mutate(ras_name=str_replace_all(ncdf_file,'.nc',"2005_2019.nc"),
                subset_name=paste0(sub_path_05_19,ras_name))




op1<-nc_open("~corpenicus_Data/Climate_data/tg_ens_mean_0.25deg_reg_v22.0e.nc")

lon<-ncvar_get(op1,'longitude')
lat<-ncvar_get(op1,'latitude')
time<-ncvar_get(op1,'time')

date0<-as.Date("1950-01-01")
all_dates<-date0+time


data_dates<-data.frame(data=all_dates,
                       year=year(all_dates),
                       month=month(all_dates)) %>% 
  dplyr::mutate(n=1:n())


year_month<-data_dates %>% 
  dplyr::group_by(year,month) %>% 
  summarise(.groups="drop",beg=min(n),end=max(n),size=n()) %>% 
  dplyr::mutate(N=1:n())

year_month_sub<-year_month %>% 
  dplyr::filter(year %in% 2005:2019)

p<-1

## get sizes

get_slab<-function(p){
    nc_fname<-inp_fls$subset_name[p]
    beg_end<-paste(c("time",min(year_month_sub$N),max(year_month_sub$N)),collapse =',')
    slab_str<-paste("time ncks -F  -d ",beg_end,"-p",ncdf_pth," ",inp_fls$ncdf_file[p],nc_fname," -O  --no_tmp_fl -4 -L 1")
    system(slab_str)
}
  
  
  ## run all and see

foreach(a=1:nrow(inp_fls))%do% get_slab(a)
    
 