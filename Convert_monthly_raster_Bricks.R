
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
library(dismo)
library(tmap)
library(tmaptools)
library(dismo)

cl<-makeCluster(6)
registerDoParallel(cl)

#setwd("")

ncdf_pth<-"~/Climate_data_Input/ncdf_2005_2019/"

brick_pth<-"~/Climate_data_Input/raster_bricks/"

inp_fls<-data.frame(ncdf_file=list.files(ncdf_pth,pattern='.nc$',ignore.case =T),
                    path=list.files(ncdf_pth,pattern='.nc$',ignore.case =T,full.names =T),
                    stringsAsFactors =F) %>% 
  dplyr::mutate(ras_name=str_replace_all(ncdf_file,'.nc',".tif"),
    monthly_brick_name=paste0(brick_pth,ras_name))


beg_year<-2005
end_year<-2019

yr_mn_pre<-expand.grid(month=1:12,year=2005:2019) %>% 
  dplyr::mutate(year_mon=paste0(year,'_', str_pad(month,width =2,side='left',pad=0)))

inp_fls$monthly_brick_name[1]
p<-1
var_corp<-"pp|rr|tg|tn|tx"

loop_Quarter_files<-function(p){
  var_corp<-"pp|rr|tg|tn|tx"
  yr_mn_pre<-expand.grid(month=1:12,year=2005:2019) %>% 
    dplyr::mutate(year_mon=paste0(year,'_', str_pad(month,width =2,side='left',pad=0)))
  op<-nc_open(inp_fls$path[p])
  lat<-ncvar_get(op,'latitude')
  lon<-ncvar_get(op,'longitude')
  #nc_close(op)
  name_nc<-paste(names(op$var),collapse =" ")
  var_ext<-str_extract(name_nc,var_corp)
  
  if(var_ext=="pp"){
    name_ras<-'pressure'
  }else if(var_ext=="rr"){
    name_ras<-'prec'
  }else if(var_ext=="tg"){
    name_ras<-'mean_temp'
  }else if(var_ext=="tn"){
    name_ras<-'min_temp'
  }else{
    name_ras<-'max_temp'
  }
  var_array<-ncvar_get(op,var_ext)
  dim(var_array)
  nc_close(op)
  
  dimnames(var_array)<-list(lon,lat,yr_mn_pre$year_mon)
  ##compute annual
  Yr<-2005
  
  get_quarter<-function(Yr){
    current_yr<-which(yr_mn_pre$year==Yr)
    arr_sub<-var_array[,,current_yr]
    
    var_long<-melt(arr_sub) %>% 
      dplyr::rename(x=1,y=2,year_month=3,var=4) %>% 
      dplyr::mutate(year=as.numeric(str_split_fixed(year_month,'_',n=2)[,1]),
                    month=as.numeric(str_split_fixed(year_month,'_',n=2)[,2]),
                    quarter_yr=case_when(month %in% 1:3 ~ '01',
                                         month %in% 4:6 ~ '02',
                                         month %in% 7:9 ~ '03',
                                         month %in% 10:12 ~ '04',
                                         TRUE~as.character(NA)))
    
    ## aggregate by year
    
    if(var_ext=="rr"){
      var_year<-var_long %>% 
        dplyr::group_by(x,y,quarter_yr) %>% 
        dplyr::summarise(.groups='drop',var=sum(var,rm.na=T)) %>% 
        dplyr::group_by(x,y) %>% 
        tidyr::spread(quarter_yr,var)
    }else{
      var_year<-var_long %>% 
        dplyr::group_by(x,y,quarter_yr) %>% 
        dplyr::summarise(.groups='drop',var=mean(var,rm.na=T)) %>% 
        dplyr::group_by(x,y) %>% 
        tidyr::spread(quarter_yr,var)
    }
    
    brick_var<-rasterFromXYZ(var_year)
    quarts<-str_pad(1:4,pad=0,side='left',width =2)
    names(brick_var)<-paste0(name_ras,'_',Yr,'Q',quarts)
    brick_var
  }
  
 all_quarter<-foreach(a=2005:2019,.final =brick,
                      .packages =c("raster","dplyr","stringr","reshape2")) %dopar% get_quarter(a)
 all_quarter1<-all_quarter
 values(all_quarter1)<-getValues(all_quarter)

list_out<-list(all_quarter1)
names(list_out)<-name_ras
list_out
}

time_n<-system.time({
  all_quater_indicators<-foreach(a=1:nrow(inp_fls),.combine = c) %do% loop_Quarter_files(a)
  
})

time_n[3]/60

plot(all_quater_indicators$mean_temp$mean_temp_2005Q04)

yr_mn_pre<-expand.grid(month=1:12,year=2005:2019) %>% 
  dplyr::mutate(year_mon=paste0(year,'_', str_pad(month,width =2,side='left',pad=0)))
  
prec_idx<-which(str_detect(inp_fls$ncdf_file,'rr'))
tmin_idx<-which(str_detect(inp_fls$ncdf_file,'tn'))
tmax_idx<-which(str_detect(inp_fls$ncdf_file,'tx'))
  
op_prec<-nc_open(inp_fls$path[prec_idx])
op_min<-nc_open(inp_fls$path[tmin_idx])
op_max<-nc_open(inp_fls$path[tmax_idx])
  
lat<-ncvar_get(op_prec,'latitude')
lon<-ncvar_get(op_prec,'longitude')
  #nc_close(op)
 
  
var_array_pre<-ncvar_get(op_prec,"rr")
var_array_tmin<-ncvar_get(op_min,"tn")
var_array_tmax<-ncvar_get(op_max,"tx")
  

  
dimnames(var_array_pre)<-list(lon,lat,yr_mn_pre$year_mon)
dimnames(var_array_tmin)<-list(lon,lat,yr_mn_pre$year_mon)
dimnames(var_array_tmax)<-list(lon,lat,yr_mn_pre$year_mon)
  ##compute annual
Yr<-2005
  
get_biovars<-function(Yr){
  
  current_yr<-which(yr_mn_pre$year==Yr)
  
  prec_arr_sub<-var_array_pre[,,current_yr]
  tmin_arr_sub<-var_array_tmin[,,current_yr]
  tmax_arr_sub<-var_array_tmax[,,current_yr]
  
  for_bri_pre<-melt(prec_arr_sub) %>% 
    dplyr::rename(x=1,y=2,year_month=3,var=4) %>% 
    dplyr::group_by(x,y) %>% 
    tidyr::spread(year_month,var)
  
  for_bri_tmin<-melt(tmin_arr_sub) %>% 
    dplyr::rename(x=1,y=2,year_month=3,var=4) %>% 
    dplyr::group_by(x,y) %>% 
    tidyr::spread(year_month,var)
  
  for_bri_tmax<-melt(tmax_arr_sub) %>% 
    dplyr::rename(x=1,y=2,year_month=3,var=4) %>% 
    dplyr::group_by(x,y) %>% 
    tidyr::spread(year_month,var)
  
  ## aggregate by year
  
  brick_pre<-rasterFromXYZ(for_bri_pre)
  brick_tmin<-rasterFromXYZ(for_bri_tmin)
  brick_tmax<-rasterFromXYZ(for_bri_tmax)
  
  biovars_yr<-biovars(prec=brick_pre,
                      tmin=brick_tmin,
                      tmax=brick_tmax)
  
  names(biovars_yr)<-paste0(names(biovars_yr),'_',Yr)
  
  biovars_yr
  
}


biovars_all<-foreach(a=2005:2019,.final =brick,
                     .packages =c("raster","dplyr","stringr","reshape2","dismo")) %dopar% get_biovars(a)

biovars_all1<-biovars_all

values(biovars_all1)<-getValues(biovars_all)
