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
# shapefile
one_shape1<-readOGR("~/NUTS_RG_01M_2021_4326_LEVL_3.shp/NUTS_RG_01M_2021_4326_LEVL_3.shp","NUTS_RG_01M_2021_4326_LEVL_3")




#all_files<-list.files(ndvi_path,recursive =T,'.nc')


clim_path<-"~/corpenicus_Data/Climate_data"
out_fls<-"~/Climate_data_Input/ncdf_files/"

inp_fls<-data.frame(ncdf_file=list.files(clim_path,pattern='.nc$',ignore.case =T),
                    path=list.files(clim_path,pattern='.nc$',ignore.case =T,full.names =T),
                    stringsAsFactors =F) %>% 
  dplyr::mutate(monthly_fname=paste0(out_fls,'monthly_',ncdf_file),
                minthly_nc_name=paste0('monthly_',ncdf_file))


op1<-nc_open("~/corpenicus_Data/Climate_data/tg_ens_mean_0.25deg_reg_v22.0e.nc")

lon<-ncvar_get(op1,'longitude')
lat<-ncvar_get(op1,'latitude')
time<-ncvar_get(op1,'time')

date0<-as.Date("1950-01-01")
all_dates<-date0+time


data_dates<-data.frame(data=all_dates,
                       year=year(all_dates),
                       month=month(all_dates)) %>% 
  dplyr::mutate(n=1:n())

temp_nc_pth<-"~corpenicus_Data/monthly_data/temp/"
temp_nc_agg_pth<-"~/corpenicus_Data/monthly_data/temp_aggregates/"
merged_nc_pth<-"~/corpenicus_Data/monthly_data/combined/"


year_month<-data_dates %>% 
  dplyr::group_by(year,month) %>% 
  summarise(.groups="drop",beg=min(n),end=max(n),size=n()) %>% 
  dplyr::mutate(N=1:n())


p<-1

## get sizes


var_names<-"tg|tn|tx|pp|rr"
NN<-3

loop_Run<-function(NN){
  
  temp_nc_pth<-"~/corpenicus_Data/monthly_data/temp/"
  temp_nc_agg_pth<-"~/corpenicus_Data/monthly_data/temp_aggregates/"
  merged_nc_pth<-"~/corpenicus_Data/monthly_data/combined/"
  clim_path<-"~/corpenicus_Data/Climate_data"
  
  var_nc<-str_extract(inp_fls$ncdf_file[NN],var_names)
  if(var_nc=="rr"){
    fun_sum<-"ttl"
  }else{
    fun_sum<-"avg"
  }
  
  get_slab<-function(p){
    #op<-nc_open(inp_fls$path[p])
    year_month[p,]
    nc_fname<-paste0(temp_nc_agg_pth,str_pad(p,width =3,side='left',pad=0),'.nc')
    #tm_c<-ncvar_get(op,"tmn")
    beg_end<-paste(c("time",year_month[p,]$beg,year_month[p,]$end),collapse =',')
    
    slab_str<-paste("time ncra -F  -d ",beg_end,"-p",clim_path," ",inp_fls$ncdf_file[NN],nc_fname," -O  --no_tmp_fl -4 -L 1 --open_ram -y ",fun_sum)
    system(slab_str)
  }
  
  
  ## run all and see
  tim_in<-system.time({
    foreach(a=1:nrow(year_month),.packages =c("stringr","dplyr","ncdf4"),
            .export =c("year_month","inp_fls","temp_nc_agg_pth"))%dopar% get_slab(a)
    
  })
  tim_in[3]/60
  tot_combine<-nrow(year_month)
  #tot_combine<-20
  comb_name<-inp_fls$monthly_fname[NN]
  merge_str<-paste0('time ncrcat  -p ',temp_nc_agg_pth," -n ",tot_combine,",3,1 '001.nc' ",comb_name,' -O --no_tmp_fl -4 -L 1')
  
  system(merge_str)
  #delete file from temp folder 
  unlink(list.files(temp_nc_agg_pth,full.names =T))
  
}

time_all<-system.time({
  foreach(a=1:nrow(inp_fls)) %do% loop_Run(a)
  
})
time_all[3]/60
##combine_files