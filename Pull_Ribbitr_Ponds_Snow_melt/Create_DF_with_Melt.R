library(RPostgres) # The database driver
library(DBI) # Functions needed to interact with the database
library(rstudioapi) # Package that asked for credentials
library(tcltk)
library(rstudioapi)

#------------- Add functions ---------------------------------------------------#
source("f_get_site_melt_info.R")
source("f_connect_to_database.R")
# ------------------------------------------------------------------------------#





#--------------------- USER CHANGE VARIABLES ------------------------#
debug = FALSE #Change to FALSE when performing actual analysis!

# This may need to be updated for your computer.  But this should find file in the master directory for this git project
#. Make sure to update the UPDATE_local_directories.csv file with the correct directories
csv_with_directory_info = read.csv(paste(getwd(), "/../", "UPDATE_local_directories.csv", sep = ""))

melt_types = c("swe", "sweHybrid", "melt") #This is all of them.  Probably best to just retrieve all these data...

#Currently only recognizes start and end years (not moths/days)
start_date = 01/01/2001
end_date = -1/01/2021
#------------ END USER CHANGEABLE VAIRABLES -------------------------#




#------------ Sets File Paths from UPDATE_local_directories.csv as needed -----#
secrets_file = csv_with_directory_info[1,2]
log_and_reactor_directory = csv_with_directory_info[2,2]
directory_of_h5_files = csv_with_directory_info[4,2] #Set to where all H5 Files are stored

# These can be changed by the user but if you follow the standard file organization
# then you should leave these alone
required_info_for_reactor_directory =  paste(log_and_reactor_directory, "/reactor_info_directory", sep = "")
log_directory = paste(log_and_reactor_directory, "/log_directory", sep = "")
#------------------------------------------------------------------------------#



# --------------------- Install Required Packages ----------------------------#
if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}
# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, DBI, RPostgres, dbplyr, kableExtra, tcltk)
# ------------------------------------------------------------------------------#






# ------------------------------------Debugging VARS----------------------------#
debug_start_date = "2003-11-19"
debug_end_date = "2006-11-19"
debug_h5_directory = directory_of_h5_files
debug_log_directory = paste(log_and_reactor_directory, "/debug/log_directory", sep = "")
debug_required_info_for_reactor_directory = paste(log_and_reactor_directory, "/debug/reactor_info_directory", sep = "")
debug_melt_types = c("swe", "sweHybrid", "melt")
# ------------------------------------------------------------------------------#






#---------------------------Checks for Debugging Key---------------------------#
if (debug == TRUE){
  star_end_dates =  c(debug_start_date, debug_end_date)
  directory_of_h5_files = debug_h5_directory
  required_info_for_reactor_directory = debug_required_info_for_reactor_directory
  log_directory = debug_log_directory
  melt_types = debug_melt_types
}else if (start_date == FALSE || end_date == FALSE){
  star_end_dates = ask_for_target_dates() 
}else { #if dates were specified above
  star_end_dates =  c(start_date, end_date)
}
# ------------------------------------------------------------------------------#





# --------------------- Establish Connection to Ribbitr Database --------------#
ribbitr_connection = connect_to_database(secrets_file)

# setting your search path
dbExecute(conn = ribbitr_connection,
          statement = "set search_path = 'survey_data'")
# ------------------------------------------------------------------------------#






# --------Fetch data from ribbitr database and combine into local DF ----------#
db_data <- tbl(ribbitr_connection, "location") %>%
  inner_join(tbl(ribbitr_connection, "region"), by = c("location_id")) %>%
  inner_join(tbl(ribbitr_connection, "site"), by = c("region_id")) %>% 
  inner_join(tbl(ribbitr_connection, "visit"), by = c("site_id")) %>%
  inner_join(tbl(ribbitr_connection, "survey"), by = c("visit_id")) %>%
  inner_join(tbl(ribbitr_connection, "capture"), by = c("survey_id")) %>%
  inner_join(tbl(ribbitr_connection, "qpcr_bd_results"), by = c("bd_swab_id")) %>% 
  filter(location %in% "usa")
# ------------------------------------------------------------------------------#




# --------------------- Reformatting DATA --------------------------------#
# Cleans up the data to only look at California sites
print("Reformatting Data (this may take some time)")
clean_data <- db_data %>%
  collect() %>% 
  filter(region == "california")

#Select the sites in California that are of interest.  For now its all sites
sites_of_interest = clean_data %>% 
  distinct(site_id)
print("Data Reformatted Successfully")
# ------------------------------------------------------------------------------#





# Converts melt_types to a dataframe for easy manipulation later
melt_types = as.data.frame(melt_types)

# get the lat long info
site_lat_lon_df = get_site_lat_lon(sites_of_interest)

list_years = get_years_included_in_target_dates(star_end_dates) #converts given start/end dates to years only

check_directory_for_valid_dates(directory_of_h5_files, list_years) # Checks that we have data for years given

h5_file_path_DF = get_h5_file_names_df(directory_of_h5_files, list_years) # Fetches the full file paths of the target years

daily_melt = get_daily_site_melt(site_lat_lon_df, h5_file_path_DF,
                                 required_info_for_reactor_directory,
                                 melt_types, log_directory) # Gets the snow melt data


print("Success!!")


