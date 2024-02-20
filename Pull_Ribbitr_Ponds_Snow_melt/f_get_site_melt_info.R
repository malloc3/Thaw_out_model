#-----------------------------------------------------------------------------#
# ---- This function will take the lat long DF locations passed to it and ----#
# return a dataframe with with the daily melt, swe, swe_hybrid from the ------#
# specified dates. -----------------------------------------------------------#
#-----------------------------------------------------------------------------#

#--- This one is tricky since the best script for analyzing the snow melt ----#
#- data is done in MATLAB.  At some point I will make a reactor in matlab ----#
#- that will respond to R and run the whole script automatically.  But since -#
#- I don't have the time to do that right now it will require user input. ----#
#- I will do my best to make this simple and create a short user interface ---#
#- that can help make the process more simple. -------------------------------#
#
# This will
# 1. Create 3 csv (h5_file_paths, location_df, parameters) that will be saved in the same spot
#    every time for use by a reactor.
# 2. Will create a 3rd csv to be saved in a "LOG" location.  This is to record 
#    what was done on which dates.
# 3. It will activate SOME_REACTOR (ssh script??) that will activate the Matlab
#    script to run.   This script will run and create a CSV with required melt
#    information in a usable DATAFRAME keyed to the Site_ID and Date
#
#- Input ---------------------------------------------------------------------#
#- location_df = DataFrame [site_id, lat, lon]
#- h5_file_path_DF = DF of file paths [year, file_path]
#- reactor_directory = String the directory where the info needed by the reactor will be stored
#- log_directory = String the directory where the log information will be stored
#
#- Output --------------------------------------------------------------------#
#- CSV: 
#- TBD:melt_df = DataFrame [site_id, date, swe, melt, sweHybrid] #may come back to this...
get_daily_site_melt <- function(site_location_df, h5_file_path_DF, reactor_directory, melt_types, log_directory){
  run_id = create_uniqe_run_id()
  log_file_path = save_melt_logs(run_id, site_location_df, h5_file_path_DF, melt_types, log_directory)
  create_reactor_run_files(run_id, site_location_df, h5_file_path_DF, melt_type, reactor_directory)
  
  #Logs Hack.  Shameful hack which is faster for me to write but runs a bit slower than the way the script was intended
  create_logs_hack_files(log_file_path, reactor_directory)

  print("Activate Matlab script!!  Currently not automatic but maybe someday :(")
}



# Creates the files needed by the reactor to run the get_melt functions in matlab
# It will overwrite exisitng files and reuses the file names for simplicity.  All
# meta data will be saved in the log data file though so that info is  NOT lost.
#
# Input:
# run_id = String string of unique characters (it actually includes the date and time but that's not official)
# site_location_df = DF of site locations with [site_id, lat, lon]
# h5_file_path_DF = DF of file paths [year, file_path]
# reactor_directory = String the directory where reactor instruction files should be saved
#
# Output:
# CSV: h5_file_paths_csv a CSV with all the required h5_filepaths needed for this run
# CSV: site_location_csv a CSV with all the required site locations for this run
# CSV: params a csv with all the parameters needed to run the analysis
create_reactor_run_files <- function(run_id, site_location_df, h5_file_paths_DF, melt_type, reactor_directory){
  # Save the h5 files dataframe into the directory indicated with the proper filename
  h5_file_paths_csv_filepath = file.path(reactor_directory, h5_files_csv_file_name())
  write.csv(h5_file_paths_DF, file = h5_file_paths_csv_filepath, row.names = FALSE) # Save
  
  # Save the site_locations files dataframe into the directory indicated with the proper filename
  site_location_csv_filepath = file.path(reactor_directory, site_location_csv_file_name())
  write.csv(site_location_df, file = site_location_csv_filepath, row.names = FALSE) # Save
  
  # Grabs standard run parameters and updates with appropriate changes.
  # There is an opportunity here to create a ton of custimization per run.  Right now the software doesnt support this
  run_parameters = create_run_parameters(run_id, h5_file_paths_csv_filepath, site_location_csv_filepath, melt_types, reactor_directory) 
  params_filepath = file.path(reactor_directory, params_file_name())  #Creates file path for params
  write.csv(run_parameters, file = params_filepath, row.names = FALSE) # Save
}



# This function is a bit funky and is pretty flexible.   There exists a "stock run format" file that this will copy
# Edit and place in the reactor directory.   Theoretically this function can modify the "stock run format" per users
# desire, but all those functions are not yet implemented.   For now this only changes the "melt_Run_ID" metadata.
#
# Uses "Default_Melt_Run_Parameters.csv" located in the reactor_directory.  If it doesn't exist then it will create
#
#-Input
#-run_id = String the run ID
#-reactor_directory = String the directory where reactor instruction files should be saved
#
# Output:
# Creates CSV of "Melt_Run_Parameters"
create_run_parameters <- function(run_id, h5_file_paths_csv_filepath, site_location_csv_filepath, melt_type, reactor_directory){
  # Check if "Default_Melt_Run_Parameters.csv" exists
  default_params_path = file.path(reactor_directory, default_params_file_name())
  if (!file.exists(default_params_path)) {
    create_default_params_file(default_params_path, h5_file_paths_csv_filepath, site_location_csv_filepath)
  } #else the file exists and we can continue
  params <- read.table(default_params_path, header = TRUE, sep = ",")
  params[["run_id"]] = run_id
  params[["h5_files_csv_filepath"]] = h5_file_paths_csv_filepath
  params[["site_location_csv_filepath"]] = site_location_csv_filepath
  
  repeat_factor <- nrow(melt_type)
  params <- do.call(rbind, replicate(repeat_factor, params, simplify = FALSE))
  params$melt_types <- melt_type$melt_types[1:repeat_factor]
  
  return(params)
}








# This will get the full file path names of all the h5 files of interest
# and put them in a nice little DATAFRAME
#
# Input:
# target_dates = List (string) c(start_date, end_date)
#- h5_directory = String the directory where the h5 files are stored
get_h5_file_names_df = function(h5_directory, list_years){
  # Get a list of file paths
  file_paths <- list.files(h5_directory, full.names = TRUE)
  file_paths = file_paths[grep(".h5", file_paths)]
  
  # Initialize an empty data frame
  result_df <- data.frame(Year = character(), FilePaths = character(), stringsAsFactors = FALSE)
  
  # Iterate over substrings and file paths
  for (year in list_years) {
    matching_files <- file_paths[grep(year, file_paths)]
    temp_df <- data.frame(Year = year, FilePaths = matching_files, stringsAsFactors = FALSE)
    result_df <- rbind(result_df, temp_df)
  }
  return(result_df)
}


#- This could genuinly come from anywhere.  In this case I am just asking ---#
#- the user for dates -------------------------------------------------------#
#
#- Input --------------------------------------------------------------------#
#- NONE
#
#- Output -------------------------------------------------------------------#
#- target_dates = List (string) c()
ask_for_target_dates <- function(){
  # Ask the user for the start date
  start_date <- as.Date(showPrompt("Start Date", "Enter the start date (YYYY-MM-DD):", default = NULL))
  # Ask the user for the end date
  end_date <- as.Date(showPrompt("End Date", "Enter the end date (YYYY-MM-DD):", default = NULL))
  return(c(start_date, end_date))
}

#- No idea how this is going to work yet. Must ask about the ribbitr data ---#
#
#- Input --------------------------------------------------------------------#
#- sites_of_interest = DataFrame [site_id]
#
#- Output -------------------------------------------------------------------#
#- location_df = DataFrame [site_id, lat, lon]
get_site_lat_lon <- function(sites_of_interest){
  sites_of_interest$lat <- runif(1, 32.5, 42)   # Restricted to California
  sites_of_interest$lon <- runif(1, -124.4096, -114.1312) # Restricted to California
  return(data.frame(sites_of_interest))
}

#- Checks the directory that the user gave and ensures that there is an -----#
#- H5 file with a date in the name that covers the dates of interest --------#
#- Input --------------------------------------------------------------------#
#- directory = string (The directory to which all H5 melt files are located) #
#- target_dates = List c(year, ..., year) years are int
check_directory_for_valid_dates <- function(directory, list_years){
  if(directory == FALSE){
    print(directory)
    stop("Invalid directory given.  Please put a valid directory for the melt data")
  }
  
  # Get the file names we are interested in
  all_files = list.files(directory)
  h5_file_names = all_files[grep(".h5", all_files)]
  
  # See if the file names include the dates
  list_of_years_no_represented = c()
  for (year in list_years) {
    is_substring = any(grepl(year, h5_file_names))
    
    if (!is_substring){
      list_of_years_no_represented = c(list_of_years_no_represented, year)
    }
  }
  
  if (length(c(list_of_years_no_represented)) != 0 ){
    print(h5_file_names)
    print(list_of_years_no_represented)
    stop("The directory given does not contain all dates within the start and end dates given.")
  }
}


# Converts the given start and end date to a list of years that we want to analyze.
# Currently doesn't support targeting specific months or anything like that though.
#
# Input:
# start_end_dates = List c(start_date, end_date) in R date format YYYY-MM-DD
get_years_included_in_target_dates <- function(start_end_dates){
  # Get a list of years we are looking for
  start_year = as.integer(substr(start_end_dates[1], start = 1, stop = 4))
  end_year = as.integer(substr(start_end_dates[2], start = 1, stop = 4))
  list_years = seq(start_year, end_year, by = 1)
  return(list_years)
}




#This function creates a new unique run ID for logging information effectively
#
#Output:
# Run_ID = String string of characters that are unique for this specific run
create_uniqe_run_id = function(){
  # Get current date and time
  current_datetime <- Sys.time()
  
  # Format the date and time as a string
  formatted_datetime <- format(current_datetime, "%Y%m%d%H%M%S")
  
  # Generate a short string of random characters (e.g., 5 characters)
  random_characters <- paste(sample(c(letters, 0:9), 5, replace = TRUE), collapse = "") %>% 
                       paste0("_calc-melt")
  
  # Create the unique string
  unique_string <- paste0(formatted_datetime, "_", random_characters)
  
  # Print the unique string
  return(unique_string)
  
}



# This will combine the dataframes, give each row the run id and save it in a new
# file in the directory folder with the run_id as the name of the file.
#
# Input:
# run_id = String string of unique characters (it actually includes the date and time but that's not official)
# site_location_df = DF of site locations with [site_id, lat, lon]
# h5_file_path_DF = DF of file paths [year, file_path]
# log_directory = String the directory where the logs should be saved
#
# Output:
# file_path = String the File path for where the log was saved
# Saves a CSV in directory for log information
save_melt_logs = function(run_id, site_location_df, h5_file_path_DF, melt_types, log_directory){
  log_df = merge(site_location_df, h5_file_path_DF, by = NULL)
  log_df= merge(log_df, melt_types, by = NULL)
  log_df$melt_run_id = run_id
  log_df$melt_run_date_time = Sys.time()
  # Full file path using run_id as file name
  file_path <- file.path(log_directory, paste0(run_id, ".csv"))
  # Save the dataframe as a CSV file
  write.csv(log_df, file = file_path, row.names = FALSE)
  return(file_path)
}


# The Somewhat Shameful hack around the original intent of this reactor design
#  This puts the logs file path in an easy to find location that another script can
#  Use to look through the logs to learn what is required to do.    This is easier to
#  write than the original design but ultimately computationally slower.
#
# Input:
#  log_file_path = String the file path of the Logs that were just generated for this run
#  reactor_directory = The directory of the reactor information
#
# Output;
#   CSV containing the log_directory_File_path
create_logs_hack_files = function(log_file_path, reactor_directory){
  #this is a shameful hack around the original design of this script.  Its faster to write but slower to run
  logs_path_reactor_hack_df = data.frame(
    Logs_path = c(log_file_path)
  )
  logs_path_hack_standar_path = file.path(reactor_directory, "logs_hack.csv")
  write.csv(logs_path_reactor_hack_df, file = logs_path_hack_standar_path, row.names = FALSE) # Save
}

# These are the current default parameters.  It is subject to update as software gets more advanced.
#
# Input:
# reactor_directory = String the directory where reactor instruction files should be saved
#
# Output:
# CSV of default run parameters
create_default_params_file <- function(default_params_file_path, h5_file_paths_csv_filepath, site_location_csv_filepath){
  default_params = data.frame(
    run_id = '',
    h5_files_csv_filepath = h5_file_paths_csv_filepath,
    site_location_csv_filepath = site_location_csv_filepath,
    melt_types = "swe"
  )
  write.csv(default_params, file = default_params_file_path, row.names = FALSE)
}


#These are really just a global variables... But it was throwing an error as a global variable
# but sticking it into this function helped somehow...
default_params_file_name = function(){return("Default_Melt_Run_Parameters.csv")}
params_file_name = function(){return("Melt_Run_Parameters.csv")}
h5_files_csv_file_name = function(){return("h5_file_paths_csv.csv")}
site_location_csv_file_name = function(){return("site_location_csv.csv")}