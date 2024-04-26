# ------------------ Lets find the Melt out date! ------------------------------#
#Default Values.  (WARNING THIS CHANGES ALL DEFAULT VALUES IN THIS FUNCTION)
default_smelt = 5 #The value that Melt must be less than to consider for melt out
default_snot_melt = 10 #The Value that Melt must be greater than to consider not melt
default_prev_days = 15 # The number of days previous to the date that must be
# greater than Snot_Melt
default_all_or_any = "any"  #can also be set to "any" or "all"
default_area_value_method = "Sum.Melt"




# This function tests if a specific date matches the "melt_out_date" criteria
# True if Snow Melt at Date is less than Smelt_out threshold AND
#    Snow Melt of previous 50 days > Snot_melt threshold
# Else
#   False
#
#  IMPORTANT:  This function only works for SWE and SWEHybrid.  There would need
#     to be a different function if we were interested in using Melt to determin
#     melt out.  I would argue that using melt does not make sense since melt
#     is still happending nonce melt out occurs.  And that melt out is a
#     somehwat arbitrarie value associated with a pond "opening" and NOT with 
#     all snow being gone.
#
#
# Input
# date = A specific Date
# site_melt_data = Df of a SINGLE site melt
#     Each of constants associated with these methods must be thought through 
#       carefully to tune our melt date appropriately.
#
# (OPTIONAL)  Below
# master_df = dataframe from wich the .data pronoun can compair to recreate dataframe (see make_dataframe_from_data)
# all_or_any_previous = String either "all" or "any"   Deliniates if all dates within previous days or any dates within previous days must be greater than the Snot_melt value
# area_value_method = String (Sum.Melt, Median.Melt, Mean.Melt, Max.Melt, Min.Melt) The method with wich the areas melt value is averaged/summed etc
# Smelt_out, Snot_Melt, previous_days = Numerical  Descriptions above in default params
#
#
# Return
#   Boolean = TRUE or FALSE if the target date matches the criteria for being a "melt out date"
is_melt_date_sum = function(target_date, site_melt_data, master_df = NULL, all_or_any_string = default_all_or_any, area_value_method = default_area_value_method, Smelt_out = default_smelt, Snot_melt = default_snot_melt, previous_days = default_prev_days){
  if (!is.data.frame(site_melt_data)){
    if(is.list(site_melt_data)){
      site_melt_data = make_dataframe_from_data(site_melt_data, master_df)
    }else{
      stop("site_melt_data is neither a DF nor a .data type.")
    }
  }
  
  check_if_not_swe_or_sweHybrid(site_melt_data) #Checks if this is the "melt" meltType 
  
  # Gets previous dates etc
  temp_previous_days_df = site_melt_data[site_melt_data$Date >= (target_date - previous_days), ]
  previous_days_df = temp_previous_days_df[temp_previous_days_df$Date < target_date, ]
  
  
  if (nrow(previous_days_df) < (previous_days-1)){
    warning("For the date below the 'previous days' calculation resulted in fewer than the required number of days and the date was dropped.") 
    warning(target_date)
    warning(nrow(previous_days_df))
    print("CHECK THE WARNINGS")
    return(FALSE)
  }
  
  melt_value = site_melt_data[[area_value_method]][site_melt_data$Date == target_date]
  if (melt_value <= Smelt_out && all_or_any(previous_days_df, area_value_method, Snot_melt, all_or_any_string) && nrow(previous_days_df) > 0 ){
    print("Came out True")
    print(target_date)
    return(TRUE)
  }else{
    return(FALSE)
  }
}

#INTERNAL USE ONLY
# Evals the all or any condition and perform evaulation on the df
# input
#   previous_days_df = df of all previous days
#   area_valuy_method = string representing the area_value_method
#   all_or_any_string = string delineating any or all
all_or_any = function(previous_days_df, area_value_method, Snot_melt, all_or_any_string){
  if (all_or_any_string == "all"){
    return(all(previous_days_df[[area_value_method]] >= Snot_melt))
  }else if (all_or_any_string == "any"){
    return(any(previous_days_df[[area_value_method]] >= Snot_melt))
  }else{
    stop("any or all variable is not equalt to 'any' or 'all")
  }
}


# This is for PIPE operations!   Custom function designed to work with single
# Variables must be Verctorized to work with pip operan
vectorized_is_melt_date_sum <- Vectorize(is_melt_date_sum, "target_date", "site_melt_data")





#makes a dataframe from the .data pronoun.   This is highly annoying but required
# There is some assumption that R will not jumble the lists when we pull them from
# the .data pronoun.   This assumption has not been completely confirmed and is hard to check
# This is done by creating a small dataframe from the .data function and then merging it
# with the larger dataframe and retaining only those rows that match!   It should end up
# being the same length.  If not then something has gone horribly wrong and will throw
# an error
# Input
#   the.data = Pronoun the .data pronoun.   cannot use .data directly without throwing error
#    master_df = large dataframe give that contains all of the .data
make_dataframe_from_data = function(the.data, master_df){
  if (is.null(master_df)){stop("the master DF is null")}
  date_list = the.data$Date
  site_id_list = the.data$site_id
  melt_list = the.data$melt_types
  the.data_DF = data.frame(Date = date_list, site_id = site_id_list, melt_types = melt_list)
  reduced_df = merge(master_df, the.data_DF, by = c("Date", "site_id", "melt_types"))
  if (nrow(reduced_df) != length(date_list)){
    stop("Something went wrong recreating the .data dataframe the lengths do not match")
  }
  return(reduced_df)
}



#  This checks if the user passed a DF that has meltType that is Melt
# Rather has a melt type that is NOT swe or sweHybrid.   These methods only work
# with SWE melt types
check_if_not_swe_or_sweHybrid = function(snow_df){
  if (!is.data.frame(snow_df)){ stop("The dataframe given is not a dataframe")}
  
  melt_type = unique(snow_df$melt_types)
  melt_type = melt_type[melt_type != "swe" && melt_type != "sweHybrid"]
  if (length(melt_type) > 0){
    print(snow_df)
    stop("Dataframe has unkown melt type when only swe and sweHybrid are allowed")
  }
}

