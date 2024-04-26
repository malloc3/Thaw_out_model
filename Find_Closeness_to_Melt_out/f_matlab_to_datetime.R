# Converts matlab dates to POSIXTL dates and vis-versa
# Matlab uses the number of days since 01/01/0000
# Most UNIX systems (including R) uses the number of SECONDS since 01/01/1970
#
# Both of these "zero" dates are arbitrary but one of them is more standard than
#    the other (classic Matlab).   To convert from MATLAB to POSIXtl we must
# 1) Subtract the number of days from 01/01/0000 to 01/01/1970 (719529)
# 2) Multiply by the number of seconds in a day 86400
#
# POSIXtl = (MATLABDATE - 719529)*86400

library(lubridate)



# Converts matlab to POSIXtl
#
# Input
#   matlab_dt = Numeric Matlab Dat Time number
# Return
#   secs_epoch = Numeric.   Number of seconds since EPOCH
matlab2POS = function(matlab_dt) {
  days = matlab_dt - 719529 	# 719529 = days from 1-1-0000 to 1-1-1970
  secs_epoch = days * 86400 # 86400 seconds in a day
  return(secs_epoch)
}


# Converst POSIXtl to matlab
#
# Input
#   secs_epoch = Numeric.  Seconds since EPOCH
# Return
#   matlab_dt = Numeric.  Matlab Date time Number
POS2matlab = function(secs_epoch){
  days = secs_epoch/86400 # 86400 seconds in a day
  matlab_dt = days + 719529 	# 719529 = days from 1-1-0000 to 1-1-1970
  return(matlab_dt)
}


# Converts matlab to usable DateTime 
# Input
#   matlab_dt = Numeric Matlab Dat Time number
# Return
#   date_time = Date Time.   R datetime object (mostly just a string)
matlab2datetime = function(matlab_dt){
  epoch = matlab2POS(matlab_dt)
  date_time = as_datetime(epoch)
  return(date_time)
}



# Converts matlab to usable Date (no time)
# Input
#   matlab_dt = Numeric Matlab Dat Time number
# Return
#   date_time = Date Time.   R datetime object (mostly just a string)
matlab2date = function(matlab_dt){
  epoch = matlab2POS(matlab_dt)
  
  # Have to run both functions since as_date alone returns nonsensical result
  date = as_date(as_datetime(epoch))
  return(date)
}

