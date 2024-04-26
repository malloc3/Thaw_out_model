# This Script handles DB connections to the Ribbitr Database

# Connect to database runs the code required for connecting to the Ribbitr Database
# It will pull from a "secrets" folder which you will need to set the path here!
#
# Input:
#    file_secrets = String file path to your DB connection secrets CSV
#       Username secrets should be a csv following the format below
#         Description, Secret
#         username,  USERNAME
#         password,  PASSWORD
#         dbname,    DATABASE_NAME
#         host,      HOST URL
#         port,      PORT
# Output:
#   ribbitr_connection = Connection the connection to the ribbitr database.
connect_to_database <- function(file_secrets){
  secrets = read.csv(file_secrets) #fetches the secrets
  print(toString(secrets[3,2]))
  tryCatch({
    print("Connecting to Database...")
    ribbitr_connection <- dbConnect(drv = dbDriver("Postgres"),
                                    dbname = toString(secrets[3,2]),
                                    host = toString(secrets[4,2]),
                                    port = toString(secrets[5,2]),
                                    user = toString(secrets[1,2]),
                                    password = toString(secrets[2,2]))
    
    print("Database Connected!")
  },
  error=function(cond) {
    print("Unable to connect to Database.")
  })
  return(ribbitr_connection)
}