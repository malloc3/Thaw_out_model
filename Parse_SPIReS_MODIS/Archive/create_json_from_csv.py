# This script will take in a CSV listing all ponds of interest and associated information then convert that to the
# runtime .json file

import json
import csv
import os.path

csv_file_path = ("/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) "
                 "/Points_OF_Interest/Initial_Ponds.csv")
j_file_path = ("/Users/Cannon/Documents/School/UCSB/Briggs "
               "Lab/Thaw_Rate_Hypothesis/snow_melt_hypothesis/run_config_example copy.json")


# Takes a CSV file following the "pond_csv_example.csv" format and creates/appends to a JSON CONFIG file for use in
# Thaw rate scrips
#
# csv_file_path = string the file path to the properly formatted CSV file.
# json_file_path = String or None. If None will create new file. If exists will either append to pond section of file
def creat_pond_json_from_csv(csv_file_path, json_file_path=None):
    if not json_file_path:
        json_file_path = '/Users/Cannon/Desktop/default_pond_file.json'

    # list of dictionaries of each pond
    ponds_dict_array = []

    # open CSV file and create dictionaries
    with open(csv_file_path, mode='r+') as pond_csv:
        pond_reader = csv.reader(pond_csv)
        keys = next(pond_reader)
        for row in pond_reader:
            pond_dict = dict(zip(keys, row))
            ponds_dict_array.append(pond_dict)

    if os.path.isfile(json_file_path):  # OUTPUTFILE_EXISTS
        with open(json_file_path, "r") as output_file:
            data = json.loads(output_file.read())
            data['ponds'] += ponds_dict_array
            dups_removed_list = []
            [dups_removed_list.append(x) for x in data['ponds'] if x not in dups_removed_list]
            data['ponds'] = dups_removed_list
        with open(json_file_path, "w") as output_file:
            output_file.write(json.dumps(data, indent=4))
    else:  # if the output json file doesnt exist
        with open(json_file_path, "w+") as output_file:
            master_dict = {
                "header": "This file contains the required configuration settings to run the thaw scripts",
                "researcher name": "Cannon Mallory",
                "snow_melt_files": {
                    "file_type": "h5",
                    "file_paths": [
                        {
                            "year": "YYYY",
                            "path": "H5 FILE PATH"
                        }
                    ]
                },
                "ponds": ponds_dict_array
            }
            pond_json_obj = json.dumps(master_dict, indent=4)
            output_file.write(pond_json_obj)


creat_pond_json_from_csv(csv_file_path, j_file_path)
print('all done')
