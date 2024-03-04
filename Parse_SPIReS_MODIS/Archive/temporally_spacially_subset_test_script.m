file_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2019.h5";
save_file = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/test_junk/saving_h5_test.h5";

p1 = [35.47566651590996, -120.08250724861969];
p2 = [39.76384335615518, -117.75903692027082];
lat_long = [p1, p2];
days = [1, 44];

metavariable = 'swe';

proj = build_Projection(file_path);


temporally_spacially_subset(file_path, save_file, lat_long, days, metavariable)