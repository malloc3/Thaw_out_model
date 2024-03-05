% This will take the formatting information given from
% "Create_DF_with_melt.R" and create a new CSV adding the appropriate daily
% melt values.

% This version uses the "log" file created.  I suspect that using the
% actual meta data etc that the script puts out could run faster however it
% is slightly harder to code and would require some thought.  So for now we
% will just run this how it is!

%Debug variable.   Set to true to use DEBUG files and DEBUG logs
debug = false;


%Legts get our Local Directories file so we know where everything is
local_direct = readtable("/Users/canonmallory/Library/Mobile Documents/com~apple~CloudDocs/Documents/School/UCSB/Briggs Lab/Thaw_out_model/UPDATE_local_directories.csv", 'Delimiter',',');
ribbitr_log_directory = local_direct{2,2};
SPIRES_log_directory = local_direct{3,2};

if debug
    Logs_Hack_Path = ribbitr_log_directory + "/debug/reactor_info_directory/logs_hack.csv";
    reactor_save_path = ribbitr_log_directory + "/debug/reactor_info_directory";
    melt_log_save_path = SPIRES_log_directory + "/debug/";
else
    Logs_Hack_Path = ribbitr_log_directory + "/reactor_info_directory/logs_hack.csv";
    reactor_save_path = ribbitr_log_directory + "/reactor_info_directory";
    melt_log_save_path = SPIRES_log_directory + "/logs/";
end

%Add the path to any functions needed (specficially CM melt functios)
addpath(pwd + "/Functions");


%Constants
%Dates.  Should be improved but for now 1 is like late summer early fall
year_start_day = 1;
number_days = 365;

region_size = 500; %meters

logs_hack_tbl = readtable(Logs_Hack_Path);
logs_path = logs_hack_tbl.Logs_path{1,1};

%Get the log table with all the sites and dates of interest
site_table_master = readtable(logs_path);

%PreAllocate the table for Matlab
columnName_arry =  ["Date", "Sum Melt", "Mean Melt", "Median Melt", "Max Melt", "Min Melt", "Standard Deviation Melt", "Variance Melt", "Region_Size_m", "Region_Shape"];
columnNames = [site_table_master.Properties.VariableNames,columnName_arry];
%var_types = [varfun(@class, columnNames, 'OutputFormat', 'cell'), ["datetime", 'double']];

nan_string = "Nan";
num_loops = size(site_table_master, 1);
numRows = size(site_table_master, 1) * number_days;
all_melt_table = array2table(repmat(nan_string, numRows, length(columnNames)), 'VariableNames', columnNames);

% Determine Unique File Paths
uniqueH5paths = unique(site_table_master.FilePaths);


%Iterate through table and fetch that year of data for that location
%  The whole row thing does somethign weird with the table.  So must first
%  fetch the table value and then get the character.  Annoying... but it 
%  doesn't do it with thelat, lon and idk why... but such is life.
%
%Modified:   Finds unique filepaths and fetches that file once for later
% use.  This saves time in the loop by pulling from the file ONCE per file
progress = 0; %For progress bar
num_h5_files_processed = 1;
number_of_h5_files_to_process = length(uniqueH5paths);
for h5_path_iter = 1:number_of_h5_files_to_process
    h5_path = string(uniqueH5paths(h5_path_iter));
    site_table_h5_split = site_table_master(strcmp(site_table_master.FilePaths, h5_path), :);
    %Determin Unique Melt types requested
    meta_variables = unique(site_table_h5_split.melt_types);

    for melt_type_iter = 1:length(meta_variables)
        melt_type = meta_variables(melt_type_iter);

        site_table = site_table_h5_split(strcmp(site_table_h5_split.melt_types, melt_type), :);
        varloc = "/Grid/" + melt_type;
        
        disp("==============================")
        disp("Reading File. Large Files may take a while")
        disp(h5_path)
        h5_file = h5read(h5_path, varloc);
        disp("File Read complete")
        disp("==============================")

        num_rows = size(site_table, 1);
        for row = 1:num_rows
            %Progress Bar information
            loop_start_time = datetime('now');
        
            h5_file_path_tbl = site_table.FilePaths(row);
            h5_file_path = h5_file_path_tbl{1,1};
        
            lat= site_table.lat(row);
            lon = site_table.lon(row);
        
            metavariable_tbl = site_table.melt_types(row);
            metavariable = metavariable_tbl{1,1};
        
            run_id_tbl = site_table.melt_run_id(row);
            run_id = run_id_tbl{1,1};
        
            small_dates = h5readatt(h5_file_path, '/', 'MATLABdates');
            small_dates = small_dates(year_start_day:(year_start_day + number_days - 1));
        
            ROI_POINT = [lat, lon];
        
            melt_grid = get_region_melt(ROI_POINT, region_size, year_start_day, number_days, metavariable, h5_file_path, h5_file);
            melt_grid = double(melt_grid);
        
            sum_region_melt = reshape(sum(melt_grid, [1 2]), 1, []);
            mean_region_melt = reshape(mean(melt_grid, [1 2]), 1, []);
            median_region_melt = reshape(median(melt_grid, [1 2]), 1, []);
            max_region_melt = reshape(max(melt_grid, [], [1 2]), 1, []);
            min_region_melt = reshape(min(melt_grid, [], [1 2]), 1, []);
            std_region_melt = reshape(std(melt_grid, 1, [1 2]), 1, []);
            variance_region_melt = reshape(var(melt_grid, 1, [1 2]), 1, []);
            region_size_arry = repelem(region_size, length(melt_grid)); %Makes an array of the same length as the others containing the region size
            region_shape_arry = repelem("square", length(melt_grid)); %Makes an array of the same length as the others containing the region size
            
            
            melt_table = table(small_dates, sum_region_melt', mean_region_melt', median_region_melt', max_region_melt', min_region_melt', std_region_melt', variance_region_melt', region_size_arry', region_shape_arry', ...
                'VariableNames', columnName_arry);
           
            row_table = site_table(row, :);
            replicated_row_table = repelem(row_table, size(melt_table, 1), 1);
            mergedTable = [replicated_row_table, melt_table];
        
            %Add mergedTable to the ALl_Melt_table now!
            overwrite_start_index = find(strcmp(all_melt_table.("Sum Melt"), nan_string), 1);
            overwrite_end_index = overwrite_start_index + size(mergedTable, 1) - 1;
            
            all_melt_table(overwrite_start_index:overwrite_end_index, :) = mergedTable;

            time_for_loop = datetime('now') - loop_start_time;
            progress = progress + 1;
            percentComplete = progress / num_loops * 100;
            progressBar = repmat('.', 1, floor(percentComplete));
            fprintf('Progress: [%s] %.2f%%\r', progressBar, percentComplete);
            disp("Estimated Time Remaining: " + string((num_loops - progress) * time_for_loop))
            disp("Loop Time " + string(time_for_loop))
            disp("Number of h5 Files Processed:" + string(num_h5_files_processed) + "/" + string(number_of_h5_files_to_process))
            disp("===============================================================")
        end
        clearvars -except number_of_h5_files_to_process columnName_arry reactor_save_path melt_log_save_path num_h5_files_processed num_loops all_melt_table num_rows nan_string columnNames logs_path logs_hack_tbl Logs_Hack_Path region_size number_ays year_start_day varloc site_table melt_type row meta_variables site_table_h5_split h5_path h5_path_iter progress number_days numRows uniqueH5paths site_table_master 
    end
    num_h5_files_processed = num_h5_files_processed + 1;
end

melt_id = all_melt_table.melt_run_id(1);

disp("Write to csvs")
% write to the log
log_save_path = melt_log_save_path + melt_id + ".csv";
writetable(all_melt_table,log_save_path,'Delimiter',',')  

%write to the repeate R save location
writetable(all_melt_table,reactor_save_path + "/melt_calculations.csv" ,'Delimiter',',')

disp("Complete!")





%This is not the fastest way to do this.... It would actually be much
%faster to get the WHOLE area first.   Then grab the small regions from the
%matrix.... But I am  lazy so ill "do that later
function small_grid = get_region_melt(ROI_POINT, region_size, start_day, number_days, metavariable, file_path, h5File)
    %%%%%   Don't mess with the code below
    [p1, p2] = make_square_region(ROI_POINT(1), ROI_POINT(2), region_size);

    [small_grid, ~] = getmelt_specifc_region_with_file( ...
                                    file_path, metavariable, p1, ...
                                    p2, start_day, number_days, h5File);

    %region_melt = reshape(sum(small_grid, [1 2]), 1, []);
end



function [p1, p2] = make_square_region(latIn, lonIn, region_size)
    distance = region_size/2;
    dist = sqrt(distance^2+distance^2); 
    distUnits = 'm';
    %Convert input distance to earth degrees (Lat, Lon are typicaly given in degrees)
    arclen = rad2deg(dist/earthRadius(distUnits)); 
    [p1_lat, p1_long] = reckon(latIn, lonIn,arclen,225);
    [p2_lat, p2_long] = reckon(latIn, lonIn,arclen,45);
    p1 = [p1_lat, p1_long];
    p2 = [p2_lat, p2_long];
end
