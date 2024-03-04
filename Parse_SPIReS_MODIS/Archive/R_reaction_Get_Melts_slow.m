% This will take the formatting information given from
% "Create_DF_with_melt.R" and create a new CSV adding the appropriate daily
% melt values.

% This version uses the "log" file created.  I suspect that using the
% actual meta data etc that the script puts out could run faster however it
% is slightly harder to code and would require some thought.  So for now we
% will just run this how it is!

%Add the path to any functions needed (specficially CM melt functios)
addpath('/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/snow_melt_hypothesis');
addpath('/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/snow_melt_hypothesis/functions');


%Lets 

%Get correct log path
Log_savePath = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Ribbitr Data/debug/Melt_Calcs_Log/";
Logs_Hack_Path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Ribbitr Data/debug/log_directory/logs_hack.csv";
reactor_save_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Ribbitr Data/debug/info_for_reactor_directory/melt_calculator.csv";


% Currently it will always just sum the total regions melt amount. % Not
% sure if it should only do that or something else.
site_melt_calculation = "sum";




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
columnNames = [site_table_master.Properties.VariableNames, ["Site_Melt_Calculation", "Date", "Sum Melt", "Mean Melt", "Median Melt", "Max Melt", "Min Melt", "Standard Deviation Melt", "Variance Melt"]];
%var_types = [varfun(@class, columnNames, 'OutputFormat', 'cell'), ["datetime", 'double']];

nan_string = "Nan";
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
for h5_path_iter = 1:length(uniqueH5paths)
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
        h5_file = ""; %h5read(h5_path, varloc);
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
            
        
        
            melt_table = table(small_dates, sum_region_melt', mean_region_melt', median_region_melt', max_region_melt', min_region_melt', std_region_melt', variance_region_melt', ...
                'VariableNames', ["Date", "Sum Melt", "Mean Melt", "Median Melt", "Max Melt", "Min Melt", "Standard Deviation Melt", "Variance Melt"]);
           
            row_table = site_table(row, :);
            row_table.Site_Melt_Calculation = site_melt_calculation;
            replicated_row_table = repelem(row_table, size(melt_table, 1), 1);
            mergedTable = [replicated_row_table, melt_table];
        
            %Add mergedTable to the ALl_Melt_table now!
            overwrite_start_index = find(strcmp(all_melt_table.("Sum Melt"), nan_string), 1);
            overwrite_end_index = overwrite_start_index + size(mergedTable, 1) - 1;
            
            all_melt_table(overwrite_start_index:overwrite_end_index, :) = mergedTable;
            
            time_for_loop = datetime('now') - loop_start_time;
            
            progress = progress + 1;
            percentComplete = progress / numRows * 100;
            progressBar = repmat('.', 1, floor(percentComplete));
            fprintf('Progress: [%s] %.2f%%\r', progressBar, percentComplete);
            disp("Estimated Time Remaining: " + string((numrows - progress) * time_for_loop))
            disp("Loop Time " + string(time_for_loop))
            disp("")
            disp("")
        
            clearvars -except all_melt_table num_rows nan_string columnNames logs_path logs_hack_tbl Logs_Hack_Path site_melt_calculation region_size number_ays year_start_day h5_file varloc site_table melt_type row meta_variables site_table_h5_split h5_path h5_path_iter progress
        end
        clearvars -except all_melt_table num_rows nan_string columnNames logs_path logs_hack_tbl Logs_Hack_Path site_melt_calculation region_size number_ays year_start_day varloc site_table melt_type row meta_variables site_table_h5_split h5_path h5_path_iter progress
    end
end

melt_id = all_melt_table.melt_run_id(1);

disp("Write to csvs")
% write to the log
log_save_path = Log_savePath + melt_id + ".csv";
writetable(all_melt_table,log_save_path,'Delimiter',',')  
zip(log_save_path,log_save_path)

%write to the repeate R save location
writetable(all_melt_table,reactor_save_path,'Delimiter',',')
zip(reactor_save_path,reactor_save_path)

disp("Complete!")





%This is not the fastest way to do this.... It would actually be much
%faster to get the WHOLE area first.   Then grab the small regions from the
%matrix.... But I am  lazy so ill "do that later
function small_grid = get_region_melt(ROI_POINT, region_size, start_day, number_days, metavariable, file_path, h5File)
    %%%%%   Don't mess with the code below
    [p1, p2] = make_square_region(ROI_POINT(1), ROI_POINT(2), region_size);

    [small_grid, ~] = getmelt_specifc_region( ...
                                    file_path, metavariable, p1, ...
                                    p2, start_day, number_days);

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
