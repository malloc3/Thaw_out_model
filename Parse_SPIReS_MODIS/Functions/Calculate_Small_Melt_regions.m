%Set file path for the data
file_paths = ["/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2019.h5", "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2020.h5", "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2021.h5"];


CSV_file_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /Points_OF_Interest/Book2.csv";

% The melt data desired (options 'sweHybrid' 'melt' 'swe')

%ROI_POINT = [37.07095335, -118.5436752];

%Dates.  Should be improved but for now 1 is like late summer early fall

start_day = 1;
number_days = 364;
regions_of_interest =  readtable(CSV_file_path);
region_size = 500; %meters
metavariable = 'sweHybrid';


ROI_melts = [];
saftey = [];
dates = [];
for idx_1 = 1:3
    file_path = file_paths(idx_1);
    small_dates = h5readatt(file_path, '/', 'MATLABdates');
    dates = cat(1,dates,small_dates);
    table_size = size(regions_of_interest);
    disp('year:')
    disp(idx_1)
    disp('location:')
    ROI_melts_temporary = [];
    for idx = 1:table_size(1)
        disp(idx)
        ROI_POINT = [regions_of_interest.Lat(idx), regions_of_interest.Lon(idx)];
        ROI_melts_temporary = [ROI_melts_temporary; get_region_melt(ROI_POINT, region_size, start_day, number_days, metavariable, file_path)];
    end
    saftey = [saftey; ROI_melts_temporary];
    ROI_melts = cat(2,ROI_melts, ROI_melts_temporary(1:25, :));
end

%{
%ROI_fixed = [];
%ROI_fixed = cat(2,ROI_fixed, ROI_melts(1:25, :));
for idx_1 = 1:3
    file_path = file_paths(idx_1);
    small_dates = h5readatt(file_path, '/', 'MATLABdates');
    dates = cat(1,dates,small_dates);
end
%}


%dates = h5readatt(file_path, '/', 'MATLABdates');
%plot(datenum(dates(start_day:(start_day+number_days - 1))), ROI_melts, '-o')
%figure
%plot(datenum(dates(start_day:(start_day+number_days - 1))), mean(ROI_melts(1:7,:)), '-o')
%title('Decreasing')
%figure
%plot(datenum(dates(start_day:(start_day+number_days - 1))), mean(ROI_melts(9:16,:)), '-o')
%title('Naive')
%figure
%plot(datenum(dates(start_day:(start_day+number_days - 1))), mean(ROI_melts(16:end,:)), '-o')
%title('Persistant')
 

real_dates = datetime(dates,'ConvertFrom','datenum');
%figure
%hold on
%    plot(real_dates(1:1092), mean(ROI_Saftey(1:7,:)), '-o')
%    plot(real_dates(1:1092), mean(ROI_Saftey(9:16,:)), '-o')
%    plot(real_dates(1:1092), mean(ROI_Saftey(16:end,:)), '-o')
%    legend('Decreasing','Naive', 'Persistent')
%hold off

figure
plot(real_dates(1:1092), ROI_melts(1:7,:), '-o')
title('Decreasing sweHybrid Melt 2019-2021')
ylabel('sweHybrid Melt mm') 

figure
plot(real_dates(1:1092), ROI_melts(9:16,:), '-o')
title('Naive sweHybrid Melt 2019-2021')
ylabel('sweHybrid Melt mm') 

figure
plot(real_dates(1:1092), ROI_melts(17:end,:), '-o')
title('Persistent sweHybrid Melt 2019-2021')
ylabel('sweHybrid Melt mm') 

figure
dates_for_fill = real_dates(1:1091)';
x2 = [dates_for_fill, fliplr(dates_for_fill)];

[s_d, m_d] = std(rolling_average(1:7,:), 1, 1);
[s_n, m_n] = std(rolling_average(9:16,:), 1, 1);
[s_p, m_p] = std(rolling_average(17:end,:), 1, 1);

curve1_d = m_d + s_d;
curve2_d = m_d - s_d;
inBetween_d = [curve1_d, fliplr(curve2_d)];

curve1_n = m_n + s_n;
curve2_n = m_n - s_n;
inBetween_n = [curve1_n, fliplr(curve2_n)];

curve1_p = m_p + s_p;
curve2_p = m_p - s_p;
inBetween_p = [curve1_p, fliplr(curve2_p)];

hold on
    hp_d = fill(x2, inBetween_d, 'r');
    set(hp_d, 'facecolor', 'r', 'edgecolor', 'r');

    %hp_n = fill(x2, inBetween_n, 'b');
    %set(hp_n, 'facecolor', 'b', 'edgecolor', 'b');


    hp_p = fill(x2, inBetween_p, 'k');
    set(hp_p, 'facecolor', 'k', 'edgecolor', 'k');
    
    alpha(hp_p, .2)
    alpha(hp_d, .2)
    %alpha(hp_n, .2)

    hl_d = plot(real_dates(1:1091), m_d, '-o');
    %hl_n = plot(real_dates(1:1092), m_n, '-o');
    hl_p = plot(real_dates(1:1091), m_p, '-o');
    set(hl_p, 'color', 'k', 'marker', 'x');
    %set(hl_n, 'color', 'b', 'marker', 'x');
    set(hl_d, 'color', 'r', 'marker', 'x');
    legend('Decreasing', 'Persistent')

    title('sweHybrid Melt 2019-2021')
    ylabel('sweHybrid Melt mm') 
hold off

%This is not the fastest way to do this.... It would actually be much
%faster to get the WHOLE area first.   Then grab the small regions from the
%matrix.... But I am  lazy so ill "do that later
function region_melt = get_region_melt(ROI_POINT, region_size, start_day, number_days, metavariable, file_path)
    %%%%%   Don't mess with the code below
    [p1, p2] = make_square_region(ROI_POINT(1), ROI_POINT(2), region_size);

    [small_grid, small_RefRaster] = getmelt_specifc_region( ...
                                    file_path, metavariable, p1, ...
                                    p2, start_day, number_days);

    region_melt = reshape(sum(small_grid, [1 2]), 1, []);
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
