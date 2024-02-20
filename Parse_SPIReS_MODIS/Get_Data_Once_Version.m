%Set file path for the data
file_paths = ["/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2019.h5", "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2020.h5", "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2021.h5"];


CSV_file_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /Points_OF_Interest/Book2.csv";
%Video_save location
video_file_path = '/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /Videos';
video_name = 'test_video'; %Will automatically add file type and meta variable used

%This sets the bounds for the small grid that will be nabbed (smaller than
%all the data for sake of reducing memory load
p1 = [35.47566651590996, -120.08250724861969];
p2 = [39.76384335615518, -117.75903692027082];

target_color = [1; 1; 0];%[0; 94/255; 184/255];
no_data_color = [55/255; 79/255; 47/255];

max_melt_constant = 1000;

%ROI_POINT = [37.07095335, -118.5436752];

%Dates.  Should be improved but for now 1 is like late summer early fall
start_day = 1;
number_days = 2;
regions_of_interest =  readtable(CSV_file_path);
region_size = 5000; %meters
metavariable = 'swe'; % The melt data desired (options 'sweHybrid' 'melt' 'swe')


%%%%%   Don't mess with the code below
video_path_and_name = [video_file_path '/' video_name '_' metavariable];

ROI_melts = [];
saftey = [];
dates = [];
video_images = [];
for idx_1 = 1:3
    file_path = file_paths(idx_1);
    small_dates = h5readatt(file_path, '/', 'MATLABdates');
    dates = cat(1,dates,small_dates);
    table_size = size(regions_of_interest);
    disp('year:')
    disp(idx_1)
    ROI_melts_temporary = [];
    proj = build_Projection(file_path);
    [small_grid, small_RefRaster] = getmelt_specifc_region( ...
                                    file_path, metavariable, p1, ...
                                    p2, start_day, number_days);
    small_image_size = small_RefRaster.RasterSize;
    
    multi_target_rgb_shape_mask = zeros(small_image_size(1), small_image_size(2), 3);
    disp('location:')
    for idx = 1:table_size(1)
        disp(idx)
        ROI_POINT = [regions_of_interest.Lat(idx), regions_of_interest.Lon(idx)];
        ROI_melts_temporary = [ROI_melts_temporary; get_region_melt(ROI_POINT, region_size, small_grid, small_RefRaster, proj)];
    
        [row_b, row_s, col_b, col_s] = make_square_region(ROI_POINT(1), ROI_POINT(2), region_size, small_RefRaster, proj);
        tbm = make_binary_shape_mask(row_s, row_b, col_s, col_b, small_image_size);
        target_color_mask = make_rgb_color_mask(target_color, small_image_size);

        target_rgb_shape_mask = make_RGB_Shape_mask(tbm, target_color_mask);
        multi_target_rgb_shape_mask = multi_target_rgb_shape_mask + target_rgb_shape_mask;
    end
    saftey = [saftey; ROI_melts_temporary];
    ROI_melts = cat(2,ROI_melts, ROI_melts_temporary(1:25, :));
    disp('start video creation')
    day_grid = small_grid(:,:,1);

    big_nums_binary = day_grid == 65535; % Records where all NAN values are %[row, col]
    melt_binary_grid = not(big_nums_binary); % Records where all non NAN values are %[row, col]
    no_data_color_mask = make_rgb_color_mask(no_data_color, small_image_size); %[row, col, rgb]
    all_days_color_mask = repmat(no_data_color_mask, 1,1,1, number_days);%[row,col,rgb,dates]
    disp('vid checkpoint 1')
    %Normalizes the display to the max melt constant (darker is hgiher
    %melt)  
    normalized_small_grid = double(1- small_grid/max_melt_constant);
    all_days_melt_binary_grid = repmat(melt_binary_grid, 1, 1, number_days); %[row, col, dates]
    reduced_normalized_small_grid = normalized_small_grid.*all_days_melt_binary_grid; %[row, col, dates]

    if max(reduced_normalized_small_grid, [], "all") > 1 || max(reduced_normalized_small_grid, [], "all") < 0
        error('Max_melt_constant is too small for dataset')
    end

    img_with_rgb_in_wrong_spot = repmat(reduced_normalized_small_grid, 1, 1, 1, 3); %[row, col, dates, rgb]
    img_all_day = permute(img_with_rgb_in_wrong_spot, [1,2,4,3]); %moves so it goes [row,col,rgb,dates]
    disp('vid checkpoint 2')
    %Sets the 65535 values no data color
    big_nums_rgb_shape_mask2 = make_RGB_Shape_mask(big_nums_binary, no_data_color_mask);%[row,col, rgb]
    big_nums_rgb_shape_mask = big_nums_rgb_shape_mask2.*big_nums_binary;%[row,col, rgb]
    all_days_big_nums_rgb_shape_mask = repmat(big_nums_rgb_shape_mask, 1,1,1, number_days); %[row,col,rgb,dates]
    
    %Creates a master mask
    %data_mask_master = big_nums_rgb_shape_mask + img;  For single image parts removed
    all_day_data_mask_master = all_days_big_nums_rgb_shape_mask + img_all_day; %[row,col,rgb,dates]
    disp('vid checkpoint 3')
    %Checks that the raster grids don't overlap (if they do then data may
    %be lost.   The only one we will tolerate overlapping is the Target
    %Region overlay
    sum_bi = unique(melt_binary_grid + big_nums_binary); %+ zeros_binary_mask
    if numel(sum_bi) ~= 1
        if max(sum_bi) > 1
            error('One or more of the masks are overlapping which is not good')
        elseif min(sum_bi) == 0
            warning('You have holes in you raster image it may display but it may be inaccurate')
        else
            warning('Your binary raster mask has values not equal to 1.  This may be okay but it may not be :/')
        end
    elseif sum_bi(1) ~= 1
        error('Your rasters are all out of scope or something.  There is no image')
    end

    
    %Adds a location dot (note that x and y must be swapped)

    %target_mask = [round(hetch_Y-1):(round(hetch_Y)+1), round(hetch_X):(round(hetch_X)+2)]
    %img(round(hetch_Y-1):(round(hetch_Y)+1), round(hetch_X):(round(hetch_X)+2), :) = [:, :, [1, 0, 0]];
    %img(round(hetch_Y-1):(round(hetch_Y)+1), round(hetch_X):(round(hetch_X)+2), 2) = 0;
    %img(round(hetch_Y-1):(round(hetch_Y)+1), round(hetch_X):(round(hetch_X)+2), 3) = 0;
    
    %Here we check that the RGB data frame doesn't have values above
    %maximum.  This is prior to adding the target since this certainly
    %overlays existing data (which will have to be delt with)
    if max(all_day_data_mask_master, [], 'all') > 1 % 1 is chosen since the WriteVideo cannot take values greater than 1
        error('Sum of all RGB masks is greater than 1 This shouldnt be possible since the masks passed binary raster check');
    end
    
    %Makes the display image with overlayed targets.
    %Must make a new mask too!
    multi_target_rgb_shape_mask(multi_target_rgb_shape_mask>1) = 1; %[row, col, rgb]
    all_days_multi_target_rgb_shape_mask = repmat(multi_target_rgb_shape_mask, 1, 1, 1, number_days);
    multi_target_binary_mask = sum(multi_target_rgb_shape_mask, 3); %[row, col, rgb]
    multi_target_binary_mask(multi_target_binary_mask>1) = 1; %[row, col]

    disp_im = all_day_data_mask_master.*not(multi_target_binary_mask) + all_days_multi_target_rgb_shape_mask;


    %unique(disp_im)
    disp('vid checkpoint 4')
    video_images = cat(4,video_images, disp_im);

    %figure
    %title(idx_1)
    %imshow(disp_im)
end
disp('writing video')
vidfile = VideoWriter(video_path_and_name, 'MPEG-4');
open(vidfile);
writeVideo(vidfile, video_images);
disp('Video Frame Numbers')
disp(vidfile.FrameCount)
close(vidfile)

real_dates = datetime(dates,'ConvertFrom','datenum');
figure
hold on
    plot(real_dates(1:1092), mean(ROI_melts(1:7,:)), '-o') %Decreasing
    plot(real_dates(1:1092), mean(ROI_melts(9:16,:)), '-o') %Naive
    plot(real_dates(1:1092), mean(ROI_melts(16:end,:)), '-o') %Persistant
    legend('Decreasing','Naive', 'Persistant')
hold off
%1092

%This is not the fastest way to do this.... It would actually be much
%faster to get the WHOLE area first.   Then grab the small regions from the
%matrix.... But I am  lazy so ill "do that later
function region_melt = get_region_melt(ROI_POINT, region_size, grid, raster, proj)
    %%%%%   Don't mess with the code below
    [row_b, row_s, col_b, col_s] = make_square_region(ROI_POINT(1), ROI_POINT(2), region_size, raster, proj);

    
    region_melt = reshape(sum(grid(row_s:row_b, col_s:col_b, :), [1 2]), 1, []);
end



function [xb, xs, yb, ys] = make_square_region(latIn, lonIn, region_size, raster, proj)
    distance = region_size/2;
    dist = sqrt(distance^2+distance^2); 
    distUnits = 'm';
   
    %Convert input distance to earth degrees (Lat, Lon are typicaly given in degrees
    arclen = rad2deg(dist/earthRadius(distUnits)); 
    [p1_lat, p1_lon] = reckon(latIn, lonIn,arclen,45);
    [p2_lat, p2_lon] = reckon(latIn, lonIn,arclen,225);

    % Converts the lat long points to image intrinsic coords.   
    [x1, y1] = lat_long_to_intrin(proj, raster, [p1_lat,p1_lon]);
    [x2, y2] = lat_long_to_intrin(proj, raster, [p2_lat,p2_lon]);

    %The problem because the axis flip so much.  We don't know which x or y
    %is large or small.... So we test for that.
    if x1 > x2
        xb = x1; %xb = big 
        xs = x2; %xs = small
    else
        xb = x2; %xb = big 
        xs = x1; %xs = small
    end

    if y1 > y2
        yb = y1; %yb = big 
        ys = y2; %ys = small
    else
        yb = y2; %xb = big 
        ys = y1; %xs = small
    end
    xs = round(xs);
    xb = round(xb);
    ys = round(ys);
    yb = round(yb);
end

%x_coor = list of x bounds
%y_coor = list of y bounds
%small_image_size [row, col] size of image
function mask1 = make_binary_shape_mask(row_s, row_b, col_s, col_b, image_size)
    x_coor = [row_s, row_b, row_b, row_s, row_s];
    y_coor = [col_s, col_s, col_b, col_b, col_s];
    mask1 = poly2mask(x_coor, y_coor,image_size(1),image_size(2));
end

% binary_shape_mask is made by "make_binary_shape_mask"
%
% rgb_color_mask is made by the "create_rgb_color_mask"
function mask2 = make_RGB_Shape_mask(binary_shape_mask, rgb_color_mask)
    mask2 = rgb_color_mask.*binary_shape_mask;
end

%Creates an RBG mask of the size of the image.   Must be multiplied by
%binary mask to create rbg_shape_mask
function mask3 = make_rgb_color_mask(RGB_VALUES, image_size)
    mask3 = permute(repmat(RGB_VALUES, [1, image_size(1), image_size(2)]), [2 3 1]);
end