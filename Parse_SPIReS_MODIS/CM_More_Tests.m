%Set file path for the data
file_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2021.h5";

%Video_save location
video_file_path = '/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/Raw Snow Melt Data (Bair et. Al) /Videos';
video_name = 'test_video'; %Will automatically add file type and meta variable used

% The melt data desired (options 'sweHybrid' 'melt' 'swe')
metavariable = 'swe';

%Set the map boundaries using p1 and p2 (points define a square!)
p1 = [36.77429578735711, -120.54067410732728];
p2 = [39.43070778973512, -119.58800589178136];

% Pinpoint a specific location on the map and set color
target = [37.07095335	-118.5436752]; %Hetch Hetchy = [37.95315771608674, -119.73316579589012]
target_color = [0; 94/255; 184/255]; %Gotta be a vertical array so use ;
target_size = [5,5]; %In pixles (its actually double this... + and - size)

%Dates.  Should be improved but for now 1 is like late summer early fall
start_day = 1;
number_days = 364;

max_melt_constant = 10000; %mm of melt.  This is slightly arbitrary for graphing.  (too high and images become too dark)









%%%%%   Don't mess with the code below
video_path_and_name = [video_file_path '/' video_name '_' metavariable];

[small_grid, small_RefRaster] = getmelt_specifc_region( ...
                                file_path, metavariable, p1, ...
                                p2, start_day, number_days);

proj = build_Projection(file_path);
[target_X, target_Y] = lat_long_to_intrin(proj, small_RefRaster, target);
small_image_size = small_RefRaster.RasterSize;

%Creates mask of target region
%Row and col have to be switched from x and y.   Because of projection x =
%column and y = row!
x_s = target_X-target_size(1); %small value of x
x_b = target_X+target_size(1); %big value of x
y_s = target_Y-target_size(2); % ^^
y_b = target_Y+target_size(2); % ^^
%vv start included twice to close vv
row_bounds = [x_s, x_b, x_b, x_s, x_s];
col_bounds = [y_s, y_s, y_b, y_b, y_s];

tbm = make_binary_shape_mask(row_bounds, col_bounds, small_image_size);
target_color_mask = make_rgb_color_mask(target_color, small_image_size);
target_rgb_shape_mask = make_RGB_Shape_mask(tbm, target_color_mask);

no_data_color = [55/255; 79/255; 47/255]; % Yellow?

no_data_color_mask = make_rgb_color_mask(no_data_color, small_image_size);

%Creates Video across dates of interest!
vidfile = VideoWriter(video_path_and_name, 'MPEG-4');
open(vidfile);
for ind = 1:number_days
    %Converts image to RGB but keeps it as gray scale (more or less lmao)
    day_grid = double(small_grid(:,:,ind));
    %zeros_binary_mask = day_grid == 0;
    big_nums_binary = day_grid == 65535; % Records where all NAN values are
    melt_binary_grid = not(big_nums_binary); % Records where all non NAN values are

    %Normalizes the display to the max melt constant (darker is hgiher
    %melt)  
    normalized_day_grid = 1- day_grid/max_melt_constant;
    reduced_normalized_day_grid = normalized_day_grid.*melt_binary_grid;

    if max(reduced_normalized_day_grid, [], "all") > 1 | max(reduced_normalized_day_grid, [], "all") < 0
        error('Max_melt_constant is too small for dataset')
    end

    img = repmat(reduced_normalized_day_grid, 1, 1, 3);
    
    %Sets the 65535 values no data color
    big_nums_rgb_shape_mask2 = make_RGB_Shape_mask(big_nums_binary, no_data_color_mask);
    big_nums_rgb_shape_mask = big_nums_rgb_shape_mask2.*big_nums_binary;
 
    %Creates a master mask
    data_mask_master = big_nums_rgb_shape_mask + img;
    
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
    if max(data_mask_master, [], 'all') > 1 % 1 is chosen since the WriteVideo cannot take values greater than 1
        error('Sum of all RGB masks is greater than 1 This shouldnt be possible since the masks passed binary raster check');
    end

    %disp_im = data_mask_master + target_rgb_shape_mask;
    %This combines 
    disp_im = data_mask_master.*not(tbm) + target_rgb_shape_mask;
    
    writeVideo(vidfile, disp_im);
end
close(vidfile)
disp('All Done')










%x_coor = list of x bounds
%y_coor = list of y bounds
%small_image_size [row, col] size of image
function mask1 = make_binary_shape_mask(x_coor, y_coor, image_size)
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
