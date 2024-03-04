% This function will pull a subset of the larger h5 file for easier use and
% create a smaller grid.  It will also provide a RasterReference to
% convert from WorldCoordinates to IntrinsicCoordinates.  It should use the
% same WorldCoordinates as the original RasterReference.   
% 
% It will NOT create a ReferenceMatrix as is included in the original Ned
% Bair data. The ReferenceMatrix is depricated and replaced by the Raster
% Reference.  Most functions work with both though!

%I have chosen to not make this as flexible as the original getmelt since
%such functionallity is not needed for my project


%INput
% file_path = the filepath to the h5file (or other formats that h5read can use)
% metavariable = variable to read, string, choices are:
%               'swe' - daily reconstructed swe, mm
%               'melt' - daily melt, mm
%               'maxswedates' - date of max swe
%               'DebrisMelt' - daily under debris ice melt, mm
% upper_left_corner = [lat, long] coordinates of upper left region of
%                     interest
% lower_right_corner = [lat, long] coordinates of lower right region of
%                      interest
% NOT_INCLUDED: dates = dates of interest (default all dates) [want to add
%                     this functionality eventually )

%OUtput
%small_grid = the subsection of the h5file.  [rows, columns, dates]. The
%             same format as the original h5file.  But just smaller
%reference_matrix = a new reference matrix to be used for referencing this
%                   smaller grid.
function [small_grid, small_RefRaster] = getmelt_specifc_region(file_path, ...
    metavariable, p1, p2, start_day, number_days)
varloc = ['/Grid/' metavariable];

%This is hard coded here for now.  Later we may include a variable date
% selection too!
% Ehh this is done this way to make it
% easier to update later.... Probably a dumb Idea but such is life
% h5info(file_path, varloc).Dataspace.Size(3); %This would just set it for
% all days
start_date_int = start_day;
end_date = start_date_int + number_days;
max_dates = h5info(file_path, varloc).Dataspace.Size(3);
if end_date > max_dates
    print(['More days requested than exist in dataset.  All available dates' ...
        'provided'])
    end_date = max_dates;
elseif end_date < start_date_int
    error(['Invalid dates Boundaries: Cannot end on or before start date'])
end
count_date = double(end_date - start_date_int);


world_projection = build_Projection(file_path);
large_RefRas = build_ReferenceRaster(file_path, metavariable);

[world_p1_x,world_p1_y] = projfwd(world_projection, p1(1), p1(2));
[intrinsic_p1_x, intrinsic_p1_y] = worldToIntrinsic(large_RefRas, world_p1_x, world_p1_y);

[world_p2_x,world_p2_y] = projfwd(world_projection, p2(1), p2(2));
[intrinsic_p2_x, intrinsic_p2_y] = worldToIntrinsic(large_RefRas, world_p2_x, world_p2_y);

array_intrinsic_x = [intrinsic_p1_x, intrinsic_p2_x];
array_intrinsic_y = [intrinsic_p1_y, intrinsic_p2_y];

intrinsic_x_start = min(array_intrinsic_x);
intrinsic_y_start = min(array_intrinsic_y);

intrinsic_x_end = min(array_intrinsic_x);
intrinsic_y_end = min(array_intrinsic_y);

count_x = abs(diff(array_intrinsic_x));
count_y = abs(diff(array_intrinsic_y));

% WHEN YOU MAKE THE ARRAY FOR START AND COUNT must switch X and Y to fit
% proper ROWS and Columns.  X = Columns, Y = Rows
start_arry = [intrinsic_y_start, intrinsic_x_start, start_date_int];
count_arry = [count_y, count_x, count_date];

if max(start_arry < 0)
    error('Invalid Boundaries: Start condition is outside bounds')
end

small_grid = h5read(file_path, varloc,start_arry, count_arry);

%Makes new RasterReference for smaller sub grid
[row, col, dates] = size(small_grid);
small_RefRaster = reduce_ref_raster(large_RefRas, [row, col], start_arry);




%This function will reduce the size of the reference raster.
%
% INput
% main_RefRaster = the ReferenceRaster for the large dataset
% new_size = size of new data [row, column, date] (date optional)
% start = start of new data in IntrinsicCoordinates (to the large dataset)
%
% OUTput
% small_RefRaster = new ReferenceRaster adjusted to new dataset size
function small_RefRaster = reduce_ref_raster(main_RefRaster, new_size, start)
small_RefRaster = main_RefRaster;

intrin_start_y = start(1); 
intrin_start_x = start(2);

intrin_end_x = intrin_start_x + new_size(2); 
intrin_end_y = intrin_start_y + new_size(1); 

[xWorld_Start, yWorld_Start] = intrinsicToWorld(main_RefRaster, intrin_start_x, intrin_start_y);
[xWorld_End, yWorld_End] = intrinsicToWorld(main_RefRaster, intrin_end_x, intrin_end_y);

%The raster image is a little tricky in the Y coordinates.   It must be
%small to large [y_low_lim, y_up_lim]  But thats opposite of the way we
%think of the image with the upper left coner being the start of the image.
%So we shall switch the yWorld_End and Start here to be smaller and then
%bigger!  But we will also add a check here with a better error message
if yWorld_End > yWorld_Start
    error("Come to the getmelt_specific_region function and read code comments for assistance")
end

small_RefRaster.XWorldLimits = [xWorld_Start, xWorld_End];
small_RefRaster.YWorldLimits = [yWorld_End, yWorld_Start];

small_RefRaster.RasterSize = new_size;