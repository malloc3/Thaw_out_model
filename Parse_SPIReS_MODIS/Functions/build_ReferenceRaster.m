%Function to build the ReferenceRaster object
% h5file = file path for h5file.  Depends on the consistant data format of
% metavariable = variable to read, string, choices are:
%               'swe' - daily reconstructed swe, mm
%               'melt' - daily melt, mm
%               'maxswedates' - date of max swe
% Ned Bair melt data
function RefRaster = build_ReferenceRaster(file_path, metavariable)

% This is the depricated Reference matrix.  Must be converted to a Raster
% Reference
varloc = ['/Grid/' metavariable]; %no idea why you have to concat like this
ReferencingMatrix = h5readatt(file_path, '/Grid', 'ReferencingMatrix');
demensions = h5info(file_path, varloc).Dataspace.Size(1:2);
RefRaster = refmatToMapRasterReference(ReferencingMatrix,demensions);

% Alright so this bit of code does the right thing but the reference
% matrix given in the data isnt..... right.  It is covering 6 total tiles
% h8v4 to h11v5.   And when you ask for the grid locations this "looks"
% right as far as the code is conserned.  However if you display the melt
% data as an image.... It clearly only cover tile h8v5...    Sooooo Ima
% hard code the boundaries of tile h8v5 into the reference matrix to see if
% it will give different results... we shall see
%RefRaster.XWorldLimits = [-11119041.92, -10007554.63];
%RefRaster.YWorldLimits = [3335851.56,4447338.73]; 