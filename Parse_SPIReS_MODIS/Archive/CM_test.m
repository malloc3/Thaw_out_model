%This code is mostly a test but for now the goal is to read the data coming
%from Ned Bair's melt data.   These data are in huge h5 files that I
%haven't worked with before so its a learning process.   Right now just
%trying to come up with a way to convert specific LAT long locations to the
%correct cell in the datafram.  THis has turned out to be harder than
%expected but thankfully MatLab has a few handy features to help with this.

%NOTE  several of the functions used by Ned to create teh code
%(Specifically the referening matrix) is now depricated from MatLab.
%There are work around but there is some uncertanty on what these
%workarounds are and how to make them work.... WE shall see

%The file path to the data.  Currently only works with NED Bairs formatted
%data.   THis is custom code don't expect it to work generally 
file_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2021.h5";

%Gets the dates from the file.  Handy for plotting later but probably
%unneeded
mat_dates = h5readatt(file_path, '/', 'MATLABdates');

%This gets all the h5 attributes that is needed to create the projection
%and referencing matrix.   Honestly there is probably a better way to do
%this but hecc
mapprojection = h5readatt(file_path, '/Grid', 'mapprojection');
angleunits = h5readatt(file_path, '/Grid', 'angleunits');
aspect = h5readatt(file_path, '/Grid', 'aspect');
falsenorthing = h5readatt(file_path, '/Grid', 'falsenorthing');
falseeasting = h5readatt(file_path, '/Grid', 'falseeasting');
geoid = h5readatt(file_path, '/Grid', 'geoid');
maplatlimit = h5readatt(file_path, '/Grid', 'maplatlimit');
maplonlimit = h5readatt(file_path, '/Grid', 'maplonlimit');
mapparallels = h5readatt(file_path, '/Grid', 'mapparallels');
nparallels = h5readatt(file_path, '/Grid', 'nparallels');
origin = h5readatt(file_path, '/Grid', 'origin');
scalefactor = h5readatt(file_path, '/Grid', 'scalefactor');
trimlat = h5readatt(file_path, '/Grid', 'trimlat');
trimlon = h5readatt(file_path, '/Grid', 'trimlon');

%The referencing matrix.  This is depricated from matlab.  Soooo thats
%life.  Will deal with later.  But pulls the matrix directly from hte h5
%data.
ReferencingMatrix = h5readatt(file_path, '/Grid', 'ReferencingMatrix');

%This creates the projection.  Should probably be named somethign other
%than mstruct.  But that is what it is for now.
mstruct = defaultm(mapprojection); %Makes a default projection based on the
                                    %Projection given from the meta data
%This changes all the default values to the values specified in the
%metadata.  Broadly these are the same as the default projection but some
%may be slightly different.
mstruct.angleunits = angleunits;
mstruct.aspect = aspect;
mstruct.falsenorthing = falsenorthing;
mstruct.falseeasting = falseeasting;
mstruct.geoid = geoid;
mstruct.maplatlimit = maplatlimit;
mstruct.maplonlimit = maplonlimit;
mstruct.mapparallels = mapparallels;
mstruct.nparallels = nparallels;
mstruct.origin = origin;
mstruct.scalefactor = scalefactor;
mstruct.trimlat = trimlat;
mstruct.trimlon = trimlon;


%This is just a single location for testing.  Will probably change later
hetch_hetchy = [37.95315771608674, -119.73316579589012];

%Uses the projection generated above to get an X, Y position based on the
%used projection.   No idea how it does that.  MatLab function that I am
%going to trust works right.  Output doesn't make a TON of sense to me
%right now but none of this project does so Ima go with it.  It is in the
%same order of magnitude as the values in the ReferencingMatrix so I feel
%kinda good about it.
[world_x,world_y] = projfwd(mstruct, hetch_hetchy(1), hetch_hetchy(2));

%R is the new ReferencingMatrix.  Its common in the documentation to use R
%as the variable for the refeencing matrix so I will go with it.
%This function is supposed to reformat the ReferencingMatrix to a format
%that is not Depricated.   I don't want to use it but it seems like the
%only way right now so we shall see.   We have to hard code the demensions
%for now.  But eventually this could come from the demensions of the data
%it's self I suppose....
rasterSize = [4800, 7200]; %This is the demensions of the data.  Should be 
                          %pulled directly from the data.
R = refmatToMapRasterReference(ReferencingMatrix,rasterSize);

%This converts the "World Coordinates" calculated above from the
%projection into "Intrinsic Coordinates" that are associated with the
%data's "pixilized" format.  (Kinda think of these data as an image and the
%colors are the melt rate!
%This function uses the Reference Matrix we came up with earlier.
[intrinsic_x, intrinsic_y] = worldToIntrinsic(R, world_x, world_y);
int_intr_x = int64(intrinsic_x);
int_intr_y = int64(intrinsic_y);


%start = [intrinsic_x, intrinsic_y, 1];
%count = [1, 1, 365];
%melt_at_location_and_Dates = h5read(file_path, '/Grid/melt',start, count)

%Gets all the data!!  This is generally a bad idea.  Mostly because it is
%SOOOO damn slow.  Dont do it.  Computer might crash
%melt_all_dates = getMelt(file_path, 'swe');

disp("all done")