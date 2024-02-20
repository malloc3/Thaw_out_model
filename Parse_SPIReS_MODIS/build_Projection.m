%Function to build the proper projection object
% h5file = file path for h5file.  Depends on the consistant data format of
% Ned Bair melt data
function mstruct = build_Projection(file_path)

%This gets all the h5 attributes that is needed to create the projection
%and referencing matrix for the large dataset.
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
