upper_left = [38.60308369519263, -124.27430789921323];
lower_right = [36.15819232551875, -118.8437565890432];

file_path = "/Users/Cannon/Documents/School/UCSB/Briggs Lab/Thaw_Rate_Hypothesis/" + ...
    "Raw Snow Melt Data (Bair et. Al) /reconstruction_WUS_2021.h5";

metavariable = 'sweHybrid';
start_day = 150;
number_days = 1;

%big_grid = h5read(file_path, ['/Grid/' metavariable], [1,1,start_day], ...
%           [4800,7200,number_days]);
big_RefRas = build_ReferenceRaster(file_path, metavariable);
%ref = h5readatt(file_path, '/Grid', 'ReferencingMatrix');
%proj = build_Projection(file_path)

sf = upper_left;%[37.54402599234827, -122.15531515461105];
[sf_x,sf_y] = projfwd(proj, sf(1), sf(2));
[sf_x_I, sf_y_I] = worldToIntrinsic(big_RefRas, sf_x, sf_y);

sf2 = lower_right;%[37.54402599234827, -122.15531515461105];
[sf_x2,sf_y2] = projfwd(proj, sf2(1), sf2(2));
[sf_x_I2, sf_y_I2] = worldToIntrinsic(big_RefRas, sf_x2, sf_y2);


[small_grid, small_RefRaster] = getmelt_specifc_region( ...
                                file_path, metavariable, upper_left, ...
                                lower_right, start_day, number_days);

sf = [[32.488261323945785, -121.30469887160692]];
[sf_x,sf_y] = projfwd(proj, sf(1), sf(2));
[sf_x_I_B, sf_y_I_B] = worldToIntrinsic(big_RefRas, sf_x, sf_y);
[sf_x_I_S, sf_y_I_S] = worldToIntrinsic(small_RefRaster, sf_x, sf_y);

figure
imshow(small_grid)
hold on
scatter(sf_x_I_S, sf_y_I_S, 33, "blue", "filled")
hold off


figure
imshow(big_grid)
hold on
scatter(sf_x_I, sf_y_I, 33, "blue", "filled")
scatter(sf_x_I2, sf_y_I2, 33, "Green", "filled")
scatter(sf_x_I2, sf_y_I, 33, "red", "filled")
scatter(sf_x_I, sf_y_I2, 33, "Yellow", "filled")
scatter(sf_x_I_B, sf_y_I_B, 33, "blue", "filled")
hold off
%figure
%[C, RC] = imfuse(big_grid, big_RefRas, small_grid, small_RefRaster, 'ColorChannels',[1 2 0]);
%imshow(C)
%geoshow(big_grid,big_RefRas)



%figure
%landareas = shaperead('landareas.shp','UseGeoCoords',true);
%axesm ('sinusoid', 'Frame', 'on', 'Grid', 'on');
%geoshow(landareas,'FaceColor',[1 1 .5],'EdgeColor',[.6 .6 .6]);
%tissot;
%worldmap('World')
%load coastlines
%plotm(coastlat,coastlon)

%big_RefRaster = build_ReferenceRaster(file_path, metavariable);
%proj = build_Projection(file_path);

%[x, y] = intrinsicToWorld(big_RefRaster, 4800, 7200);
%[lat, lon] =  projinv(proj, x, y);
%disp([lat lon]);

%hetch_hetchy = [37.95315771608674, -119.73316579589012];
%[x_proj,y_proj] = projfwd(proj, hetch_hetchy(1), hetch_hetchy(2));
%[xint, yint] = worldToIntrinsic(big_RefRaster,x_proj,y_proj);
%row = yint;
%column = xint;
%data = h5read(file_path, '/Grid/swe', [row, column, 1], [1,1,365]);
%imshow(h5read(file_path, '/Grid/melt', [1,1,150],[4800,7200,1]))
disp('all done')