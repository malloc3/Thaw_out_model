function [x_intr, y_intr] = lat_long_to_intrin(proj, ReferenceRaster, lat_lon)
[w_X,w_Y] = projfwd(proj, lat_lon(1), lat_lon(2));
[x_intr, y_intr] = worldToIntrinsic(ReferenceRaster, w_X, w_Y);