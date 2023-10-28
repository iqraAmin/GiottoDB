
# functions to aid in plotting with database sources

# Plotting pipeline from
# https://blog.djnavarro.net/posts/2022-08-23_visualising-a-billion-rows/

#' @name desparse_to_grid_bin_data
#' @title Desparsify binned data and assign to rasterized locations
#' @param data data.frame
#' @param extent terra extent in which to plot
#' @param resolution min number of tiles to collect
#' @keywords internal
desparse_to_grid_bin_data = function(data, px_x, px_y) {
  checkmate::assert_class(data, 'data.frame')
  checkmate::assert_numeric(px_x, len = 1L)
  checkmate::assert_numeric(px_y, len = 1L)
  px_x = as.integer(px_x)
  px_y = as.integer(px_y)

  tidyr::expand_grid(x_bin = 1:px_x, y_bin = 1:px_y) %>%
    dplyr::left_join(data, by = c('x_bin', 'y_bin')) %>%
    dplyr::mutate(density = tidyr::replace_na(density, 0))
}




# function to bin spatial x and y point information
#' @name raster_bin_data
#' @title Bin database values in xy
#' @param data tbl_Pool to use
#' @param extent terra extent in which to plot
#' @param resolution min number of tiles to collect
#' @return matrix of rasterized point information
#' @keywords internal
raster_bin_data = function(data,
                           extent,
                           resolution = 5e4,
                           count_info_column = NULL) {
  checkmate::assert_class(extent, 'SpatExtent')
  checkmate::assert_numeric(resolution)
  resolution = as.integer(resolution)
  checkmate::assert_class(data, 'dbPointsProxy')
  if (!is.null(count_info_column)) {
    checkmate::assert_character(count_info_column, len = 1L)
  }

  # 1. decide binning plan #
  # ---------------------- #
  # res is y and x bins needed to reach the number of tiles
  # requested by resolution
  res = get_dim_n_chunks(n = resolution, e = extent)
  px_y = res[[1L]]
  px_x = res[[2L]]
  # Find total x and y ranges
  span_x = extent$xmax - extent$xmin ; names(span_x) = NULL
  span_y = extent$ymax - extent$ymin ; names(span_y) = NULL
  # Find x and y starting locations
  xmin = extent$xmin ; names(xmin) = NULL
  ymin = extent$ymin ; names(ymin) = NULL

  # 2. perform data binning then collect values #
  # ------------------------------------------- #
  bin_data <- data[] %>%
    # select the requested spatial extent (hard selection on all sides)
    extent_filter(extent = extent, include = rep(TRUE, 4L)) %>%
    # create cols x_bin and y_bin to track what bin each record is in
    dplyr::mutate(
      unit_scaled_x = (x - xmin) / span_x,
      unit_scaled_y = (y - ymin) / span_y,
      x_bin = as.integer(round(px_x * unit_scaled_x, digits = 0L)),
      y_bin = as.integer(round(px_y * unit_scaled_y, digits = 0L))
    )

  # 3. summarize the binned information and collect #
  # ----------------------------------------------- #
  # Summary values found by grouping by x_bin and y_bin values.
  # If count_info_column is NULL, use default behavior (dplyr::count)
  # otherwise, find the sum of values in the specified count_info_column
  if (is.null(count_info_column)) {
    rast_data <- bin_data %>%
      dplyr::count(x_bin, y_bin, name = 'density') %>%
      dplyr::collect() %>%
      desparse_to_grid_bin_data(px_x = px_x, px_y = px_y)
  } else {
    rast_data <- bin_data %>%
      dplyr::group_by(x_bin, y_bin) %>%
      dplyr::summarize(density = sum(!!count_info_column)) %>%
      dplyr::collect() %>%
      desparse_to_grid_bin_data(px_x = px_x, px_y = px_y)
  }

  # 4. format collected information as a matrix of the bins #
  # ------------------------------------------------------- #
  matrix(data = rast_data$density,
         nrow = px_y,
         ncol = px_x)
}




#' internal function for rasterized rendering from matrix data
render_image = function(mat,
                        col = c("#002222", "white", "#800020"),
                        axes = FALSE,
                        ...) {
  # save current values for reset at end
  op = par()$mar
  on.exit(par(mar = op))
  par(mar = rep(0L, 4L))

  if (length(col) == 1) shades = grDevices::colorRampPalette(c("black", col))
  else shades = grDevices::colorRampPalette(col)

  graphics::image(
    z = log10(t(mat + 1)),
    axes = axes,
    asp = 1L,
    col = shades(256),
    useRaster = TRUE,
    ...
  )
}



setMethod(
  'plot', signature(x = 'dbPointsProxy', y = 'missing'),
  function(x,
           col = c("#002222", "white", "#800020"),
           resolution = 5e6,
           ...)
  {
    e = extent_calculate(x)

    # rasterize (bin) the points information as a matrix
    bin_mat = raster_bin_data(data = x, extent = e, resolution = resolution, ...)
    render_image(
      mat = bin_mat,
      x = seq(e$xmin, e$xmax, length.out = ncol(bin_mat)),
      y = seq(e$ymin, e$ymax, length.out = nrow(bin_mat))
    )
  })





