
#' @importFrom methods setAs
#' @importFrom terra as.polygons
#' @importFrom terra as.points
NULL


# Data Coercion




# as.matrix ####


#' @rdname hidden_aliases
#' @title Convert a GiottoDB object to a matrix
#' @param x dbMatrix
#' @param ... additional params to pass
#' @export
setMethod('as.matrix', signature('dbMatrix'), function(x, ...) {
  # message('Pulling DB matrix into memory...')
  p_tbl = x@data %>%
    tidyr::pivot_wider(names_from = 'j', values_from = 'x') %>%
    dplyr::collect()

  mtx = p_tbl %>%
    dplyr::select(-i) %>%
    as('matrix')

  rownames(mtx) = p_tbl$i

  return(mtx)
})





# as.polygons ####

#' @rdname hidden_aliases
#' @export
setMethod('as.polygons', signature('dbPolygonProxy'), function(x, ...) {
  dbspat_to_sv(x)
})


# as.points ####
#' @rdname hidden_aliases
#' @export
setMethod('as.points', signature('dbPointsProxy'), function(x, ...) {
  dbspat_to_sv(x)
})

# as.spatvector ####
#' @rdname as.spatvector
#' @title Convert to terra spatvector
#' @export
setMethod('as.spatvector', signature('dbSpatProxyData'), function(x, ...) {
  switch(class(x),
         'dbPolygonProxy' = as.polygons(x, ...),
         'dbPointsProxy' = as.points(x, ...))
})



# as.dbspat ####
#' @name as.dbspat
#' @title Convert to dbspat

#' @rdname as.dbspat
#' @export
setMethod('as.dbspat', signature('dbPointsProxy'), function(x, ...) {
  duckdb_extension(cPool(x), 'spatial')

  dbst = new(
    'dbPointsST',
    data = x@data,
    hash = x@hash,
    remote_name = x@remote_name,
    init = x@init,
    extent = x@extent)

  dbst <- sql_query(
    dbst,
    drop = FALSE,
    statement = "
    SELECT ST_Point(x, y) AS geom, *
    FROM :data:"
  )
})





