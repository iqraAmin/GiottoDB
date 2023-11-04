

# Data Coercion


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






