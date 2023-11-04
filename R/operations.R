

# Spatial Ops ####

## t ####

#' @rdname hidden_aliases
#' @export
setMethod('t', signature(x = 'dbPointsProxy'), function(x) {
  x = reconnect(x)
  x@data = x@data %>% dplyr::select(x = y, y = x, dplyr::everything())
  e = x@extent
  x@extent = terra::ext(e$ymin, e$ymax, e$xmin, e$xmax)
  x
})

#' @rdname hidden_aliases
#' @export
setMethod('t', signature(x = 'dbPolygonProxy'), function(x) {
  x = reconnect(x)
  x@data = x@data %>% dplyr::select(geom, part, x = y, y = x, hole)
  e = x@extent
  x@extent = terra::ext(e$ymin, e$ymax, e$xmin, e$xmax)
  x
})



## nrow ####

#' @name nrow
#' @title The number of rows/cols
#' @description
#' \code{nrow} and \code{ncol} return the number of rows or columns present in
#' \code{x}.
#' @aliases ncol
NULL

#' @rdname hidden_aliases
#' @export
setMethod('nrow', signature(x = 'dbDataFrame'), function(x) {
  x = reconnect(x)
  dim(x)[1L]
})

#' @rdname hidden_aliases
#' @export
setMethod('nrow', signature(x = 'dbPointsProxy'), function(x) {
  x = reconnect(x)
  dim(x)[1L]
})

#' @rdname hidden_aliases
#' @export
setMethod('nrow', signature(x = 'dbPolygonProxy'), function(x) {
  x = reconnect(x)
  dim(x@attributes)[1L]
})

## ncol ####


#' @rdname hidden_aliases
#' @export
setMethod('ncol', signature(x = 'dbDataFrame'), function(x) {
  x = reconnect(x)
  ncol(x@data)
})

#' @rdname hidden_aliases
#' @export
setMethod('ncol', signature(x = 'dbPointsProxy'), function(x) {
  x = reconnect(x)
  ncol(x@data) - 3L # remove 3 for .uID,  x, and y cols
})

#' @rdname hidden_aliases
#' @export
setMethod('ncol', signature(x = 'dbPolygonProxy'), function(x) {
  x = reconnect(x)
  ncol(x@attributes@data) - 1L # remote one for geom col in attrs
})

#' @rdname hidden_aliases
#' @export
setMethod('ncol', signature(x = 'dbDataFrame'), function(x) {
  x = reconnect(x)
  ncol(x@data)
})

## dim ####

#' @rdname hidden_aliases
#' @export
setMethod('dim', signature('dbData'), function(x) {
  x = reconnect(x)
  nr = x@data %>%
    dplyr::summarise(n()) %>%
    dplyr::pull() %>%
    as.integer()
  c(nr, ncol(x@data))
})

#' @rdname hidden_aliases
#' @export
setMethod('dim', signature('dbPointsProxy'), function(x) {
  res = callNextMethod(x)
  res[2L] = res[2L] - 3L # hide ncols that include .uID, x, and y cols
  res
})

#' @rdname hidden_aliases
#' @export
setMethod('dim', signature('dbPolygonProxy'), function(x) {
  nr = nrow(x@attributes) # use method for dbDataFrame on attr table
  nc = ncol(x@attributes) - 1L # remove count for 'geom' col
  c(nr, nc)
})

#' @rdname hidden_aliases
#' @export
setMethod('length', signature('dbSpatProxyData'), function(x) {
  nrow(x)
})

## head ####

#' @export
setMethod('head', signature(x = 'dbDataFrame'), function(x, n = 6L, ...) {
  x = reconnect(x)

  x[] = x[] %in% head(x, n = n)
  x
})

## tail ####

#' @export
setMethod('tail', signature(x = 'dbDataFrame'), function(x, n = 6L, ...) {
  x = reconnect(x)

  x[] = x[] %in% tail(x, n = n)
  x
})

# Column data types ####
# Due to how these functions will be commonly seen within other functions, a
# call to `reconnect()` is omitted.

## colTypes ####

#' @name colTypes
#' @title Column data types of GiottoDB objects
#' @description
#' Get the column data types of objects that inherit from \code{'dbData'}
#' @param x GiottoDB data object
#' @param ... additional params to pass
#' @export
setMethod('colTypes', signature(x = 'dbData'), function(x, ...) {
  vapply(data.table::as.data.table(head(x[], 1L)), typeof, character(1L))
})

## castNumeric ####

#' @name castNumeric
#' @title Set a column to numeric
#' @description
#' Sets a column to numeric after first checking the column data type. Does
#' nothing if the column is already a \code{double}
#' This precaution is to avoid truncation of values.
#' @param x GiottoDB data object
#' @param col column to cast to numeric
#' @param ... additional params to pass
#' @export
setMethod('castNumeric', signature(x = 'dbData', col = 'character'), function(x, col, ...) {
  if(colTypes(x)[col] != 'double') {
    sym_col = dplyr::sym(col)
    x[] = x[] %>% dplyr::mutate(!!sym_col := as.numeric(!!sym_col))
  }
  x
})
