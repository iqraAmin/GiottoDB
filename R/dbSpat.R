# initialize ####
## dbSpatData ####
setMethod('initialize', signature('dbSpatData'), function(.Object, extent, ...) {

  # call dbData initialize
  .Object = methods::callNextMethod(.Object, ...)

  # dbSpatData specific data input #
  # ------------------------------ #

  if(!missing(extent)) if(!is.null(extent)) .Object@extent = extent

  if(!is.null(.Object@data)) {
    if(sum(.Object@extent[]) == 0) .Object@extent = extent_calculate(.Object)
  }

  # check and return #
  # ---------------- #

  validObject(.Object)
  return(.Object)

})


# geom should be first col


#' @rdname extent_calculate
#' @return terra SpatExtent
#' @family Extent processing functions
#' @keywords internal
setMethod('extent_calculate', signature(x = 'dbSpatData'), function(x, ...) {
  x <- sql_query(x,
                 drop = TRUE,
                 "ST") %>%
    dplyr::pull()
  stopf('under construction')
})
