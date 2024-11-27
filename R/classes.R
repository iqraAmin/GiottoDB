
# collate
#' @include package_imports.R
NULL






# Virtual parent non-spatial classes ####


## GiottoDB ####
# Overarching package class
#' @noRd
setClass('GiottoDB', contains = 'VIRTUAL')


## dbData ####

#' @name dbData
#' @title dbData
#' @description Framework for objects that link to the database backend
#' @slot data dplyr tbl that represents the database data
#' @slot hash unique hash ID for backend
#' @slot remote_name name of table within database that contains the data
#' @slot init logical. Whether the object is fully initialized
#' @noRd
# setClass('dbData',
#          contains = c('GiottoDB',
#                       'VIRTUAL'),
#          slots = list(
#            data = 'ANY',
#            hash = 'character',
#            remote_name = 'character',
#            init = 'logical'
#          ),
#          prototype = list(
#            data = NULL,
#            hash = NA_character_,
#            remote_name = NA_character_,
#            init = FALSE
#          ))







# Backend specific ####

abbrev_path <- function(path, head = 15, tail = 35L) {
  nch <- nchar(path)
  if (nch > 60L) {
    p1 <- substring(path, first = 0L, last = head)
    p2 <- substring(path, first = nch - tail, last = nch)
    path <- paste0(p1, "[...]", p2)
  }
  return(path)
}

setClass("gdbBackendID", contains = "character")

setClass("gdbBackend", contains = "list")

setMethod("print", signature("gdbBackend"), function(x, ...) show(x))

setMethod("show", signature("gdbBackend"), function(object) {
  fields <- c("factory", "path", "id", "valid")
  pre <- sprintf("%s :", format(fields))
  names(pre) <- fields
  p <- object$info@db_path
  p <- abbrev_path(p)
  f <- sprintf("`%s`", object$info@driver_call)
  id <- object$info@hash
  v <- pool::dbIsValid(object$pool)

  cat(sprintf("GiottoDB <%s>\n", class(object)))
  cat(pre["id"], id, "\n")
  cat(pre["path"], p,"\n")
  cat(pre["factory"], f,"\n")
  cat(pre["valid"], v,"\n")
})

## backendInfo ####
#' @name backendInfo
#' @title backendInfo
#' @description
#' Simple S4 class to contain information about the database backend and
#' regenerate connection pools. A hash is generated from the db_path to act
#' as a unique identifier for each backend.
#' @slot driver_call DB driver call stored as a string
#' @slot db_path path to database
#' @slot hash xxhash64 hash of the db_path
#' @export
setClass('backendInfo',
         contains = c('GiottoDB'),
         slots = list(
           driver_call = 'character',
           db_path = 'character',
           hash = 'character'
         ),
         prototype = list(
           driver_call = NA_character_,
           db_path = NA_character_,
           hash = NA_character_
         ))






# Data Container Classes ####



## dbDataFrame ####


#' @title S4 dbDataFrame class
#' @description
#' Representation of dataframes using an on-disk database. Each object
#' is used as a connection to a single table that exists within the database.
#' @slot data dplyr tbl that represents the database data
#' @slot hash unique hash ID for backend
#' @slot remote_name name of table within database that contains the data
#' @slot key column to set as key for ordering and subsetting on i
#' @export
dbDataFrame = setClass(
  'dbDataFrame',
  contains = 'dbData',
  slots = list(
    key = 'character'
  ),
  prototype = list(
    key = NA_character_
  )
)

setMethod('show', signature('dbDataFrame'), function(object) {
  print_dbDataFrame(object, 6)
})

setMethod('print', signature('dbDataFrame'), function(x, n = 6, ...) {
  print_dbDataFrame(x, n, ...)
})

print_dbDataFrame = function(x, n, ...) {
  df_dim = dim(x@data)
  dfp = capture.output(print(x@data, n = n, na.print = NULL, ...))
  nr = df_dim[1L]
  nc = df_dim[2L]
  nr = as.character(ifelse(is.na(nr), '??', nr))
  db = gsub('# Database:', 'database   :', dfp[2L])

  cat('An object of class \'', class(x), '\'\n', sep = '')
  cat('backend_ID : ', x@hash, '\n', sep = '')
  cat('name       : \'', x@remote_name, '\' [', nr,' x ', nc,']', '\n', sep = '')
  cat(db, '\n\n')
  writeLines(dfp[-(1:2)])
}



# Virtual parent spatial classes ####

# consider ProxyData as the storage format?
# consider SpatST as the more flexible function that can be non-permanent?


## dbSpatProxyData ####
#' @name dbSpatProxyData
#' @title dbSpatProxyData
#' @description Framework for terra SpatVector database backend proxy objects
#' @slot data dplyr tbl that represents the database data
#' @slot hash unique hash ID for backend
#' @slot remote_name name of table within database that contains the data
#' @slot extent spatial extent
#' @slot poly_filter polygon SpatVector that is used to filter values on read-in
#' @noRd
setClass('dbSpatProxyData',
         contains = c('dbData', 'VIRTUAL'),
         slots = list(
           extent = 'SpatExtent',
           poly_filter = 'ANY'
         ),
         prototype = list(
           extent = terra::ext(0, 0, 0 ,0),
           poly_filter = NULL
         ))






# Spatial Data Container Classes ####

## dbPolygonProxy ####
#' @title S4 dbPolygonProxy class
#' @description
#' Representation of polygon information using an on-disk database. Intended to
#' be used to store information that can be pulled into terra polygon SpatVectors
#' @slot data lazy table containing geometry information with columns geom, part,
#' x, y, and hole
#' @slot attributes dbDataFrame of attributes information, one of which (usually
#' the first) being 'ID' that can be joined/matched against the 'geom' values in
#' \code{attributes}
#' @slot n_poly number of polygons
#' @slot poly_ID polygon IDs
#' @slot extent extent of polygons
#' @slot poly_filter polygon SpatVector that is used to filter values on read-in
#' @export
dbPolygonProxy = setClass(
  'dbPolygonProxy',
  contains = 'dbSpatProxyData',
  slots = list(
    attributes = 'dbDataFrame',
    n_poly = 'numeric'
  ),
  prototype = list(
    n_poly = NA_integer_
  )
)



setMethod('show', signature(object = 'dbPolygonProxy'), function(object) {
  print(object)
})

setMethod('print', signature(x = 'dbPolygonProxy'), function(x, n = 3, ...) {
  print_dbPolygonProxy(x = x, n = n, ...)
})


print_dbPolygonProxy = function(x, n, ...) {
  refresh = getOption('gdb.update_show', FALSE)
  p = capture.output(print(dplyr::select(x@attributes@data, -c('geom')), n = n,
                           na.print = NULL, max_footer_lines = 0L,
                           width = getOption('width') - 12L, ...))
  db = gsub('# Database:', 'database   :', p[2L])
  vs = paste0('\nvalues     : ', p[3L], '\n')
  indent = '            '
  # update values
  if(refresh) ex = extent_calculate(x)
  else ex = '??'
  ex = paste0(
    paste(ex[], collapse = ', '), ' (',
    paste(c('xmin', 'xmax', 'ymin', 'ymax'), collapse = ', '), ')'
  )
  ds = if(refresh) paste(dim(x), collapse = ', ')
  else paste('??', ncol(x), sep = ', ')

  cat('An object of class \'', class(x), '\'\n', sep = '')
  cat('backend    : ', x@hash, '\n', sep = '')
  cat('table      : \'', x@remote_name, '\'\n', sep = '')
  cat(db, '\n')
  cat('dimensions :', ds, ' (points, attributes)\n')
  cat('extent     : ', ex, sep = '')
  cat(vs)
  writeLines(paste(indent, p[-(1:3)])) # skip first header lines
  if(!refresh) cat('\n# set options(gdb.update_show = TRUE) to calculate ??')
}





## dbPointsProxy ####
#' @title S4 dbPointsProxy class
#' @description
#' Representation of point information using an on-disk database. Intended to
#' be used to store information that can be pulled into terra point SpatVectors
#' @slot n_points number of points
#' @slot feat_ID feature IDs
#' @slot extent extent of points
#' @slot poly_filter polygon SpatVector that is used to filter values on read-in
#' @export
dbPointsProxy = setClass(
  'dbPointsProxy',
  contains = 'dbSpatProxyData',
  slots = list(
    n_point = 'numeric'
  ),
  prototype = list(
    n_point = NA_integer_
  )
)



setMethod('show', signature(object = 'dbPointsProxy'), function(object) {
  print(object, n = 3)
})

setMethod('print', signature(x = 'dbPointsProxy'), function(x, n = 3, ...) {
  print_dbPointsProxy(x = x, n = n, ...)
})

print_dbPointsProxy = function(x, n, ...) {
  refresh = getOption('gdb.update_show', FALSE)
  tbl_data = x@data %>%
    dplyr::arrange(.uID) %>%
    dplyr::select(-c('.uID', 'x', 'y'))
  p = capture.output(print(tbl_data, n = n,
                           na.print = NULL, max_footer_lines = 0L,
                           width = getOption('width') - 12L, ...))
  db = gsub('# Database:', 'database   :', p[2L])
  vs = paste0('\nvalues     : ', p[3L], '\n')
  indent = '            '
  # update values
  if(refresh) ex = extent_calculate(x)
  else ex = '??'
  ex = paste0(
    paste(ex[], collapse = ', '), ' (',
    paste(c('xmin', 'xmax', 'ymin', 'ymax'), collapse = ', '), ')'
  )
  ds = if(refresh) paste(dim(x), collapse = ', ')
  else paste('??', ncol(x), sep = ', ')


  cat('An object of class \'', class(x), '\'\n', sep = '')
  cat('backend    : ', x@hash, '\n', sep = '')
  cat('table      : \'', x@remote_name, '\'\n', sep = '')
  cat(db, '\n')
  cat('dimensions :', ds, ' (points, attributes)\n')
  cat('extent     : ', ex, sep = '')
  cat(vs)
  writeLines(paste(indent, p[-(1:3)])) # skip first header lines
  if(!refresh) cat('\n# set options(gdb.update_show = TRUE) to calculate ??')
}













# Virtual Class Unions ####

## dgbIndex ####
#' @title Virtual Class "gdbIndex" - Simple Class for GiottoDB indices
#' @name gdbIndex
#' @description
#' This is a virtual class used for indices (in signatures) for indexing
#' and sub-assignment of 'GiottoDB' objects. Simple class union of 'logical',
#' 'numeric', 'integer', and  'character'.
#' Based on the 'index' class implemented in \pkg{Matrix}
#' @keywords internal
#' @noRd
setClassUnion('gdbIndex',
              members = c('logical', 'numeric', 'integer', 'character'))
#' @title Virtual Class "gdbIndexNonChar" - Simple Class for GiottoDB indices
#' @name gdbIndex
#' @description
#' This is a virtual class used for indices (in signatures) for indexing
#' and sub-assignment of 'GiottoDB' objects. Simple class union of 'logical' and
#' 'numeric'.
#' Based on the 'index' class implemented in \pkg{Matrix}
#' @keywords internal
#' @noRd
setClassUnion('gdbIndexNonChar',
              members = c('logical', 'numeric'))

## dbMF ####
#' @title Virtual Class "dbMFData" - Simple class for GiottoDB matrix and dataframes
#' @name dbMFData
#' @description
#' deprecated
#' @keywords internal
#' @noRd
setClassUnion('dbMFData',
              members = c('dbDataFrame'))


