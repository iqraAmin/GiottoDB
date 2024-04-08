
# dbData ####
## Empty ####
### Extract [] ####
#' @rdname hidden_aliases
#' @export
setMethod('[', signature(x = 'dbData', i = 'missing', j = 'missing', drop = 'missing'),
          function(x, i, j) {
            x@data
          })



### Set [] ####
# no initialize to prevent slowdown
#' @rdname hidden_aliases
#' @export
setMethod('[<-', signature(x = 'dbData', i = 'missing', j = 'missing', value = 'ANY'),
          function(x, i, j, value) {
            x@data = value
            x
          })




# dbDataFrame ####
## rows only ####
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbDataFrame', i = 'gdbIndex', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x = .reconnect(x)
    if(any(is.na(x@key))) stopf('Set dbDataFrame key with `keyCol()` to subset on \'i\'')

    # numerics and logical
    if(is.logical(i) | is.numeric(i)) {
      if(is.logical(i)) i = which(i)
      x@data = x@data %>%
        flex_window_order(x@key) %>%
        dplyr::mutate(.n = dplyr::row_number()) %>%
        dplyr::collapse() %>%
        dplyr::filter(.n %in% i) %>%
        dplyr::select(-.n) %>%
        dplyr::collapse()
    } else { # character
      x@data = x@data %>%
        flex_window_order(x@key) %>%
        dplyr::filter(!!as.name(x@key) %in% i) %>%
        dplyr::collapse()
    }
    x
  })
## cols only ####
#' @rdname hidden_aliases
#' @export
setMethod('[', signature(x = 'dbDataFrame', i = 'missing', j = 'gdbIndex', drop = 'ANY'),
          function(x, j, ..., drop = FALSE) {
            x = .reconnect(x)
            checkmate::assert_logical(drop)

            if(is.logical(j)) j = which(j)
            x@data = x@data %>% dplyr::select(dplyr::all_of(j))
            x
          })
#' @rdname hidden_aliases
#' @export
setMethod('[', signature(x = 'dbDataFrame', i = 'gdbIndex', j = 'gdbIndex', drop = 'ANY'),
          function(x, i, j, ..., drop = FALSE) {
            x = .reconnect(x)
            x = x[i,]
            x = x[, j]
            x
          })
## Empty ####
### Extract [] ####
#' @rdname hidden_aliases
#' @export
setMethod('[', signature(x = 'dbDataFrame', i = 'missing', j = 'missing', drop = 'ANY'),
          function(x, i, j, drop = FALSE) {
            x@data
          })
### Set [] ####
# no initialize to prevent slowdown
#' @rdname hidden_aliases
#' @export
setMethod('[<-', signature(x = 'dbDataFrame', i = 'missing', j = 'missing', value = 'ANY'),
          function(x, i, j, value) {
            x@data = value
            x
          })


# internal function
# workaround for multiple dbplyr window column ordering
flex_window_order = function(x, order_cols) {
  keys = paste0('!!as.name("', order_cols, '")')
  keys = paste0(keys, collapse = ', ')
  call_str = paste0('x %>% dbplyr::window_order(', keys, ')')
  eval(str2lang(call_str))
}





# dbSpatProxyData ####

## rows only ####
# character input subsets the attribute table
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPolygonProxy', i = 'character', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x = .reconnect(x)
    x@attributes@data = x@attributes@data %>%
      dplyr::select(i)
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPointsProxy', i = 'character', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x = .reconnect(x)
    x@data = x@data %>%
      dplyr::select(c(.uID, x, y, i))
    x
  }
)
# numeric input subsets by row index
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPointsProxy', i = 'numeric', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x = .reconnect(x)
    x@data = x@data %>%
      dplyr::filter(.uID %in% i)
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPointsProxy', i = 'logical', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x = .reconnect(x)
    uIDs = x@data %>%
      dplyr::pull(.uID)
    bool_vect = uIDs[i]
    x@data = x@data %>%
      dplyr::filter(.uID %in% bool_vect)
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPolygonProxy', i = 'numeric', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x <- .reconnect(x)
    x@data <- x@data %>%
      dplyr::filter(geom %in% i)
    x@attributes[] <- x@attributes[] %>%
      dplyr::filter(geom %in% i)
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPolygonProxy', i = 'logical', j = 'missing', drop = 'ANY'),
  function(x, i, ..., drop = FALSE) {
    x <- .reconnect(x)
    geomIDs <- x@attributes[] %>%
      dplyr::pull(geom)
    bool_vect <- geomIDs[i]
    x@data <- x@data %>%
      dplyr::filter(geom %in% bool_vect)
    x@attributes[] <- x@attributes[] %>%
      dplyr::filter(geom %in% bool_vect)
    x
  }
)
## cols only ####
# character input subsets the attribute table
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPolygonProxy', i = 'missing', j = 'character', drop = 'ANY'),
  function(x, j, ..., drop = FALSE) {
    x = .reconnect(x)
    x@attributes@data = x@attributes@data %>%
      dplyr::select(c(geom, dplyr::all_of(j)))
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPolygonProxy', i = 'missing', j = 'numeric', drop = 'ANY'),
  function(x, j, ..., drop = FALSE) {
    x = .reconnect(x)
    sel_cols = names(x)[j]
    x@attributes@data = x@attributes@data %>%
      dplyr::select(c(geom, dplyr::all_of(sel_cols)))
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPolygonProxy', i = 'missing', j = 'character', drop = 'ANY'),
  function(x, j, ..., drop = FALSE) {
    x = .reconnect(x)
    sel_cols = names(x)[j]
    x@attributes@data = x@attributes@data %>%
      dplyr::select(c(geom, dplyr::all_of(sel_cols)))
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPointsProxy', i = 'missing', j = 'character', drop = 'ANY'),
  function(x, j, ..., drop = FALSE) {
    x = .reconnect(x)
    x@data = x@data %>%
      dplyr::select(c(.uID, x, y, dplyr::all_of(j)))
    x
  }
)
# numeric input subsets by row index
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPointsProxy', i = 'missing', j = 'numeric', drop = 'ANY'),
  function(x, j, ..., drop = FALSE) {
    x = .reconnect(x)
    sel_cols = names(x)[j]
    x@data = x@data %>%
      dplyr::select(.uID, x, y, dplyr::all_of(sel_cols))
    x
  }
)
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbPointsProxy', i = 'missing', j = 'logical', drop = 'ANY'),
  function(x, j, ..., drop = FALSE) {
    x = .reconnect(x)
    sel_cols = names(x)[j]
    x@data = x@data %>%
      dplyr::select(.uID, x, y, dplyr::all_of(sel_cols))
    x
  }
)

## rows and cols ####
# i char / j char NOT DEFINED
#
#' @rdname hidden_aliases
#' @export
setMethod(
  '[', signature(x = 'dbSpatProxyData', i = 'gdbIndexNonChar', j = 'gdbIndex', drop = 'ANY'),
  function(x, i, j, ..., drop = FALSE) {
    x = .reconnect(x)
    x = x[, j, ..., drop]
    x = x[i, , ..., drop]
    x
  }
)



## $ ####

#' @rdname hidden_aliases
setMethod('$', signature('dbPointsProxy'), function(x, name) {
  x <- .reconnect(x)
  x[] %>%
    dplyr::arrange(.uID) %>%
    dplyr::pull(!!name)
})
#' @rdname hidden_aliases
setMethod('$', signature('dbPolygonProxy'), function(x, name) {
  x <- .reconnect(x)
  x@attributes[] %>%
    dplyr::arrange(geom) %>%
    dplyr::pull(!!name)
})

## $<- ####
#
# setMethod('$<-', signature('dbPointsProxy'), function(x, name, value) {
#   x <- .reconnect(x)
#   p = cPool(x)
#   conn <- evaluate_conn(p, mode = 'conn')
#   on.exit(pool::poolReturn(conn), add = TRUE)
#   cPool(x) <- conn
#
#   n_row <- nrow(x)
#   if (length(value) != n_row) {
#     if (n_row %% length(value) != 0 ||
#         n_row < length(value)) {
#       warning(GiottoUtils::wrap_txt(
#         'Number of geometries is not a multiple of length of attributes to assign.'
#       ))
#     }
#
#     value <- rep(value, length.out = n_row)
#   }
#
#   assign_dfr <- data.frame(
#     idx = seq_along(value),
#     value = value
#   )
#
#   # TODO
#
#   x[] <- x[] %>%
#     dplyr::mutate(!!name := value)
#
#   cPool(x) <- p
#   return(x)
# })


