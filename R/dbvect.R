# collate
#' @include dbSpatProxy.R
NULL


# docs ####



# * dbvect ####

#' @name dbvect
#' @title Create a framework for chunked processing with terra SpatVectors
#' @description
#' Create an S4 dbPointsProxy object
#' @param x data.frame or terra SpatVector
#' @param db filepath or hash ID of existing DB backend or pool connection. Also
#' accepts :temp: token for `tempdir()`.
#' @param remote_name name of table to use in backend
#' @param overwrite logical for whether to overwrite if table already exists.
#' Alternatively, pass 'append' to add to existing table.
#' @param \dots additional params to pass to table creation and `stream_to_db`
#' @examples
#' \dontrun{
#' createBackend()
#' ### SpatVector from file
#' f <- system.file("ex/lux.shp", package="terra")
#' f
#' v <- vect(f)
#'
#' dbpoly <- dbvect(v, overwrite = TRUE)
#' }
#'
#' ## Appending geometries
#' nrow(dbpoly)
#' dbpoly <- dbvect(v, overwrite = "append")
#' nrow(dbpoly)
#'
NULL


# methods ####
# data.frame #

#' @rdname dbvect
#' @param geom character. The field name(s) with the geometry data. Either two
#' names for x and y coordinates of points, or a single name for a single column
#' with WKT geometries)
#' @export
setMethod(
  'dbvect', signature(x = 'data.frame'),
  function(
    x, db = ':temp:', remote_name = 'dbv_test',
    type = 'points', geom = c('x', 'y'), ...
  )
  {
    type = match.arg(type, choices = c('points', 'polygons'))

    switch(
      type,
      'points' = dbvect_points_df(x, db, remote_name, geom = geom, ...),
      'polygons' = dbvect_poly_df(x, db, remote_name, geom = geom, ...)
    )
  })

# SpatVector #

#' @rdname dbvect
#' @param atts data.frame with the attributes. The number of rows must match the
#' number of geometrical elements
#' @param include_values (SpatVector input only) whether to include attribute
#' values in output
#' @export
setMethod(
  'dbvect', signature(x = 'SpatVector'),
  function(
    x, db = ':temp:', remote_name = 'dbv_test', ...
  )
  {
    type <- terra::geomtype(x)

    switch(
      type,
      'points' = dbvect_points_sv(x, db, remote_name, ...),
      'polygons' = dbvect_poly_sv(x, db, remote_name, ...)
    )
  })


# character (filepaths) #

# Employs stream_to_db with chunked reading for large files
#' @rdname dbvect
#' @param read_fun function. Controls how information should be read. Should
#' expose formal arguments 'x', 'n', and 'callback'
#' @param n numeric. number of units (usually rows) to read per chunk
#' @param stop_cond function. Function to run on each chunk. Will stop the chunk
#' read looping when evaluates to TRUE. Default provided function stops the read
#' looping when a chunk that has been read in has 0 rows.
#' @param callback (optional) function. Single param function that will take a
#' chunk as input, then modify it into the format expected by the 'write_fun'
#' and db representation
#' @param verbose be verbose
#' @param log_to directory path. Create a logfile in this directory that logs
#' the progress of the chunked read. Any specified directory must exist.
#' etting 'NULL' logs to the same directory as the database.
#' @param report_n_chunks if 'verbose = TRUE', report in the console every n
#' chunks that are processed.
#' @export
setMethod(
  'dbvect', signature(x = 'character'),
  function
  (
    x,
    db = ':temp:',
    remote_name = 'dbv_test',
    type = 'points',
    geom = c('x', 'y'),
    read_fun,
    n = 10L,
    stop_cond = function(x) nrow(x) == 0L, # stop when a chunk has 0 rows
    callback = NULL,
    verbose = TRUE,
    log_to = NULL,
    report_n_chunks = 10L,
    ...
  )
  {
    p = evaluate_conn(db, 'pool')

    # read values from file and write to DB using dbvect
    # chunked reads and writes will be performed.
    #
    # How `stream_to_db` will deal with overwrite inputs:
    # [FALSE] - Throw error if table already exists when writing the first chunk
    #   During subsequent chunks, overwrite will be internally set to 'append'
    # ['append'] - Table will be created if it does not exist in first round.
    #   Subsequent chunks will be appended
    # [TRUE] - Table will be overwritten if it already exists in first round.
    #   Subsequent chunks will be appended
    #
    # `write_fun` controlled:
    # ... params
    # updating of .uID or geom cols happens inside dbvect depending on whether
    # table named by remote_name already exists and overwrite param value

    # declare writer function #
    # # --------------------- #
    # p, remote_name, x and ... params are the required params for all writer
    # functions used with stream_to_db
    dbvect_writer = function(p, remote_name, x, ...) {
      # dbvect output objects are discarded during this process
      dbvect(
        x = x,
        db = p,
        remote_name = remote_name,
        ...,
        type = type, #                       pull from this stack frame
        geom = geom, #                       pull from this stack frame
        read_fun = read_fun, #               pull from this stack frame
        n = n, #                             pull from this stack frame
        stop_cond = stop_cond, #             pull from this stack frame
        callback = callback, #               pull from this stack frame
        verbose = verbose, #                 pull from this stack frame
        log_to = log_to, #                   pull from this stack frame
        report_n_chunks = report_n_chunks #  pull from this stack frame
      )
      return(NULL)
    }

    stream_to_db(
      p = p,
      remote_name = remote_name,
      x = x,
      write_fun = dbvect_writer,
      ...
    )

    # create object to return
    res <- switch(
      type,
      'points' = {
        # create and return dbpoints object
        # INIT: when following values are NULL:
        # [data] table attached during init
        new(
          'dbPointsProxy',
          data = NULL,
          hash = evaluate_conn(p, mode = 'id'),
          remote_name = remote_name
        )
      },
      'polygons' = {
        # create and return dbpoly object
        # INIT: when following values are NULL:
        # [data] table attached during init
        # [attributes] table attached during init
        # [extent] values are calculated during init
        new(
          'dbPolygonProxy',
          data = NULL,
          attributes = NULL,
          hash = evaluate_conn(p, mode = 'id'),
          remote_name = remote_name,
          n_poly = sql_max(p, attr_name, 'geom')
        )
      }
    )
    return(res)
  }
)





# method internals ####
dbvect_poly_df <- function(
    x,
    db = ':temp:',
    remote_name = 'poly_test',
    atts = NULL,
    geom = NULL, # expected WKT col
    overwrite = FALSE,
    ...
) {
  p <- evaluate_conn(db, mode = 'pool')
  if (!is.null(geom)) checkmate::assert_character(geom)
  checkmate::assert_character(remote_name, len = 1L)
  x <- data.table::as.data.table(x)
  if (!is.null(atts) && !is.null(geom)) {
    stopf("dbvect: When poly geometry info is provided as WKT, atts should be provided as part of the same dataframe.")
  }

  # get attribute table name
  attr_name = paste0(remote_name, '_attr')

  # organize input as geom and atts info
  col_n = colnames(x)

  # If 'geom' param not null, WKT is expected
  # Otherwise, expect geom, x, y, part, hole to be cols
  if (!is.null(geom)) { ### WKT workflow ###
    x <- terra::vect(x, geom = geom)
    # get geom info
    geom_dt <- terra::geom(x, df = TRUE) %>%
      data.table::setDT()
    # get atts info (only allow atts input as part of same table)
    atts_dt <- terra::values(x) %>%
      data.table::setDT()

  } else { ### geom 5 cols workflow ###
    # get geom info
    geom_cols <- c('geom', 'x', 'y', 'part', 'hole')
    # throw error if any geom cols missing
    if (!all(geom_cols %in% col_n)) {
      stopf("dbvect: if WKT column not supplied to geom param,
            'geom', 'x', 'y', 'part', 'hole' cols are expected.")
    }
    geom_dt <- x[, c(geom_cols), with = FALSE]

    # if 'atts' not provided, use remaining cols as attributes
    # otherwise, use atts as the atts table
    if (is.null(atts)) {
      atts_cols <- col_n[!col_n %in% geom_cols]
      # get only the uniques since there are identical atts for each vertex of
      # the same poly
      atts_dt <- unique(x[, c(atts_cols), with = FALSE])
    } else {
      atts_dt <- data.table::as.data.table(atts)
    }
  }

  # assign common geom col to atts table
  n_atts <- nrow(atts)
  geom_ids <- geom_dt[, unique(geom)]
  if (n_atts != length(geom_ids)) stopf('dbvect: number of poly attributes do not match number of geometries')
  atts_dt[, geom := geom_ids]

  # append considerations
  if (overwrite == 'append') {
    # Assign start index if table does not exist yet
    # Otherwise, ensure that geom index respects the values already existing
    if(!existsTableBE(x = p, remote_name = remote_name)) {
      start_index = 0L
    } else {
      start_index = sql_max(p, attr_name, 'geom')
    }

    # increment indices
    geom_dt[, geom := geom + start_index]
    atts_dt[, geom := geom + start_index]
  }

  # write values to DB
  stream_to_db( # write geom
    p = p,
    remote_name = remote_name,
    x = geom_dt,
    overwrite = overwrite,
    custom_table_fields = fields_preset$dbPoly_geom,
    ...
  )
  stream_to_db( # write atts
    p = p,
    remote_name = attr_name,
    x = atts_dt,
    overwrite = overwrite,
    ...
  )

  # create and return dbpoly object
  # INIT: when following values are NULL:
  # [data] table attached during init
  # [attributes] table attached during init
  # [extent] values are calculated during init
  new(
    'dbPolygonProxy',
    data = NULL,
    attributes = NULL,
    hash = evaluate_conn(p, mode = 'id'),
    remote_name = remote_name,
    n_poly = sql_max(p, attr_name, 'geom')
  )
}


dbvect_poly_sv <- function(
    x,
    db = ':temp:',
    remote_name = 'poly_test',
    overwrite = FALSE,
    include_values = TRUE,
    ...
)
{
  attr_name = paste0(remote_name, '_attr')
  # get geometry info
  # if no attributes, expect a null dt with 0 rows/cols
  geom_dt = data.table::as.data.table(terra::geom(x))

  if(isTRUE(include_values) && nrow(geom_dt) > 0L) {
    # get attributes info
    # if no attributes, expect a null dt with 0 rows/cols
    atts_dt = data.table::as.data.table(terra::values(x))
  } else {
    # create a new NULL dt
    atts_dt = data.table::data.table()
  }

  # create matching geom col from seq of maximum geom val in geom_dt
  # (works without issue for NULL dts)
  atts_dt[, geom := seq(geom_dt[, max(geom)])]

  # send geom and atts DTs to dbvect_poly_df
  dbvect_poly_df(
    x = geom_dt,
    db = db,
    remote_name = remote_name,
    geom = NULL, # require geom 5 col workflow
    atts = atts_dt,
    overwrite = overwrite,
    ...
  )
}



dbvect_points_df = function(
    x,
    db = ':temp:',
    remote_name = 'pnt_test',
    geom = c('x', 'y'),
    overwrite = FALSE,
    ...
)
{
  p <- evaluate_conn(db, mode = 'pool')
  checkmate::assert_character(geom)
  checkmate::assert_character(remote_name, len = 1L)
  x <- data.table::as.data.table(x)

  # coerce to xy data.table with .uID col
  geom_len = length(geom) # discern if xy or wkt input
  if (geom_len == 1L) { # WKT
    x <- x %>%
      terra::vect(geom = geom) %>% # have terra handle WKT
      svpoint_to_dt() # convert to xy dt and add .uID col
  } else if(geom_len == 2L) { # xy data.table
    col_n <- colnames(x)
    if (any(".uID" %in% col_n)) {
      stopf("dbvect: .uID is a reserved column name.
            Please edit or remove .uID from the input data.frame")
    }
    if (!all(c("x", "y") %in% geom) &&
        all(c("x", "y") %in% col_n)) {
      stopf("dbvect: Not allowed for geom param input to be other than 'x' and 'y',
            when 'x' and 'y' columns to already exist")
    }
    x <- data.table::setnames(x, old = geom, new = c("x", "y"))
    x[, .uID := seq(.N)]
  } else {
    stopf("dbvect: geom param only allows characters of length either 2 (xy) or 1 (wkt)")
  }

  # if append, update the .uID
  if (overwrite == 'append') {
    # Assign start index if table does not exist yet
    # Otherwise, ensure that .uID respects the values already existing
    if (!existsTableBE(x = p, remote_name = remote_name)) {
      start_index = 0L
    } else {
      start_index = sql_max(p, remote_name = remote_name, '.uID')
    }

    # increment .uID index
    x[, .uID := .uID + start_index]
  }

  # write values to database
  stream_to_db(p, remote_name = remote_name, x = x, overwrite = overwrite, ...)

  # open the written table
  geom_table <- tableBE(cPool = p, remote_name = remote_name)

  # create and return dbpoints object
  new(
    'dbPointsProxy',
    data = geom_table,
    hash = evaluate_conn(p, mode = 'id'),
    remote_name = remote_name
  )
}




dbvect_points_sv <- function(
    x,
    db = ':temp:',
    remote_name = 'pnt_test',
    overwrite = FALSE,
    include_values = TRUE,
    ...
)
{
  if (isTRUE(include_values)) {
    DT_values = cbind(terra::crds(x), terra::values(x)) %>%
      data.table::setDT()
  }
  else {
    DT_values = terra::crds(x) %>%
      data.table::as.data.table()
  }
  dbvect_points_df(x = DT_values,
                   db = db,
                   geom = c('x', 'y'),
                   remote_name = remote_name,
                   overwrite = overwrite,
                   ...)
}
