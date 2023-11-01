



#' @title Read matrix via R and data.table
#' @name readMatrixDT
#' @description Function to read a matrix in from flat file via R and data.table
#' @param path path to the matrix file
#' @param cores number of cores to use
#' @param transpose transpose matrix
#' @return matrix
#' @details The matrix needs to have both unique column names and row names
#' @export
readMatrixDT = function(path,
                       cores = 1L,
                       transpose = FALSE) {

  # check if path is a character vector and exists
  if(!is.character(path)) stopf('path needs to be character vector')
  path = path.expand(path)
  if(!file.exists(path)) stopf('the file: ', path, ' does not exist')


  data.table::setDTthreads(threads = cores)

  # read and convert
  DT = suppressWarnings(data.table::fread(input = path, nThread = cores))
  mtx = as.matrix(DT[,-1])

  if(isTRUE(transpose)) {
    mtx = Matrix::Matrix(data = mtx,
                         dimnames = list(DT[[1]], colnames(DT[,-1])),
                         sparse = TRUE)
    mtx = Matrix::t(mtx)
  }

  return(mtx)
}


# Other similar functions can be easily added by swapping out the repeat loop and
# colnames collection sections










#' @name streamToDB_fread
#' @title Stream large flat files to database backend using fread
#' @description
#' Files are read in chunks of lines via \code{fread} and then converted to the
#' required formatting with plugin functions provided through the \code{callback}
#' param before being written/appended to the database table.
#' This is slower than directly writing the information in, but is a scalable
#' approach as it never requires the full dataset to be in memory. \cr
#' If more than one or a custom callback is needed for the formatting then a
#' combined or new function can be defined on the spot as long as it accepts
#' \code{data.table} input and returns a \code{data.table}.
#' @param path path to the matrix file
#' @param backend_ID ID of the backend to use
#' @param remote_name name to assign table of read in values in database
#' @param idx_col character. If not NULL, the specified column will be generated
#' as unique ascending integers
#' @param pk character. Which columns to use as primary key
#' @param nlines integer. Number of lines to read per chunk
#' @param cores fread cores to use
#' @param callback callback functions to apply to each data chunk before it is
#' sent to the database backend
#' @param overwrite whether to overwrite if table already exists (default = FALSE)
#' @param custom_table_fields default = NULL. If desired, custom setup the fields
#' of the table. See \code{\link[DBI]{dbCreateTable}}
#' @param ... additional params to pass to fread
#' @export
streamToDB_fread = function(path,
                            backend_ID,
                            remote_name = 'test',
                            idx_col = NULL,
                            pk = NULL,
                            nlines = 10000L,
                            cores = 1L,
                            callback = NULL,
                            overwrite = FALSE,
                            custom_table_fields = NULL,
                            ...) {
  checkmate::assert_file_exists(path)
  checkmate::assert_character(remote_name)
  checkmate::assert_numeric(nlines, len = 1L)
  if(!is.integer(nlines)) nlines = as.integer(nlines)
  if(!is.null(idx_col)) checkmate::assert_character(idx_col, len = 1L)
  if(!is.null(custom_table_fields)) stopifnot(is.character(custom_table_fields))
  p = evaluate_conn(backend_ID, mode = 'pool')

  # overwrite if necessary
  overwrite_handler(p = p, remote_name = remote_name, overwrite = overwrite)

  # get rows
  fext = file_extension(path)
  if('csv' %in% fext) file_format = 'csv'
  if('tsv' %in% fext) file_format = 'tsv'
  atab = arrow::open_dataset(path, format = file_format)
  n_rows = nrow(atab)

  # chunked reading
  chunk_num = idx_count = 0
  c_names_cache = colnames(data.table::fread(input = path, nrows = 0L))
  idx_list = NULL
  extab = FALSE
  repeat {
    chunk_num = chunk_num + 1
    nskip = (chunk_num - 1) * nlines + 1
    if(nskip >= n_rows) {
      # end of file
      break
    }

    chunk = data.table::fread(input = path,
                  nrows = nlines,
                  skip = nskip,
                  header = FALSE,
                  nThread = cores,
                  ...)

    # apply colnames
    data.table::setnames(chunk, new = c_names_cache)
    # apply callbacks
    if(!is.null(callback)) {
      stopifnot(is.function(callback))
      chunk = callback(chunk)
    }

    # index col generation
    if(!is.null(idx_col)) {
      if(idx_col %in% names(chunk)) {
        warning('idx_col already exists as a column.\n The unique index will not be generated')
        idx_col = NULL # set to null so warns only once
      } else {
        chunk[, (idx_col) := seq(.N) + idx_count]
        idx_count = chunk[, max(.SD), .SDcols = idx_col]
        data.table::setcolorder(chunk, idx_col)
      }
    }

    if(!extab) {
      # custom table creation
      # allows setting of specific data types (that do not conflict with data)
      # allows setting of keys and certain constraints
      createTableBE(
        conn = p,
        name = remote_name,
        fields_df = chunk,
        fields_custom = custom_table_fields,
        pk = pk,
        row.names = NULL,
        temporary = FALSE
      )
      extab = TRUE
    }

    # append data (create if necessary)
    pool::dbAppendTable(conn = p,
                        name = remote_name,
                        value = chunk)
  }

  return(invisible(n_rows))
}




# possible read methods: arrow, vect, rhdf5
#' @name streamSpatialToDB_arrow
#' @title Stream spatial data to database (arrow)
#' @param path path to database
#' @param backend_ID backend ID of database
#' @param remote_name name of table to generate in database backend
#' @param id_col ID column of data
#' @param xy_col character vector. Names of x and y value spatial coordinates or
#' vertices
#' @param extent terra SpatExtent (optional) that can be used to subset the data
#' to read in before it is saved to database
#' @param file_format (optional) file type (one of csv/tsv, arrow, or parquet)
#' @param chunk_size chunk size to use when reading in data
#' @param callback callback function to allow access to read-chunk-level data
#' formatting and filtering. Input and output should both be data.table
#' @param overwrite logical. whether to overwrite if table already exists in
#' database backend
#' @param custom_table_fields default = NULL. If desired, custom setup the fields
#' of the table. See \code{\link[DBI]{dbCreateTable}}
#' @param custom_table_fields_attr default = NULL. If desired, custom setup the
#' fields of the paired attributes table
#' @return invisibly returns the final geom ID used
streamSpatialToDB_arrow = function(path,
                                   backend_ID,
                                   remote_name = 'test',
                                   id_col = 'poly_ID',
                                   xy_col = c('x', 'y'),
                                   extent = NULL,
                                   file_format = NULL,
                                   chunk_size = 10000L,
                                   callback = NULL,
                                   overwrite = FALSE,
                                   custom_table_fields = NULL,
                                   custom_table_fields_attr = NULL,
                                   ...) {
  checkmate::assert_file_exists(path)
  checkmate::assert_character(remote_name, len = 1L)
  checkmate::assert_character(backend_ID, len = 1L)
  checkmate::assert_character(id_col, len = 1L)
  checkmate::assert_character(xy_col, len = 2L)
  checkmate::assert_numeric(chunk_size, len = 1L)
  if(!is.null(custom_table_fields)) checkmate::assert_character(custom_table_fields)
  if(!is.null(custom_table_fields_attr)) checkmate::assert_character(custom_table_fields_attr)

  p = getBackendPool(backend_ID = backend_ID)
  attr_name = paste0(remote_name, '_attr')

  # overwrite if necessary
  overwrite_handler(p = p, remote_name = remote_name, overwrite = overwrite)
  overwrite_handler(p = p, remote_name = attr_name, overwrite = overwrite)

  # custom table creation
  # allows setting of specific data types (that do not conflict with data)
  # allows setting of keys and certain constraints
  if(!is.null(custom_table_fields)) {
    DBI::dbCreateTable(
      conn = p,
      name = remote_name,
      fields = custom_table_fields,
      row.names = NA,
      temporary = FALSE
    )
  }
  if(!is.null(custom_table_fields_attr)) {
    DBI::dbCreateTable(
      conn = p,
      name = attr_name,
      fields = custom_table_fields_attr,
      row.names = NA,
      temporary = FALSE
    )
  }

  # determine compatible filetype
  if(is.null(file_format)) {
    fext = file_extension(path)
    if('csv' %in% fext) file_format = 'csv'
    if('tsv' %in% fext) file_format = 'tsv'
    if(any(c('pqt', 'parquet') %in% fext)) file_format = 'parquet'
    if(any(c('ipc', 'arrow', 'feather') %in% fext)) file_format = 'arrow'
  }


  # create file connection and get details
  x_col = xy_col[1]
  y_col = xy_col[2]
  atable = arrow::open_dataset(sources = path, format = file_format)
  # rename columns to standard names
  atable = atable %>%
    dplyr::rename(c(poly_ID = !!id_col,
                    x = !!x_col,
                    y = !!y_col))
  peek = atable %>% head(1) %>% dplyr::collect()
  c_names = colnames(peek)
  # include terra-needed columns
  if(!'part' %in% c_names) atable = atable %>% dplyr::mutate(part = 1L)
  if(!'hole' %in% c_names) atable = atable %>% dplyr::mutate(hole = 0L)

  # extent filtering
  if(!is.null(extent)) {
    atable = extent_filter(atable, extent = extent)
  }

  # chunked reading
  chunk_num = 0L
  pIDs = dplyr::distinct(atable, poly_ID) %>%
    dplyr::arrange(poly_ID) %>%
    dplyr::collapse()
  npoly = dplyr::pull(dplyr::tally(pIDs), as_vector = TRUE)
  repeat{

    chunk_num = chunk_num + 1L
    chunk_start = (chunk_num - 1L) * chunk_size + 1L
    chunk_end = min(chunk_size + chunk_start - 1L, npoly)
    if(chunk_start > npoly) {
      # end of file
      break
    }
    poly_select = pIDs[chunk_start:chunk_end,] %>%
      dplyr::collect() %>%
      dplyr::pull()

    chunk = atable %>%
      dplyr::filter(poly_ID %in% poly_select) %>%
      dplyr::collect() %>%
      data.table::setDT() %>%
      data.table::setkeyv('poly_ID')

    # callbacks
    if(!is.null(callback)) {
      checkmate::assert_function(callback)
      chunk = callback(chunk)
    }

    # setup geom column (polygon unique integer ID)
    nr_of_cells_vec = seq_along(poly_select) + chunk_start - 1L
    names(nr_of_cells_vec) = poly_select
    new_vec = nr_of_cells_vec[as.character(chunk$poly_ID)]
    chunk[, geom := new_vec]

    all_colnames = colnames(chunk)
    geom_values = c('geom', 'part', 'x', 'y', 'hole')
    attr_values = c('geom', all_colnames[!all_colnames %in% geom_values])

    spat_chunk = chunk[,geom_values, with = FALSE]
    attr_chunk = unique(chunk[,attr_values, with = FALSE])

    # append data (create if necessary)
    pool::dbWriteTable(conn = p,
                       name = remote_name,
                       value = spat_chunk,
                       append = TRUE,
                       temporary = FALSE)
    pool::dbWriteTable(conn = p,
                       name = attr_name,
                       value = attr_chunk,
                       append = TRUE,
                       temporary = FALSE)
  }
  return(invisible(max(nr_of_cells_vec)))
}





# chunk reading callbacks ####

#' @name callback_combineCols
#' @title Combine columns values
#' @param x data.table
#' @param col_indices numeric vector. Col indices to combine
#' @param new_col name of new combined column
#' @param remove_originals remove originals cols of the combined col
#' default = TRUE
#' @return data.table with specified column combined
#' @export
callback_combineCols = function(x,
                                col_indices,
                                new_col = 'new',
                                remove_originals = TRUE) {
  if(missing(col_indices)) return(x)
  assert_DT(x)

  selcol = colnames(x)[col_indices]
  x[, (new_col) := do.call(paste, c('ID', .SD, sep = '_')), .SDcols = selcol]
  data.table::setcolorder(x, new_col)
  if(isTRUE(remove_originals)) {
    if(new_col %in% selcol) selcol = selcol[-which(selcol == new_col)]
    x[, (selcol) := NULL]
  }

  return(x)
}




#' @name callback_formatIJX
#' @title Convert table to ijx
#' @description
#' Building block function intended for use as or in callback functions used
#' when streaming large flat files to database. Converts the input data.table
#' to long format with columns i, j, and x where i and j are row and col
#' names and x is values. Columns i and j are additionally set as 'character'.
#' @param x data.table
#' @param group_by numeric or character designating which column of current data
#' to set as i. Default = 1st column
#' @return data.table in ijx format
#' @export
callback_formatIJX = function(x, group_by = 1) {
  assert_DT(x)

  x = data.table::melt(data = x, group_by, value.name = 'x')
  data.table::setnames(x, new = c('i', 'j', 'x'))
  x[, i := as.character(i)]
  x[, j := as.character(j)]
  x[, x := as.numeric(x)]
  return(x)
}

#' @name callback_swapCols
#' @title Swap values in two columns
#' @param x data.table
#' @param c1 col 1 to use (character)
#' @param c2 col 2 to use (character)
#' @return data.table with designated column values swapped
#' @export
callback_swapCols = function(x, c1, c2) {
  assert_DT(x)
  if(identical(c1, c2)) stop('Cols to swap can not be identical')

  x[, c(c2, c1) := .(get(c1), get(c2))]
  return(x)
}




# Old
# streamToDB = function(path,
#                       backend_ID,
#                       remote_name = 'read_temp',
#                       ...) {
#   stopifnot(is.character(remote_name))
#   stopifnot(is.character(backend_ID))
#   stopifnot(file.exists(path))
#
#   conn = getBackendConn(backend_ID = backend_ID)
#   on.exit(pool::poolReturn(conn))
#
#   if(match_file_ext(path = path, ext_to_match = '.csv')) { # .csv
#     if(dbms(conn) == 'duckdb') {
#       duckdb::duckdb_read_csv(conn = conn,
#                               name = remote_name,
#                               files = path,
#                               delim = ',',
#                               header = TRUE,
#                               ...)
#     } else {
#       arkdb::unark(files = path,
#                    db_con = conn,
#                    tablenames = remote_name,
#                    overwrite = TRUE,
#                    ...)
#     }
#
#   } else if(match_file_ext(path = path, ext_to_match = '.tsv')) { # .tsv
#
#     arkdb::unark(files = path,
#                  db_con = conn,
#                  tablenames = remote_name,
#                  overwrite = TRUE,
#                  ...)
#
#   } else { # else throw error
#     stopf('readMatrixDB only works for .csv and .tsv type files
#           Not:', basename(path))
#   }
# }







# TODO read .mtx format (uses dictionaries and has no 0 values. Already in ijx)

# TODO read .parquet (arrow::read_parquet(), arrow::to_duckdb(), tidyr::pivot_longer())




# match_file_ext = function(path, ext_to_match) {
#   stopifnot(file.exists(path))
#
#   grepl(pattern = paste0(ext_to_match, '$|', ext_to_match, '\\.'),
#         x = path,
#         ignore.case = TRUE)
# }








# data appending ####
# TODO




# @name writeAsGDB
# @title Write an object into the database backend as a GiottoDB object
# @description
# Given a matrix, data.frame-like object, or SpatVector polygon or points,
# write the values to database. Supports appending of information in case this
# data should be added iteratively into the database (such as when operations
# are parallelized). Returns a GiottoDB object of the analogous class.
# @param backend_ID backend_ID, or pool object with which to associate this object
# to a specific database backend
# @param x object to write
# @param append whether to append the values
# @param remote_name name to assign in the database
# @param ... additional params to pass
# setMethod('writeAsGDB', signature('data.frame'), function(backend_ID, x, append = FALSE, ...) {
#   p = evaluate_conn(backend_ID, mode = 'pool')
#   checkmate::assert_logical(append)
#
#   dbDataFrame(key = ,data = ,hash = ,remote_name = )
# })






# @name append_permanent
# @title Append values to database
# @description
# Writes values to database in an appending manner. As such, it does not check
# if there is already an existing table.
# @param p Pool connection object
# @param x data to append, given as a data.frame-like object
# @param remote_name name to assign the object on the database
# @param data_type type of data that is being appended to
# @param fields additional fields constraints that might be desired if the table
# is being newly generated
# @param ... additional params to pass to \code{\link[DBI]{dbCreateTable}}
# append_permanent = function(p,
#                             x,
#                             remote_name,
#                             data_type = c('matrix', 'df', 'polygon', 'points'),
#                             fields = NULL,
#                             ...) {
#   checkmate::assert_class(p, 'Pool')
#   checkmate::assert_character(remote_name)
#   checkmate::assert_character(data_type)
#
#   # create table if fields constraints are provided and table does not exist
#   if(!is.null(fields) & !existsTableBE(p, remote_name)) {
#     switch(data_type,
#            'matrix' = {
#              pool::dbCreateTable(conn = p,
#                                  name = remote_name,
#                                  fields = fields,
#                                  row.names = NA,
#                                  temporary = FALSE)
#            },
#            'df' = {
#              pool::dbCreateTable(conn = p,
#                                  name = remote_name,
#                                  fields = fields,
#                                  row.names = NA,
#                                  temporary = FALSE)
#            },
#            'polygon' = {
#              pool::dbCreateTable(conn = p,
#                                  name = remote_name,
#                                  fields = fields[[1L]],
#                                  row.names = NA,
#                                  temporary = FALSE)
#              if(!is.null(fields[[2L]])) {
#                pool::dbCreateTable(conn = p,
#                                    name = paste0(remote_name, '_attr'),
#                                    fields = fields[[2L]],
#                                    row.names = NA,
#                                    temporary = FALSE)
#              }
#            },
#            'points' = {
#              pool::dbCreateTable(conn = p,
#                                  name = remote_name,
#                                  fields = fields,
#                                  row.names = NA,
#                                  temporary = FALSE)
#            })
#   }
#
#   # append in data
#   # assumes input is from terra::geom() and terra::values()
#   switch(data_type,
#          'matrix' = {
#
#          },
#          'df' = {
#          },
#          'polygon' = {
#
#          },
#          'points' = {
#          })
# }












append_permanent_dbpoly = function(p,
                                   SpatVector,
                                   remote_name,
                                   start_index = NULL,
                                   custom_table_fields = NULL,
                                   custom_table_fields_attr = NULL,
                                   ...) {
  checkmate::assert_class(p, 'Pool')
  checkmate::assert_character(remote_name, len = 1L)
  checkmate::assert_class(SpatVector, 'SpatVector')
  checkmate::assert_true(terra::geomtype(SpatVector) == 'polygons')
  if(!is.null(custom_table_fields)) checkmate::assert_character(custom_table_fields)
  if(!is.null(custom_table_fields_attr)) checkmate::assert_character(custom_table_fields_attr)


  # get attribute table name
  attr_name = paste0(remote_name, '_attr')

  # assign start index
  if(!existsTableBE(x = p, remote_name = remote_name)) {
    start_index = 0L
  } else {
    start_index = sql_max(p, attr_name, 'geom')
  }


  # extract info from terra SpatVector as DT
  geom_DT = terra::geom(SpatVector, df = TRUE) %>%
    data.table::setDT()
  atts_DT = terra::values(SpatVector) %>%
    data.table::setDT()
  # assign common geom col to atts table
  atts_DT[, geom := unique(geom_DT$geom)]

  # increment indices
  geom_DT[, geom := geom + start_index]
  atts_DT[, geom := geom + start_index]


  # append/write info
  stream_to_db(
    p = p,
    remote_name = remote_name,
    x = geom_DT,
    custom_table_fields = custom_table_fields,
    ...
  )
  stream_to_db(
    p = p,
    remote_name = remote_name,
    x = atts_DT,
    custom_table_fields = custom_table_fields_attr,
    ...
  )
}



# TODO mostly replaced by dbvect
# Append information from a terra SpatVector points to the db
append_permanent_dbpoints = function(p,
                                     SpatVector,
                                     remote_name,
                                     custom_table_fields = NULL,
                                     ...) {
  checkmate::assert_class(p, 'Pool')
  checkmate::assert_character(remote_name, len = 1L)
  checkmate::assert_class(SpatVector, 'SpatVector')
  checkmate::assert_true(terra::geomtype(SpatVector) == 'points')
  if(!is.null(custom_table_fields)) checkmate::assert_character(custom_table_fields)

  last_uid <- sql_max(p, remote_name = remote_name, col = '.uID')

  # extract info from terra SpatVector as DT
  geom_DT <- data.table::setDT(terra::geom(SpatVector, df = TRUE))
  atts_DT <- data.table::setDT(terra::values(SpatVector))
  chunk <- cbind(geom_DT[, .(x, y)], atts_DT)
  chunk[, .uID := last_uid + seq(.N)] # assign unique IDs

  stream_to_db(
    p = p,
    remote_name = remote_name,
    x = chunk,
    custom_table_fields = custom_table_fields,
    ...
  )
}






# stream_to_db methods ####

#' @name stream_to_db
#' @title Stream data and write to DB
#' @description
#' Stream specific information into the database backend, whether for initial
#' data ingestion or for chunked processes.\cr
#' if custom_table_fields is provided and the table does not yet exist then
#' a specific DB table will be created with constraints. In all cases,
#' values will be fed to `dbWriteTable` with `append = TRUE` so that the values
#' will be appended, and if a specific table does not yet exist, it will be created.
#' @param p connection info or object (must be coercible to pool through
#' [evaluate_conn])
#' @param remote_name character. name of table in database to write to.
#' @param x information to stream
#' @keywords internal
NULL


# if x is [SpatVector]
#' @rdname stream_to_db
#' @export
setMethod('stream_to_db', signature(p = 'Pool', remote_name = 'character', x = 'SpatVector'), function(p, remote_name, x, ...) {
  switch(
    terra::geomtype(x),
    'polygons' = append_permanent_dbpoly(
      p = p, remote_name = remote_name, SpatVector = x, ...
    ),
    'points' = append_permanent_dbpoints(
      p = p, remote_name = remote_name, SpatVector = x, ...
    )
  )
})


# If x is [data.frame], append it to the database table it is targeted towards.
# If the table does not exist yet, generate it.
# Usually the last `stream_to_db()` call in the chain

#' @rdname stream_to_db
#' @param custom_table_fields named character vector of columns and manually
#' assigned data types along with any other in-line constraints
#' @param pk character. Which column(s) to select as the primary key
#' @param overwrite whether to overwrite if table already exists (default = FALSE)
#' @inheritDotParams createTableBE -conn -name -fields_custom -row.names -temporary
#' @export
setMethod(
  'stream_to_db',
  signature(p = 'ANY', remote_name = 'character', x = 'data.frame'),
  function(p, remote_name, x, custom_table_fields = NULL, pk = NULL, overwrite = FALSE, ...) {
    checkmate::assert_character(remote_name, len = 1L)

    # coerce input connection object to pool
    p = evaluate_conn(conn = p, mode = 'pool')

    # overwrite if needed
    overwrite_handler(p = p, remote_name = remote_name, overwrite = overwrite)

    # table creation if needed
    if (!existsTableBE(x = p, remote_name = remote_name)) {
      createTableBE(
        conn = p,
        name = remote_name,
        fields_df = x[1,], # sample from first row for datatype
        fields_custom = custom_table_fields,
        row.names = NA,
        temporary = FALSE,
        pk = pk,
        ...
      )
    }

    # append/write info
    pool::dbWriteTable(
      conn = p,
      name = remote_name,
      value = x,
      append = TRUE,
      temporary = FALSE
    )
  })

# if x is [character], assume that it is a filepath to read from.
#' @rdname stream_to_db
#' @param read_fun function. Controls how information should be read. Should
#' expose formal arguments 'x', 'n', and 'callback'
#' @param n number of units (usually rows) to read per chunk
#' @param stop_cond function. Function to run on each chunk. Will stop the
#' chunk read looping when evaluates to TRUE.
#' @param callback (optional) function. Single param function that will take a
#' chunk as input, then modify it into the format expected by the `write_fun`
#' and db representation
#' @param write_fun (optional) function. Controls how the chunk is written to
#' the database. Default functions will automatically run for `data.table` and
#' terra `SpatVector` chunks.
#' @param verbose be verbose
#' @param log_to directory path. Create a logfile in this directory that logs
#' the progress of the chunked read. Any specified directory must exist. Setting
#' `NULL` logs to the same directory as the database
#' @param report_n_chunks if `verbose = TRUE`, report in the console every
#' n chunks that are processed.
#' @param \dots additional params to pass to write_fun
#' @export
setMethod(
  'stream_to_db',
  signature(p = 'ANY', remote_name = 'character', x = 'character'),
  function(p, remote_name, x, # params for write_fun
           read_fun,
           n = 10L,
           stop_cond = function(x) nrow(x) == 0L, # stop iterating when TRUE
           callback = NULL, # default is no change
           write_fun = NULL,
           verbose = TRUE,
           log_to = NULL,
           report_n_chunks = 100L,
           ...) {

    checkmate::assert_character(remote_name, len = 1L)
    checkmate::assert_function(read_fun, args = c('x', 'n', 'i'))
    checkmate::assert_numeric(n)
    checkmate::assert_function(stop_cond)
    if (!is.null(callback)) checkmate::assert_function(callback)
    if (!is.null(write_fun)) checkmate::assert_function(write_fun, args = c('p', 'remote_name', 'x'))
    if (!is.null(log_to)) checkmate::assert_directory_exists(log_to)

    # coerce input connection object to pool
    p = evaluate_conn(conn = p, mode = 'pool')

    # values to track
    rounds = 0L
    time_start = Sys.time()
    started_at = proc.time()

    # log info #
    # -------- #
    if (is.null(log_to)) {
      file_conn <- log_to_dbdir(p)
    } else {
      file_conn <- log_create(log_to)
    }
    on.exit(close(file_conn), add = TRUE)
    if (verbose) wrap_msg('Logging to:', getOption('gdb.last_logpath'))

    # use read_fun to iterate through chunks #
    # -------------------------------------- #
    repeat {

      # read_fun needs 3 params
      # 1. [x] reader input
      # 2. [n] n units to read per round
      # 3. [i] ith round of reading (0 indexed here)
      # read_fun must not throw error when fewer are read than expected.
      # read_fun must not throw error when 0 are found.
      chunk = read_fun(
        x = x,
        n = n,
        i = rounds
      )

      # check chunk for stop condition (chunk of size 0 is default) #
      # ----------------------------------------------------------- #
      if (stop_cond(chunk)) break

      # run callback on chunk #
      # --------------------- #
      if (!is.null(callback)) {
        chunk = callback(chunk)
      }

      # write chunk with write_fun #
      # -------------------------- #
      write_args = list(
        p = p,
        remote_name = remote_name,
        x = chunk,
        ...
      )

      # set overwrite to append after initial round
      if (rounds >= 1L) write_args$overwrite = "append"

      if (!is.null(write_fun)) {
        do.call(write_fun, args = write_args)
      } else {
        # if no specific write_fun passed, let stream_to_db dispatch
        do.call(stream_to_db, args = write_args)
      }

      # increment counter #
      # ----------------- #
      rounds = rounds + 1L

      # log progress #
      # ------------ #
      log_write(
        file_conn = file_conn,
        x = paste(
          # report which chunk and how many lines written according to nrow
          #(may not always be appropriate)
          'chunk: ', rounds, 'written -- nrow', nrow(chunk)
        ),
        main = paste('chunked read to:', remote_name)
      )
      if (isTRUE(verbose) &&
          rounds %% report_n_chunks == 0L) {
        cat("chunk:", rounds, "\n")
      }
    }

    time_end = Sys.time()

    # report time metrics
    if (isTRUE(verbose)) {
      cat('\n[Finished]\n')
      cat(rounds, 'chunks written\n')
      cat('Start  :', as.character(time_start), '\n')
      cat('End    :', as.character(time_end), '\n')
      cat('Elapsed:', data.table::timetaken(started_at))
    }

    log_write(
      file_conn = file_conn,
      x = c(
        '[Finished]\n',
        rounds, 'chunks written\n',
        'Elapsed:', data.table::timetaken(started_at)
      )
    )

    return(TRUE)
  })




# reader functions ####

# read_fun needs 3 params
# 1. [x] reader input
# 2. [n] n units to read per round
# 3. [i] ith round of reading (0 indexed here)
# read_fun must not throw error when fewer are read than expected.
# read_fun must not throw error when 0 are found.

#' @name stream_reader_fun
#' @title Streamable reader functions for specific file formats
#' @description
#' Functions to access files and then read them in a chunkwise manner. Params
#' `x`, `n`, and `i` are required. Outputs from these functions should be in
#' formats that are usable by the downstream write_fun or of classes that
#' have pre-defined specific write functions (currently `SpatVector`)
#' @param x reader input. Usually character filepath to file
#' @param n units (however defined in reader) to read per round
#' @param i 'i'th round of reading (0 indexed)
#' @param bin_size .gef bin size to use (default is "bin100")
#' @param output return as DT (data.table) or SV (terra SpatVector)
#' @details
#' Rules:\cr
#' read_fun must not throw error when fewer are read than expected.\cr
#' read_fun must not throw error when 0 are found.

#' @rdname stream_reader_fun
#' @param bin_size .gef data bin size to use
#' @export
stream_reader_gef_tx <- function(x,
                                 n = 4000L,
                                 i = 0L,
                                 bin_size = "bin100",
                                 output = c('DT', 'SV')) {
  checkmate::assert_file(x)
  checkmate::assert_numeric(i)
  checkmate::assert_character(bin_size)
  output = match.arg(output, choices = c('DT', 'SV'))

  # for gef reading, i stands for number of gene results to retrieve at a time

  # require rhdf5 package
  pkg_name = 'rhdf5'
  if (!requireNamespace(pkg_name, quietly = TRUE)) {
    stopf(
      pkg_name, "is not yet installed.
          To install: \n", "if(!requireNamespace('BiocManager', quietly = TRUE)) install.packages('BiocManager');\nBiocManager::install('",
      pkg_name, "')"
    )
  }

  # Define locations to read from
  gef_root <- paste0("/geneExp/", bin_size)
  gef_gene <- paste0(gef_root, "/gene")
  gef_expr <- paste0(gef_root, "/expression")


  # Find gene dataset dimensions
  fid <- rhdf5::H5Fopen(x, flags = 'H5F_ACC_RDONLY')
  on.exit(try(rhdf5::H5Fclose(fid), silent = TRUE), add = TRUE)
  gid <- rhdf5::H5Gopen(fid, gef_root)
  on.exit(try(rhdf5::H5Gclose(gid), silent = TRUE), add = TRUE, after = FALSE)


  group_ls <- rhdf5::h5ls(gid) %>%
    data.table::setDT()
  gene_len <- group_ls[name == 'gene', dim] %>%
    as.integer()
  # close fid and gid handles
  rhdf5::h5closeAll()

  # Find gene indices to read
  gene_start <- n * i + 1L


  # ERROR CATCHES
  # condition: gene start is greater than gene length
  # - send empty spatvector (end condition where nrow(sv) == 0)
  if (gene_start > gene_len) {
    return(terra::vect())
  }

  # condition: end of gene list is shorter than block
  # - create smaller chunk
  if (gene_start + n > gene_len) {
    gene_block <- gene_len - gene_start + 1L
  } else {
    gene_block <- n
  }


  # TODO add splitting of the hyperslab here in order to parallelize the read


  # Read gene information
  geneDT <- rhdf5::h5read(
    file = x,
    name = gef_gene,
    index = NULL,
    start = gene_start,
    stride = NULL, # sampling
    block = gene_block, # selection unit
    count = 1L # how many blocks
  ) %>%
    data.table::setDT()
  geneDT[, gene := as.character(gene)]
  geneDT[, offset := as.integer(offset)]
  geneDT[, count := as.integer(count)]


  # read expression information dependent on gene subset
  expr_start <- geneDT[1L, offset] + 1
  expr_block <- geneDT[.N, offset + count] - expr_start + 1

  exprDT <- rhdf5::h5read(
    file = x,
    name = gef_expr,
    index = NULL,
    start = expr_start,
    stride = NULL, # sampling
    block = expr_block, # selection unit
    count = 1L # how many blocks
  ) %>%
    data.table::setDT()
  exprDT[, x := as.integer(x)]
  exprDT[, y := as.integer(y)]
  exprDT[, count := as.integer(count)]

  # append genes information to spatial expression
  exprDT[, 'genes' := rep(x = geneDT$gene, geneDT$count)]
  data.table::setnames(exprDT, old = 'genes', new = 'feat_ID')
  data.table::setcolorder(exprDT, c('x', 'y', 'feat_ID', 'count'))

  switch(
    output,
    'DT' = return(exprDT),
    'SV' = {
      # convert to spatvector points
      sv_chunk = terra::vect(
        x = as.matrix(exprDT[,.(x,y)]),
        atts = exprDT[,.(count, feat_ID)]
      )
      return(sv_chunk)
    }
  )
}







# writer functions ####

#' @name stream_writer_fun
#' @title Streamable writer functions for specific file formats
#' @description
#' Functions to take partial reads of files and then write them to DB in a
#' chunkwise manner. Params `x`, `n`, and `i` are required. Outputs from these
#' functions should be in formats that are usable by classes
#' @param x writer input. Usually data.table
#' @param \dots additional params to pass to [createTableBE]
NULL




# Reader for gef workflow. Takes a data.table chunk and writes it for usage
# with dbPointsProxy

#' @rdname stream_writer_fun
#' @inheritParams db_params
#' @export
stream_writer_gef_tx = function(
    p, remote_name, x, ...
)
{
  checkmate::assert_class(p, 'Pool')
  checkmate::assert_character(remote_name, len = 1L)
  checkmate::assert_data_frame(x)

  # When there are no records in table yet
  if (!existsTableBE(x = p, remote_name = remote_name)) {
    last_uid = 0L
  } else {
    last_uid <- sql_max(p, remote_name = remote_name, col = '.uID')
  }

  x[, .uID := last_uid + seq(.N)] # assign unique IDs

  stream_to_db(
    p = p,
    remote_name = remote_name,
    x = x,
    ...
  )
}





