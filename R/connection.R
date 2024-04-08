#' @include package_imports.R
#' @include classes.R
#' @include generics.R
NULL


pool_closed <- function(x) {
  x$counters$free + x$counters$taken == 0
}



# backendID ####
#' @name backendID-generic
#' @title Get and set hash ID of backend
#' @description
#' Get the hash value/slot
#' @aliases backendID, backendID<-
#' @param x object to get hash from
#' @param value (character) hash ID to set
#' @export
setMethod('backendID', signature(x = 'backendInfo'), function(x) {
  slot(x, 'hash')
})
#' @rdname backendID-generic
#' @export
setMethod('backendID', signature(x = 'dbData'), function(x) {
  slot(x, 'hash')
})


#' @rdname backendID-generic
#' @export
setMethod('backendID<-', signature(x = 'backendInfo', value = 'character'),
          function(x, ..., value) {
            slot(x, 'hash') = value
          })
#' @rdname backendID-generic
#' @export
setMethod('backendID<-', signature(x = 'dbData', value = 'character'),
          function(x, ..., value) {
            slot(x, 'hash') = value
          })









# cPool ####

#' @name cPool-generic
#' @title Database connection pool
#' @description Return the database connection pool object
#' @aliases cPool, cPool<-
#' @inheritParams db_params
#' @include extract.R
#' @export
setMethod('cPool', signature(x = 'dbData'), function(x) {
  dbplyr::remote_con(x[])
})
#' @rdname cPool-generic
#' @export
setMethod('cPool<-', signature(x = 'dbData'), function(x, value) {
  dbtbl = x[]
  dbtbl$src$con = value
  initialize(x, data = dbtbl)
})

#' @rdname cPool-generic
#' @export
setMethod('cPool', signature(x = 'ANY'), function(x) {
  stopifnot(inherits(x, 'tbl_sql'))
  dbplyr::remote_con(x)
})
#' @rdname cPool-generic
#' @export
setMethod('cPool<-', signature(x = 'ANY'), function(x, value) {
  stopifnot(inherits(x, 'tbl_sql'))
  x$src$con = value
  x
})





# remoteName ####
#' @name remoteName-generic
#' @title Database table name
#' @description
#' Returns the table name within the database the the object acts as a link to
#' @aliases remoteName
#' @inheritParams db_params
#' @export
setMethod('remoteName', signature(x = 'dbData'), function(x) {
  slot(x, 'remote_name')
})




# is_init ####
#' @name is_init-generic
#' @title Determine if dbData object is initialized
#' @param x dbData object
#' @keywords internal
setMethod('is_init', signature(x = 'dbData'), function(x, ...) {
  slot(x, 'init')
})




# dbIsValid ####
#' @name dbIsValid
#' @title Check if GiottoDB backend object has valid connection
#' @description
#' The method for `dbData` objects checks if they possess a pool that is valid.
#' The methods for `gdbBackend` or `gdbBackendID` check if the backend has a
#' valid connection. The two are not the same since if a `dbData` object does
#' not have a valid connection, it can still pull a valid connection from the
#' backend.
#' @param dbObj GiottoDB object to check for database valid connection
NULL

#' @rdname dbIsValid
#' @export
setMethod('dbIsValid', signature('dbData'), function(dbObj) {
  p = cPool(dbObj)
  if(is.null(p)) stop('No connection object found\n')
  ifelse(pool::dbIsValid(p) && !pool_closed(p), TRUE, FALSE)
})

#' @rdname dbIsValid
#' @export
setMethod("dbIsValid", signature("gdbBackendID"), function(dbObj) {
  p <- try({getBackendPool(dbObj)}, silent = TRUE)
  if (inherits(p, "try-error")) return(FALSE)
  return(dbIsValid(.DB_ENV[[dbObj]]))
})

#' @rdname dbIsValid
#' @export
setMethod("dbIsValid", signature("gdbBackend"), function(dbObj) {
  p <- dbObj$pool
  ifelse(pool::dbIsValid(p) && !pool_closed(p), TRUE, FALSE)
})



# listTablesBE ####
#' @name listTablesBE
#' @title List the tables in a connection
#' @param x GiottoDB object or DB connection
#' @param ... additional params to pass
#' @export
setMethod('listTablesBE', signature(x = 'dbData'), function(x, ...) {
  stopifnot(dbIsValid(x))
  pool::dbListTables(cPool(x), ...)
})

#' @rdname listTablesBE
#' @export
setMethod('listTablesBE', signature(x = 'character'), function(x, ...) {
  x_pool = try(getBackendPool(x), silent = TRUE) # should be backend_ID
  if(inherits(x_pool, 'try-error')) {
    x_pool = getBackendID(x)
  }
  stopifnot(pool::dbIsValid(x_pool))
  pool::dbListTables(x_pool, ...)
})

#' @rdname listTablesBE
#' @export
setMethod('listTablesBE', signature(x = 'ANY'), function(x, ...) {
  stopifnot(pool::dbIsValid(x))
  pool::dbListTables(x, ...)
})






# existsTableBE ####
#' @name existsTableBE
#' @title Whether a particular table exists in a connection
#' @param x GiottoDB object or DB connection
#' @param ... additional params to pass
#' @export
setMethod('existsTableBE',
          signature(x = 'dbData', remote_name = 'character'),
          function(x, remote_name, ...) {
            stopifnot(dbIsValid(x))
            extabs = pool::dbListTables(conn = cPool(x), ...)
            remote_name %in% extabs
          })

#' @rdname existsTableBE
#' @export
setMethod('existsTableBE',
          signature(x = 'ANY', remote_name = 'character'),
          function(x, remote_name, ...) {
            stopifnot(pool::dbIsValid(x))
            extabs = pool::dbListTables(conn = x, ...)
            remote_name %in% extabs
          })










#' @title Create a pool of database connections
#' @name create_connection_pool
#' @description Generate a pool object from which connection objects can be
#' checked out.
#' @param drv DB driver (default is duckdb::duckdb())
#' @param dbpath path to database
#' @param with_login (default = FALSE) flag to check R environment variables
#' for login info and/or prompt for password
#' @param ... additional params to pass to pool::dbPool()
#' @return pool of connection objects
#' @keywords internal
create_connection_pool = function(drv = 'duckdb::duckdb()',
                                  dbdir = ':memory:',
                                  ...) {
  pool::dbPool(drv = eval(parse(text = drv)), dbdir = dbdir, ...)
}



# dbBackend ####

#' @title GiottoDB backend
#' @name dbBackend
#' @description
#' Defines and creates a database connection pool to be used by the backend
#' when a database driver function is provided.
#' When a `backendInfo` or `list` input is provided to `x` param instead, a
#' reconnection to an existing GiottoDB backend will be performed.
#' @param x database driver (default is duckdb::duckdb()), `backendInfo` object,
#' or or list of 1. DB driver call in string format, 2. path to the existing DB
#' file.
#' @param with_login (default = FALSE) whether a login is needed
#' @param verbose be verbose
#' @param ... additional params to pass to pool::dbPool()
#' @returns invisibly returns backend ID
NULL


#' @rdname dbBackend
#' @param dbdir directory to create a backend database
#' @param factory a function called to create database driver objects for the
#' backend. Default is `duckdb::duckdb()`
#' @param extension file extension (default = '.duckdb')
#' @export
setMethod("dbBackend", signature("missing"), function(
    dbdir = ":temp:",
    factory = duckdb::duckdb(),
    extension = ".duckdb",
    with_login = FALSE,
    verbose = TRUE,
    ...
  ) {
  # store driver call as a string
  drv_call = deparse(substitute(factory))

  # setup database filepath
  dbpath = set_db_path(path = dbdir, extension = extension, verbose = verbose)

  # collect connection information
  gdb_info = new('backendInfo',
                 driver_call = drv_call,
                 db_path = dbpath)
  if(isTRUE(verbose)) wrap_msg(
    ' Backend Identifier:', backendID(gdb_info)
  )

  # assemble params
  list_args = list(...)
  if(isTRUE(with_login)) { # check login details
    if(is.null(list_args$user)) list_args$user = Sys.getenv('DB_USERNAME')
    if(is.null(list_args$pass)) {
      if(requireNamespace('rstudioapi', quietly = TRUE)) {
        list_args$pass =
          Sys.getenv('DB_PASSWORD', rstudioapi::askForPassword('Database Password'))
      } else {
        list_args$pass = Sys.getenv('DB_PASSWORD')
      }
    }
  }
  list_args$drv = drv_call
  list_args$dbdir = dbpath

  # get connection pool and store info
  con_pool = do.call(what = 'create_connection_pool', args = list_args)

  b <- new("gdbBackend")
  b$info <- gdb_info
  b$pool <- con_pool

  .DB_ENV[[backendID(gdb_info)]] = b
  invisible(backendID(gdb_info))
})

#' @rdname dbBackend
#' @export
setMethod(
  'dbBackend', signature('backendInfo'),
  function(x, with_login = FALSE, verbose = TRUE, ...)
  {
    b_ID = backendID(x)
    p <- .DB_ENV[[b_ID]]$pool
    # if already valid, exit
    try_val = try(DBI::dbIsValid(p), silent = TRUE)
    # updated for changes in how pool is behaving where it is valid, but
    # can have no available handles
    if(isTRUE(try_val) && !pool_closed(p)) return(invisible(b_ID))


    # assemble params
    list_args = list(...)
    if(isTRUE(with_login)) { # check login details
      if(is.null(list_args$user)) list_args$user = Sys.getenv('DB_USERNAME')
      if(is.null(list_args$pass)) {
        if(requireNamespace('rstudioapi', quietly = TRUE)) {
          list_args$pass =
            Sys.getenv('DB_PASSWORD', rstudioapi::askForPassword('Database Password'))
        } else {
          list_args$pass = Sys.getenv('DB_PASSWORD')
        }
      }
    }
    list_args$drv = x@driver_call
    list_args$dbdir = x@db_path

    # regenerate connection pool object
    con_pool = do.call(what = create_connection_pool, args = list_args)

    b <- new("gdbBackend")
    b$info <- x
    b$pool <- con_pool

    .DB_ENV[[b_ID]] <- b

    return(invisible(b_ID))
  })

# for reconnects where a backend info is not available.
#' @rdname dbBackend
#' @export
setMethod(
  'dbBackend', signature('list'),
  function(x, with_login = FALSE, verbose = TRUE, ...)
  {
    dbdrv <- x[[1]]
    dbpath <- normalizePath(x[[2]])

    if (!inherits(dbdrv, "character")) {
      stopf(
        "dbBackend:
        First value in list must be a DB driver function call written as character
        e.g: \"duckdb::duckdb()\""
      )
    }
    if (!inherits(dbpath, 'character')) {
      stopf(
        "dbBackend:
        Second value in list must be a filepath to the DB"
      )
    }


    # test the driver to be functional in memory
    test_db_path = paste0(tempdir(), '/test.db')
    test_res <- try(create_connection_pool(dbdrv, dbdir = test_db_path),
                    silent = TRUE)
    if (!inherits(test_res, 'try-error')) {
      on.exit(pool::poolClose(test_res), add = TRUE)
      on.exit(file.remove(test_db_path), add = TRUE)
    } else {
      stopf(
        "dbBackend:
        First value in list must be a DB driver function call written as character
        e.g: \"duckdb::duckdb()\""
      )
    }

    # create backend info
    bInfo <- new(
      'backendInfo',
      driver_call = dbdrv,
      db_path = dbpath,
      hash = calculate_backend_id(dbpath)
    )

    # attempt reconnect
    dbBackend(x = bInfo, ...)
  }
)






# .DB_ENV getters ####

#' @title Get the backend details environment
#' @name getBackendEnv
#' @export
getBackendEnv = function() {
  return(.DB_ENV)
}





#' @name getBackendPool
#' @title Get backend connection pool
#' @description
#' Get backend information. \code{getBackendPool} gets the associated
#' connection pool object. \code{getBackendInfo} gets the backendInfo object
#' that contains all connection details needed to regenerate another connection
#' @param backend_ID hashID generated from the dbpath (xxhash64) used as unique ID for
#' the backend
#' @export
getBackendPool = function(backend_ID) {
  p = try(.DB_ENV[[backend_ID]]$pool, silent = TRUE)
  if(is.null(p) | inherits(p, 'try-error')) {
    stopf('No associated backend found.
          Please create or reconnect the backend.')
  }
  return(p)
}

#' @describeIn getBackendPool get backendInfo object containing connection details
#' @export
getBackendInfo = function(backend_ID) {
  i = try(.DB_ENV[[backend_ID]]$info, silent = TRUE)
  if(is.null(i) | inherits(i, 'try-error')) {
    stopf('No associated backend found.
          Please create or reconnect the backend.')
  }
  return(i)
}





#' @describeIn getBackendPool Get a DBI connection object from pool.
#' Provided for convenience. Must be returned using pool::poolReturn() after use.
#' @export
getBackendConn = function(backend_ID) {
  p = getBackendPool(backend_ID)
  pool::poolCheckout(p)
}




#' @describeIn getBackendPool get filepath of backend
#' @export
getBackendPath = function(backend_ID) {
  conn = evaluate_conn(conn = backend_ID, mode = 'conn')
  on.exit(pool::poolReturn(conn))
  conn@driver@dbdir
}





# evaluate_conn ####
# Internal function to allow functions to take connection inputs as a DBI
# connection object, Pool of connections, or a hash ID of a backend
# The connections outputted will be preferentially a pool, but if a DBI
# connection is given as input, it will also be returned. This default can also
# be overridden using the mode = 'pool' or mode = 'conn' param for hash or pool
# inputs


#' @name evaluate_conn
#' @title Convert between DB handles and identifiers
#' @description GiottoDB refers to the database in several ways in different
#' situations.
#' \itemize{
#'   \item **path** (filepath) usually used whenever user input is part of the
#' workflow since that is the easiest for the user to have access to.
#'   \item **id** (backend hash ID) a shortened and unique way for functions to
#' reference a specific database backend.
#'   \item **pool** (pool of connection objects) a pool object that can be used
#'   to generate a database connected tbl.
#'   \item **conn** (DBI connection) a DBIConnection object that can be used
#'   to generate a database connected tbl. Used in situations where pool objects
#'   are not possible to be used, such as with temporary table generation.
#' }
#' Conversions between any of the four types are possible except for conn to
#' pool. These conversions can be performed using evaluate_conn.
#' @param conn object to convert from
#' @param mode desired output type. One of pool, conn, path, or id
#' @param ... additional params to pass
NULL



# character.
#' @rdname evaluate_conn
#' @export
setMethod(
  'evaluate_conn', signature(conn = 'character'),
  function(conn, mode = c('pool', 'conn', 'path', 'id'), ...
  )
  {
    # if pattern matched, assume hash input
    if (nchar(conn) == 19 && grepl('^ID_', conn)) {
      mode = match.arg(mode, choices = c('pool', 'conn', 'path', 'id'))
      out = switch(
        mode,
        'pool' = getBackendPool(conn),
        'conn' = getBackendConn(conn),
        'path' = {
          getBackendPool(conn) %>%
            pool::localCheckout() %>%
            conn_to_path()
        },
        'id' = conn
      )

      return(out)
    } else { # otherwise, assume db_path input or token
      # ensure that it is a giotto backend and that it is a file rather
      # than a directory.

      check_backend_path <- function(x) {
        grepl('giotto_backend', basename(conn)) &&
          checkmate::test_file_exists(conn)
      }

      # if conn is token
      if (conn == ":temp:") conn <- tempdir()

      # if conn is not the backend path, see if it is the directory that
      # contains a backend db file then check again
      if (!check_backend_path(conn)) {
        conn <- list.files(conn, pattern = 'giotto_backend.', full.names = TRUE)[1L]
        conn <- normalizePath(conn)
        if (!check_backend_path(conn)) stopf('evaluate_conn: db path not found')
      }

      hash = calculate_backend_id(conn)
      evaluate_conn(hash, mode = mode)
    }
  })

#' @rdname evaluate_conn
#' @export
setMethod('evaluate_conn', signature(conn = 'Pool'),
          function(conn, mode = c('pool', 'conn', 'path', 'id'), ...)
{
  mode = match.arg(mode, choices = c('pool', 'conn', 'path', 'id'))
  switch(
    mode,
    'pool' = conn,
    'conn' = pool::poolCheckout(conn),
    'path' = conn_to_path(conn),
    'id' = {
      calculate_backend_id(
        evaluate_conn(conn, mode = 'path')
      )
    }
  )
})

# pool not supported for this conversion, but pool is kept as default so that
# people are aware that the default conversion is not possible.
#' @rdname evaluate_conn
#' @export
setMethod('evaluate_conn', signature(conn = 'DBIConnection'),
          function(conn, mode = c('pool', 'conn', 'path', 'id'), ...)
{
  switch(
    mode,
    'pool' = stopf('Cannot get pool from conn object'),
    'conn' = {
      if(class(conn) %in% c("Microsoft SQL Server",
                            "PqConnection",
                            "RedshiftConnection",
                            "BigQueryConnection",
                            "SQLiteConnection",
                            "duckdb_connection",
                            "Spark SQL",
                            "OraConnection",
                            "Oracle",
                            "Snowflake")) {
        return(conn)
      } else {
        stopf(class(conn), "is not a supported connection type.")
      }
    },
    'path' = conn_to_path(conn),
    'id' = {
      evaluate_conn(conn, mode = 'path') %>%
        calculate_backend_id()
    }
  )
})




conn_to_path <- function(x) {
  if (inherits(x, 'Pool')) {
    x <- pool::localCheckout(x)
  }
  x@driver@dbdir
}



conn_to_id <- function(x) {
  calculate_backend_id(
    conn_to_path(x)
  )
}




# dbBackendClose ####

#' @name dbBackendClose
#' @title Disconnect backend from database
#' @description
#' Closes pools. If specific backend_ID(s) are given then those will be closed.
#' When no specific ID is given, all existing backends will be closed.
#' If a `dbData` object is provided instead, closes the associated backend.
#' @param x `gdbBackendID` of backend to close (optional) or `dbData`
#' @returns NULL invisibly
NULL


#' @rdname dbBackendClose
#' @export
setMethod("dbBackendClose", signature("gdbBackendID"), function(x, ...) {
  dbBackendClose(.DB_ENV[[x]])
})

#' @rdname dbBackendClose
#' @export
setMethod("dbBackendClose", signature("gdbBackend"), function(x, ...) {
  if(!dbIsValid(x$pool)) return(invisible())
  dots <- list(...)
  if (is.null(dots$shutdown)) dots$shutdown <- TRUE
  # dbDisconnect is not supported for `pool` objects
  con <- pool::poolCheckout(x$pool)
  do.call(pool::dbDisconnect, args = c(list(conn = con), dots))
  pool::poolReturn(con)
  # normally throws warning that connection is already closed
  if (!pool_closed(x$pool)) suppressWarnings(pool::poolClose(x$pool))
  return(invisible(NULL))
})

#' @rdname dbBackendClose
#' @export
setMethod('dbBackendClose', signature('dbData'), function(x, shutdown = TRUE) {
  # already closed
  if(!dbIsValid(x)) return(invisible())
  pool::poolClose(cPool(x), force = TRUE)
  return(invisible())
})

#' @rdname dbBackendClose
#' @export
setMethod("dbBackendClose", signature("environment"), function(x, ...) {
  lapply(x, function(id) {
    dbBackendClose(id)
  })
  return(invisible())
})

#' @rdname dbBackendClose
#' @export
setMethod("dbBackendClose", signature("missing"), function() {
  dbBackendClose(.DB_ENV)
})



#' @name dbBackendList
#' @title Existing GiottoDB backends info
#' @returns data.table of backend IDs and database paths
#' @examples
#' dbBackend()
#' dbBackendList()
#' @export
dbBackendList <- function() {
  info <- lapply(as.list(.DB_ENV), function(b) b$info@db_path)
  data.table::data.table(
    ID = names(info),
    path = as.character(info)
  )
}




# dbData reconnection ####

# For data representations:
# Checks if the object still has a valid connection. If it does then it returns
# the object without modification. If it does need reconnection then a copy of
# the associated active connection pool object is pulled from .DB_ENV and if
# that is also closed, an error is thrown asking for a the backend to be
# reconnected.
#
# Also ensures that the object contains a reference to a valid table within
# the database. Runs at the beginning of most function calls involving
# dbData object queries.
# internal .reconnect method
setMethod(
  '.reconnect', signature(x = 'dbData'),
  function(x) {

    # if connection is still active, return directly
    if(dbIsValid(x)) return(x)

    # otherwise get conn pool
    p = .DB_ENV[[x@hash]]$pool
    if(!dbIsValid(p)) {
      stopf(sprintf('invalid backend [%s]\n', x@hash),
            "Reconnect the backend or create a new one.")
    }

    cPool(x) <- p

    if(!x@remote_name %in% listTablesBE(x)) {
      stopf('Object reconnection attempted, but table of name', x@remote_name,
            'not found in db connection.\n',
            'Memory-only table that was never written to DB?')
    }

    return(initialize(x))
  })



#' @title Load a GiottoDB dbData object
#' @name loadGDB
#' @description Loads a GiottoDB `dbData` object with `readRDS()`. Also performs
#' initialization to repair the database connection.
#' @param file filepath
#' @param silent silences read-in console prints
#' @details
#' Some warnings and messages are expected when loading GiottoDB `dbData`
#' objects due to the way that pool and dplyr work. These are diverted to a
#' temporary file with `sink()` if `silent` is TRUE. These messages should
#' have no effect on function.
#' @returns loaded in `dbData` object
#' @export
loadGDB <- function(file, silent = TRUE) {
  checkmate::assert_file_exists(file)
  # Some warnings and messages are expected:
  # <pool> Failed to activate and/or validate existing object.
  # <pool> Trying again with a new object.
  # Error: rapi_unlock: Invalid database reference
  # In addition: Warning message:
  #   Connection already closed.
  # These are silenced by diverting them to a tempfile sink
  if (silent) {
    f <- file(tempfile(), open = "wt")
    sink(f, type = "message")
    on.exit(sink(type = "message"))
  }

  x <- readRDS(file)
  return(force(initialize(x)))
}



# DB table management ####



#' @title Write a persistent table to the database
#' @name writeTableBE
#' @param p database connection pool object
#' @param remote_name name to assign the table within the database
#' @param value dataframe (or coercible) to write to backend
#' @param ... additional params to pass to DBI::dbWriteTable()
#' @export
writeTableBE = function(p, remote_name, value, ...) {
  pool::dbWriteTable(conn = p, name = remote_name, value = value, ...)
}



#' @title Open a table from database
#' @name tableBE
#' @param cPool database connection pool object
#' @param remote_name name of table with database
#' @param ... additional params to pass to dplyr::tbl()
#' @export
tableBE = function(cPool, remote_name, ...) {
  dplyr::tbl(src = cPool, remote_name, ...)
}





# DBMS detection ####

# From package CDMConnection

#' @name dbms-generic
#' @title Database management system
#' @aliases dbms
#' @description
#' Get the database management system (dbms) of an object
#' @param x A connection pool object, DBI connection, or dbData
#' @param ... additional params to pass
#' @export
setMethod('dbms', signature(x = 'dbData'), function(x, ...) {
  dbms(cPool(x))
})
#' @rdname dbms-generic
#' @export
setMethod('dbms', signature(x = 'character'), function(x, ...) {
  dbms(evaluate_conn(x, mode = 'pool'))
})

#' @rdname dbms-generic
#' @export
setMethod('dbms', signature(x = 'Pool'), function(x, ...) {
  conn = pool::poolCheckout(x)
  on.exit(pool::poolReturn(conn))
  res = dbms(conn)
  return(res)
})
#' @rdname dbms-generic
#' @export
setMethod('dbms', signature(x = 'DBIConnection'), function(x, ...) {
  if (!is.null(attr(x, "dbms")))
    return(attr(x, "dbms"))
  result = switch(
    class(x),
    "Microsoft SQL Server" = "sql server",
    "PqConnection" = "postgresql",
    "RedshiftConnection" = "redshift",
    "BigQueryConnection" = "bigquery",
    "SQLiteConnection" = "sqlite",
    "duckdb_connection" = "duckdb",
    "Spark SQL" = "spark",
    "OraConnection" = "oracle",
    "Oracle" = "oracle",
    "Snowflake" = "snowflake"
    # add mappings from various connection classes to dbms here
  )
  if (is.null(result)) {
    stopf(class(x), "is not a supported connection type.")
  }
  return(result)
})









