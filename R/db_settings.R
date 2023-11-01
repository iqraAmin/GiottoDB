# DB settings ####

#' @name dbSettings
#' @title Get and set database settings
#' @param x backend_ID or pool object
#' @param setting character. Setting to get or set
#' @param value if missing, will retrieve the setting. If provided, will attempt
#' to set the new value. If 'RESET' will reset the value to default
#' @param ... additional params to pass
#' @export
setMethod('dbSettings', signature(x = 'ANY', setting = 'character', value = 'missing'),
          function(x, setting, value, ...) {
            stopifnot(length(setting) == 1L)

            p = evaluate_conn(conn = x, mode = 'pool')
            DBI::dbGetQuery(p, 'SELECT current_setting(?);', params = list(setting))
          })
#' @rdname dbSettings
#' @export
setMethod('dbSettings', signature(x = 'ANY', setting = 'character', value = 'ANY'),
          function(x, setting, value, ...) {
            stopifnot(length(setting) == 1L)

            p = evaluate_conn(conn = x, mode = 'pool')
            quoted_setting = DBI::dbQuoteIdentifier(p, setting)
            if(value == 'RESET') {
              DBI::dbExecute(p, paste0('RESET ', quoted_setting, ';'))
              return(invisible())
            }
            quoted_value = DBI::dbQuoteLiteral(p, value)
            DBI::dbExecute(p, paste0('SET ', quoted_setting, ' = ', quoted_value))
            invisible() # avoid printout of affected rows from dbExecute
          })



#' @name dbMemoryLimit
#' @title Get and set DB memory limits
#' @param x backend_ID or pool object
#' @param limit character. Memory limit to use with units included (for example
#' '10GB'). If missing, will get the current setting. If 'RESET' will reset to
#' default
#' @param ... additional params to pass
#' @export
dbMemoryLimit = function(x, limit, ...) {
  if(missing(limit)) {
    return(dbSettings(x = x, setting = 'memory_limit'))
  } else if(limit != 'RESET') {
    stopifnot(is.character(limit))
    stopifnot(length(limit) == 1L)
  }
  return(dbSettings(x = x, setting = 'memory_limit', value = limit))
}




#' @name dbThreads
#' @title Set and get number of threads to use in database backend
#' @param x backend ID or pool object of backend
#' @param threads numeric or integer. Number of threads to use.
#' If missing, will get the current setting. If 'RESET' will reset to default.
#' @param ... additional params to pass
#' @export
dbThreads = function(x, threads, ...) {
  if(missing(threads)) {
    return(dbSettings(x = x, setting = 'threads'))
  } else if(threads != 'RESET') {
    stopifnot(length(threads) == 1L)
    threads = as.integer(threads)
  }
  return(dbSettings(x = x, setting = 'threads', value = threads))
}
