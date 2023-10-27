
# duckdb specific functionalities


#' @name duckdb_extension
#' @title DuckDB extensions
#' @param p pool or DBI connection object. If missing, will retrieve dataframe
#' of duckdb extensions information.
#' @param extension character. When `p` is also provided, will install and load
#' the designated extension.
#' @return invisibly returns TRUE after an installation or load
#' @export
duckdb_extension <- function(p, extension = NULL) {
  if (!missing(p)) {
    if (is.null(extension)) {
      stopf("extension to install/load must be provided")
    } else {
      checkmate::assert_character(extension, len = 1L)
      extension = pool::dbQuoteString(p, extension)
      conn = evaluate_conn(p, mode = 'conn')
      on.exit(pool::poolReturn(conn))
      DBI::dbSendQuery(conn, paste0("INSTALL ", extension,";"))
      DBI::dbSendQuery(conn, paste0("LOAD ", extension,";"))
      return(invisible(TRUE))
    }
  } else {
    conn = DBI::dbConnect(duckdb::duckdb())
    on.exit(DBI::dbDisconnect(conn, shutdown = TRUE))
    DBI::dbGetQuery(conn, "FROM duckdb_extensions();")
  }
}


