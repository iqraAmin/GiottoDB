
# Functions for logging progress

#' @name gdbReadLog
#' @title Read from the last generated log file
#' @param filepath character. filepath to log file. If omitted, tries to find the
#' last created log (will not work after a crash)
#' @export
gdbReadLog <- function(filepath) {
  if (missing(filepath)) filepath = getOption('gdb.last_logpath', NULL)

  if (is.null(filepath)) {
    stopf('No last log found.')
  }

  file_conn = file(filepath)
  on.exit(close(file_conn))
  readLines(file_conn)
}


#' @name log_to_dbdir
#' @title Create log file in DB directory
#' @param p pool of connections
#' @keywords internal
log_to_dbdir <- function(p) {
  db_path <- evaluate_conn(p, mode = 'path')
  log_path <- gsub(basename(db_path), '', db_path)
  log_create(log_path)
}


#' @name log_create
#' @title Create a log file
#' @param filedir character. Directory to create a logfile
#' @description
#' Creates a file called 'log.txt' at the specified directory. If no directory
#' is provided, it defaults to `tempdir()`. The path is additionally written to
#' the option 'gdb.last_logpath' and the open file connection to
#' 'gdb.last_logconn'
#' @keywords internal
log_create = function(filedir = tempdir()) {
  filepath = paste0(filedir, '/log.txt') %>%
    normalizePath()
  if (!file.exists(filepath)) file.create(filepath)
  fileConn = file(filepath, open = 'a+') # open in 'a'ppend and reading (+) mode

  options('gdb.last_logpath' = filepath)
  options('gdb.last_logconn' = fileConn)
  fileConn
}

#' @name log_write
#' @title Write to log file
#' @param file_conn a file connection
#' @param x character vector. Content to write
#' @param main character. Title to assign log entry
#' @keywords internal
log_write <- function(file_conn, x = '', main = '') {
  log_entry <- c(
    paste0('[', main, ']'),
    paste0('time:', as.character(Sys.time())),
    paste(x),
    '\n'
  )
  writeLines(log_entry, con = file_conn, sep = '\n')
}




