
# Print Formatting ####


# Custom stop function
stopf = function(...) {
  wrap_txt('GiottoDB:', ..., errWidth = TRUE) %>%
    stop(call. = FALSE)
}


# From a vector, generate a string with the pattern 'item1', 'item2'
#' @keywords internal
#' @noRd
vector_to_string = function(x) {
  toString(sprintf("'%s'", x))
}



#' @title Generate array for pretty printing of matrix values
#' @name print_array
#' @param i,j,x matched vectors of integers in i and j, with value in x
#' @param dims dimensions of the array (integer vector of 2)
#' @param fill fill character
#' @param digits default = 5. If numeric, round to this number of digits
#' @keywords internal
print_array = function(i = NULL,
                       j = NULL,
                       x = NULL,
                       dims,
                       rownames = rep('', dims[1]),
                       fill = '.',
                       digits = 5L) {
  total_len = prod(dims)

  # pre-generate filler values
  a_vals = rep('.', total_len)

  ijx_nargs = sum(!is.null(i), !is.null(j), !is.null(x))
  if(ijx_nargs < 3 && ijx_nargs > 1) {
    stopf('All values for i, j, and x must be given when printing')
  }
  if(ijx_nargs == 3) {
    # format numeric
    if(is.numeric(x)) {
      if(x < 1e4) {
        x = format(x, digits = digits)
      } else {
        x = format(x, digits = digits, scientific = TRUE)
      }
    }
    # populate sparse values by replace nth elements
    # note that i and j index values must be determined outside of this function
    # since the colnames are not known in here
    for(n in seq_along(x)) {
      a_vals[ij_array_map(i = i[n], j = j[n], dims = dims)] <- x[n]
    }
  }

  # print array
  array(a_vals, dims, dimnames = list(rownames, rep('', dims[2]))) %>%
    print(quote = FALSE, right = TRUE)
}



# Map row (i) and col (j) indices to nth value of an array vector
#' @keywords internal
#' @noRd
#' @return integer position in array vector the i and j map to
ij_array_map = function(i, j, dims) {
  # arrays map vector values first by row then by col
  (j - 1) * dims[1] + i
}





# chunking ####

# find chunk ranges to use. Returns as a list
#' @keywords internal
#' @noRd
chunk_ranges = function(start, stop, n_chunks) {
  stops = floor(seq(start, stop, length.out = n_chunks + 1L))
  stops_idx = lapply(seq(n_chunks), function(i) {
    c(stops[[i]], stops[[i + 1]] - 1)
  })
  stops_idx[[length(stops_idx)]][2] = stops_idx[[length(stops_idx)]][2] + 1L
  stops_idx
}






# DB characteristics ####



#' @name getDBPath
#' @title Get the database backend path
#' @description
#' Get the full normalized filepath of a Giotto backend database. Additionally
#' passing path = ':memory:' will directly return ':memory:' and ':temp:' will
#' will have the function check tempdir() for the backend.
#' @param path directory path in which to place the backend
#' @param extension file extension of the backend (default is .duckdb)
#' @export
getDBPath = function(path = ':temp:', extension = '.duckdb') {
  stopifnot(is.character(path))
  if(path == ':memory:') return(path)
  parent = switch(path,
                  ':temp:' = tempdir(),
                  path)
  parent = normalizePath(parent, mustWork = FALSE)

  if(!basename(parent) == paste0('giotto_backend', extension)) {
    full_path = file.path(parent, paste0('giotto_backend', extension))
  } else {
    full_path = parent
  }
  full_path = normalizePath(full_path, mustWork = FALSE)

  if(!file.exists(full_path)) {
    stopf('No Giotto backend found at\n',
          full_path,
          '\nPlease create or restart the backend.')
  }

  return(full_path)
}





# DB size

#' @name backendSize
#' @title Size of backend database
#' @description Given a backend ID, find the current size of the DB backend file
#' @param backend_ID backend ID
#' @export
backendSize = function(backend_ID) {
  dbdir = getBackendPath(backend_ID)
  file.size(dbdir)
}






# DB path / hash ID generation ####


# Internal function. Given a path and extension, create a full path that ends in
# 'giotto_backend[extension]'.
# The path is normalized to ensure relative path param inputs have
# the same outputs as the absolute path.
#
# If the directory in which to place the database does not exist, then it will
# be generated.
#' @keywords internal
#' @noRd
set_db_path = function(path = ':temp:', extension = '.duckdb', verbose = TRUE) {
  stopifnot(is.character(path))
  if(path == ':memory:') return(path)
  parent = switch(path,
                  ':temp:' = tempdir(),
                  path)
  parent = normalizePath(parent, mustWork = FALSE)

  # get dir if already full path
  if(basename(parent) == paste0('giotto_backend', extension)) {
    parent = gsub(pattern = basename(parent),
                  replacement = '',
                  x = parent)
  }
  parent = normalizePath(parent, mustWork = FALSE)

  if(!dir.exists(parent)) {
    dir.create(dir.create(parent, recursive = TRUE))
  }

  full_path = file.path(parent, paste0('giotto_backend', extension))
  if(isTRUE(verbose)) wrap_msg('Creating backend at\n', full_path)
  return(full_path)
}





#' @name getBackendID
#' @title Get the backend hash ID from the database path
#' @param path directory path to the database. Accepts :memory: and :temp:
#' inputs as well
#' @param extension file extension of database backend (default = '.duckdb')
#' @export
getBackendID = function(path = ':temp:', extension = '.duckdb') {
  full_path = getDBPath(path = path, extension = extension)
  hash = calculate_backend_id(full_path)
  return(hash)
}





#' @name calculate_backend_id
#' @title Calculate a backend ID
#' @description Calculate a GiottoDB backend ID from its filepath. A hash value
#' is calculate and pre-pended with 'ID_' for clarity.
#' @param path filepath to database backend
#' @keywords internal
#' @noRd
calculate_backend_id = function(path) {
  hash = calculate_hash(path)
  return(paste0('ID_', hash))
}




#' @name calculate_hash
#' @title Calculate a hash value
#' @description
#' Generate a hash value for x
#' @param object anything
#' @keywords internal
#' @noRd
calculate_hash = function(object) {
  digest::digest(object = object, algo = 'xxhash64')
}








#' @name file_extension
#' @title Get file extension(s)
#' @param file filepath
#' @keywords internal
file_extension = function(file)
{
    ex = strsplit(basename(file), split = ".", fixed = TRUE)[[1L]]
    return(ex[-1])
}






# https://stackoverflow.com/a/25902379
#' @name result_count
#' @title Create a counter
#' @noRd
result_count = function() {
  count = getOption('gdb.res_count')
  options(gdb.res_count = count + 1L)
  paste0('gdb_', sprintf('%03d', count))
}







