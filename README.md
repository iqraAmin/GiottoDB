# GiottoDB

[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)

Framework and structures for Giotto Suite database scalability.

### Backend Framework

#### 1. Creating a backend

The first step is to create a *GiottoDB* backend. The database driver and filepath (defaults to duckdb and a temporary directory respectively) is provided at this step. This provides a centralized location from which to retrieve connections to the database.

```         
bID = createBackend()
bID # hash ID value for the database file
```

Each database filepath in use is assigned an unique identifier which is returned invisibly.

$~$

#### 1.1 Interacting with the backend

This package uses *pool* to abstract away connection handling. To retrieve a pool of connections from the backend.

```         
p = getBackendPool(bID)
```

This is one of several functions to work with the backend. Other examples are below. Most are not needed for users because they are used internally.

-   `getBackendID()` get backend ID hash based on the full database filepath

-   `getBackendPath()` get the specified database filepath using the backend ID hash

-   `getBackendEnv()` retrieves the active backend environment that is holding all connection details and connection pools

-   `getBackendConn()` get a *DBI* connection object to the specified database.

-   `getBackendInfo()` get the `backendInfo` object that holds connection details for the specified database.

$~$

#### 2. Closing and reconnecting the backend

The specified backend can be closed using `closeBackend()`, this is generally not necessary to be manually run. All backends and associated connections will be finalized at the end of the session.\
*GiottoDB* data objects ask for new pool connections to the database from the backend when the connections are closed or otherwise become invalid. When the backend itself is closed, the user must reconnect the backend.\
Reconnections to an existing backend can be performed using the associated `backendInfo` object using `reconnectBackend()`

```         
# keep backend connection details
bInfo = getBackendInfo(bID)

p # still valid
closeBackend(bID)
p # now invalid 

reconnectBackend()
p = getBackendPool(bID)
p # new valid pool connection
```

$~$

### Writing values into the database

*GiottoDB* implements a number of S4 structures to represent database tables as more familiar data types. (See the class hierarchy diagram in docs)

#### 1 Creating objects

If a `character` input is provided to the object creation functions then the values will be read into the database and a database-backed representation of that data will be created.

-   `createDBPolygonProxy()`

-   `createDBPointProxy()`

#### 1.1 Chunked reading

Data that requires out of memory processing is often too large to load into memory and be written into the database in a single step. *GiottoDB* reads in values in a chunked manner to get around this.

`streamToDB_fread()` and `streamSpatialToDB_arrow()` (still experimental likely to change in the future) are the functions to read data in in a chunked manner. A table is created using `createTableBE()` (currently internal), then the chunks are read into the database. These functions (and also the object creation functions) expose parameters for how the chunked reading occurs. Important params include:

-   `callback` a function to apply to each read in chunk of data (mainly for data cleaning and formatting purposes) before it gets written into the database as a table. This function should take a data.table as input and output a data.table as well

-   `custom_table_fields` (advanced) specify SQL specific data types and constraints for columns to use when creating the database table for this data. If not provided for any column, default schemas decided by *DBI* will be used for that column.

-   `pk` assign a primary key column when creating the database table

### Database settings

Database and PRAGMA settings can be retrieved and set using `dbSettings()`

```         
dbSettings(bID, 'memory_limit')
dbSettings(bID, 'memory_limit', '2GB')
```

Some settings have dedicated convenience functions like `dbMemoryLimit()`
