# GiottoDB

[![Lifecycle: experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)

Framework and structures for Giotto Suite database scalability.

### Backend Framework

S4 classes such as the the *Giotto* object and *Giotto* subobjects are complex data structures that contain many data representation objects within them (data.frames, matrices, SpatVectors, etc.) that are operated on with the assumption that they function independently from the overarching *Giotto* object. However, this characteristic is not easily transferrable to simple database (DB) connected tables since they have connections that must be regenerated at the start of a session. Storage of connection details in the overarching object with instructions to reconnect a table using those details at the start of a function involving the DB object is a workable way to do things, but effectively ties the usage of the database objects to that overarching object. Moreover, it puts the onus of database management on the side of any packages utilizing the database, increasing the complexity of implementation when objects should be expected to function in a largely self-contained manner.

To solve this issue, *GiottoDB* creates a backend as a centralized location from which to retrieve connections to the database. On package load, an environment called `.DB_ENV` is defined at the package-level to store this information, allowing *GiottoDB* objects to be able to find a set of connection details for themselves no matter where they are, either independent or nested deeply inside a larger data structure. On session close, all open connections within `.DB_ENV` are closed. Additionally, *GiottoDB* defines a parent virtual `dbData` class that all other *GiottoDB* objects inherit from and handles connection and reconnection of *GiottoDB* objects to the database backend they are backed by.

#### 1. Creating a backend

The first step is to create a *GiottoDB* backend. The database type to use is decided through a database driver passed to `drv` param. It defaults to `duckdb::duckdb()`. The location of the database file to generate is also decided at this step through the param `dbdir`. The default value is ":temp:" which is treated as a special token input that is a stand-in for `tempdir()`.

```         
bID = createBackend()
bID # hash ID value for the database file
```

Each database filepath in use is assigned an unique identifier which is returned invisibly. More than one backend can be used at the same time, allowing for usage of multiple database files or datasets at the same time.

This is implemented by creating a object in `.DB_ENV` with the same variable name as the hash/backend ID. The list consists of a `backendInfo` object which records the database connection details to facilitate backend reconnection and a connection pool object (described in the next section) that can be used to provide connections to the database.

$~$

#### 1.1 Connections and the backend

This package uses *pool* to abstract away connection handling. To retrieve a pool of connections from the backend. Either of the following functions can be used.

```         
?evaluate_conn
p = evaluate_conn(tempdir())
# or
# p = getBackendPool(bID)
```

`getBackendPool()` is one of several functions to work with the backend. Other examples are below. Most are not needed for users because they are used internally. `evaluate_conn` is a single function that wraps each of these functions, allowing easy interconversion between each of the different ways of referring to a database backend.

-   `getBackendPool()` get a pool connection object for the backend ID specified

-   `getBackendID()` get backend ID hash based on the full database filepath

-   `getBackendPath()` get the specified database filepath using the backend ID hash

-   `getBackendEnv()` retrieves the active backend environment that is holding all connection details and connection pools

-   `getBackendConn()` get a *DBI* connection object to the specified database.

-   `getBackendInfo()` get the `backendInfo` object that holds connection details for the specified database.

-   `existingHashIDs()` get list of all the existing hash IDs

#### 1.2 Interacting with the backend

Functions for interacting with the backend and also table management.

-   `validBE()` check if backend exists and has a valid connection

-   `listTablesBE()` list tables in the backend

-   `existsTableBE()` check if a specific table exists within the backend

-   `tableBE()` create a *dbplyr* `tbl` connected to an existing table in the backend

-   `createTableBE()` create a new table. It is possible to include constraints such as a primary key and column typing and constrains.

-   `writeTableBE()` write a persistent table of values to the table (to be deprecated since `stream_to_db()` has taken over most of this role)

-   `dropTableBE()` remove a table from the backend

$~$

#### 2. Closing and reconnecting the backend

A specified backend can be closed by using `closeBackend()` along with a specific backend ID. Omitting any IDs will close all current GiottoDB backends.

Backend closing is typically not necessary to be manually run. All backends and associated connections will be finalized at session close.

```         
# keep backend connection details
bInfo = getBackendInfo(bID)

p # still valid
closeBackend(bID)
p # now invalid 
```

*GiottoDB* data objects request new pool connections to the database from the backend when the connections are closed or otherwise become invalid.

When the backend itself is closed, the user must reconnect the backend. Reconnections to an existing backend can be performed using the associated `backendInfo` object using `reconnectBackend()`. Alternatively, `createBackend()` can be called again with the same parameters to regenerate it (no overwriting or deletion will happen).

```         
reconnectBackend()
p = getBackendPool(bID)
p # new valid pool connection
```

### 3. Database settings

Database and PRAGMA settings can be retrieved and set using `dbSettings()`

```         
dbSettings(bID, 'memory_limit')
dbSettings(bID, 'memory_limit', '2GB')
```

Some settings have dedicated convenience functions like `dbMemoryLimit()`

$~$

### Writing values into the database

#### 1. Creating objects

*GiottoDB* currently contains representations for spatial points and polygons information as `dbPointsProxy` and `dbPolygonProxy` respectively. The function `dbvect()` can be used to generate these objects starting from a `data.frame` or *terra* `SpatVector`. Of course, if a scalable solution is necessary, then having a prepared in-memory object to convert from is often a luxury, so with `character` (filepath) inputs, `dbvect()` will also hook into `stream_to_db()`, a function that can be used to perform chunked reads from file and writes into the database. `dbvect()` will return a db object of the desired type after running.

Of note, `dbvect()` has an `overwrite` param that allows overwriting of existing data when set to `TRUE`. If it is instead set to "append", then new information will be appended to the previously existing information.

#### 2. Chunked reading

Data that requires out of memory processing is often too large to load into memory and be written into the database in a single step. *GiottoDB* reads in values in a chunked manner to get around this.

`stream_to_db()` is the core function that handles this. A table is created using `createTableBE()`, then chunks are iteratively read into memory, checked for whether a break condition is satisfied that would terminate the looping, modified on the spot by any provided callback function, then finally read into the database. These functions (and also the object creation functions) expose parameters for how the chunked reading occurs. Important params include:

-   `p` connection object to use

-   `remote_name` name of table in database to write to

-   `read_fun` function to parse the input file into a manner ingestible by the database. Must expose formal arguments `x`, `n`, and `i`, where `x` is the filepath, `n` is the number of 'units' to read in per chunk, and `i` is which round of chunked reading (0-indexed) is currently being performed. The 'units' defined for `n` vary depending on what kind of data it is. In many cases, `n` is sufficient to be the number of rows per chunk to read in, however, when data is grouped and a simple approach will split apart data across chunks that should not be split apart, 'units' should then refer to these groupings.

-   `n` described in `read_fun`. This value is passed from `stream_to_db` to the write function.

-   `callback` a single param and single output function that will take a chunk as input, then modify it into the format expected by the `write_fun` and db representation. A default function is supplied to perform no modifications.

-   `stop_cond` is a function to run on each chunk of read-in data. Will break the chunk read looping when it evaluates to `TRUE`. A default function is provided (`function(x) nrow(x) == 0L`) that will stop the looping when a chunk is read in that has 0 rows.

-    `write_fun` is a function that controls how the chunk is written to the database. Default functions will automatically run for `data.table` and *terra* `SpatVector` chunks. Custom functions should be defined for specific situations such as when more than one table should be created per chunk of data read-in. Required params to expose are `p`, `remote_name`, `x`, and `…`.

-   `overwrite` whether to overwrite if table already exists (default is `FALSE`)

-   `custom_table_fields` (advanced) specify SQL specific data types and constraints for columns to use when creating the database table for this data. If not provided for any column, default schemas decided by *DBI* will be used for that column.

-   `pk` assign a primary key column when creating the database table

-   ... additional params that are passed to the `write_fun`

A timestamped log of what table is being written, which chunk, and how many lines were written per chunk is saved to the same directory as the database by default.

### GiottoDB objects and the Backend

`dbData` is a class inherited by all GiottoDB data representation classes. It has slots 4 slots:

-   `data` a *dbplyr* `tbl` connected to a database table that contains the data needed for the object

-   `remote_name` name of table in backend that contains the data that this object references

-   `hash` hash ID of backend it should be connected to

-   `init` whether the object has been initialized and is ready for usage

dbData objects respond to several functions for accessing each of the slots and editing its behavior.

-   `cPool()` get the connection `pool` that the object is using

-   `queryStack()`/`queryStack<-()` get and set the delayed evaluations that are applied on the *dbplyr* `tbl` in the data slot.

-   `remoteName()` get the remote_name slot contents

-   `disconnect()` disconnect the backend that the object is associated with

-   `reconnect()` reconnect the object to the backend if its `pool` object is no longer valid
