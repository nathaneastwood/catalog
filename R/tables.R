#' Cache And Uncache Tables
#'
#' Spark SQL can cache tables using an in-memory columnar format by calling
#' `cache_table()`. Spark SQL will scan only required columns and will
#' automatically tune compression to minimize memory usage and GC pressure.
#' You can call `uncache_table()` to remove the table from memory. Similarly you
#' can call `clear_cache()` to remove all cached tables from the in-memory
#' cache. Finally, use `is_cached()` to test whether or not a table is cached.
#'
#' @inheritParams get_table
#'
#' @seealso
#' [create_table()], [get_table()], [list_tables()], [refresh_table()],
#' [table_exists()], [uncache_table()]
#'
#' @return
#' * `cache_table()`: If successful, `TRUE`, otherwise `FALSE`.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)
#'
#' # By default the table is not cached
#' is_cached(sc = sc, table = "mtcars")
#'
#' # We can manually cache the table
#' cache_table(sc = sc, table = "mtcars")
#' # And now the table is cached
#' is_cached(sc = sc, table = "mtcars")
#'
#' # We can uncache the table
#' uncache_table(sc = sc, table = "mtcars")
#' is_cached(sc = sc, table = "mtcars")
#' }
#'
#' @export
cache_table <- function(sc, table) {
  check_character_one(x = table)
  if (!table_exists(sc = sc, table = table)) {
    stop(sQuote(table), " does not exist")
  }
  if (is_cached(sc = sc, table = table)) {
    message(
      sQuote(table), " is already cached. Maybe you want `refresh_table()`?"
    )
    return(FALSE)
  }
  invoke_catalog(sc = sc, method = "cacheTable", table)
  is_cached(sc = sc, table = table)
}

#' @return
#' * `clear_cache()`: `NULL`, invisibly.
#' @rdname cache_table
#' @export
clear_cache <- function(sc) {
  invisible(invoke_catalog(sc = sc, method = "clearCache"))
}

#' Create A Table
#'
#' Creates a table, in the hive warehouse, from the given path and returns the
#' corresponding `DataFrame`. The table will contain the contents of the file
#' that is in the `path` parameter.
#'
#' @details
#' The default data source type is parquet.
#' This can be changed using `source` or setting the configuration option
#' `spark.sql.sources.default` when creating the spark session using or after
#' you have created the session using
#'
#' ```{r, eval = FALSE}
#' config <- sparklyr::spark_config()
#' config[["spark.sql.sources.default"]] <- "csv"
#' ```
#'
#' @inheritParams get_table
#' @param table `character(1)`. The name of the table to create.
#' @param path `character(1)`. The path to use to create the table.
#' @param source `character(1)`. The data source to use to create the table such
#' as `"parquet"`, `"csv"`, etc.
#' @param ... Additional options to be passed to the `createTable` method.
#'
#' @seealso
#' [cache_table()], [get_table()], [list_tables()], [refresh_table()],
#' [table_exists()], [uncache_table()]
#'
#' @return
#' A `tbl_spark`.
#'
#' @importFrom dplyr tbl
#' @export
create_table <- function(sc, table, path, source, ...) {
  check_character_one(table)
  check_character_one(path)
  check_character_one(source)
  invoke_catalog(sc = sc, method = "createTable", table, path, source, ...)
  dplyr::tbl(src = sc, table)
}

#' Get A Table
#'
#' Get the table or view with the specified name in the specified database. You
#' can use this to find the table's description, database, type and whether it
#' is a temporary table or not.
#'
#' @param sc A `spark_connection`.
#' @param table `character(1)`. The name of the table.
#' @param database `character(1)`. The name of the database for which the
#' functions should be listed (default: `NULL`).
#'
#' @seealso
#' [cache_table()], [create_table()], [list_tables()], [refresh_table()],
#' [table_exists()], [uncache_table()]
#'
#' @return
#' An object of class `spark_jobj` and `shell_jobj`.
#'
#' @export
get_table <- function(sc, table, database = NULL) {
  check_character_one(table)
  if (!is.null(database)) {
    check_character_one(database)
    db_exists <- database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "getTable", database, table)
  } else {
    invoke_catalog(sc = sc, method = "getTable", table)
  }
}

#' @return
#' * `is_cached()`: A `logical(1)` vector indicating `TRUE` if the table is
#' cached and `FALSE` otherwise.
#' @rdname cache_table
#' @export
is_cached <- function(sc, table) {
  check_character_one(x = table)
  invoke_catalog(sc = sc, method = "isCached", table)
}

#' List Tables In A Spark Connection
#'
#' Returns a list of tables/views in the current database. The result includes
#' the name, database, description, table type and whether the table is
#' temporary or not.
#'
#' @inheritParams get_table
#'
#' @return
#' A `tibble` containing 5 columns:
#' * `name` - The name of the table.
#' * `database` - Name of the database the table belongs to.
#' * `description` - Description of the table.
#' * `tableType` - The type of table (e.g. view/table)
#' * `isTemporary` - Whether the table is temporary or not.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spakr <- sparklyr::copy_to(dest = sc, df = mtcars)
#' list_tables(sc = sc)
#' }
#'
#' @seealso
#' [cache_table()], [create_table()], [get_table()], [refresh_table()],
#' [table_exists()], [uncache_table()]
#'
#' @importFrom sparklyr collect
#' @export
list_tables <- function(sc, database = NULL) {
  tables <- if (!is.null(database)) {
    invoke_catalog(sc = sc, method = "listTables", database)
  } else {
    invoke_catalog(sc = sc, method = "listTables")
  }
  sparklyr::collect(tables)
}

#' Refreshing Data
#'
#' * `recover_partitions()`: Recovers all the partitions in the directory of a
#' table and update the catalog. This only works for partitioned tables and not
#' un-partitioned tables or views.
#' * `refresh_by_path()`: Invalidates and refreshes all the cached data (and the
#' associated metadata) for any Dataset that contains the given data source
#' path. Path matching is by prefix, i.e. "/" would invalidate everything that
#' is cached.
#' * `refresh_table()`: Invalidates and refreshes all the cached data and
#' metadata of the given table. For performance reasons, Spark SQL or the
#' external data source library it uses might cache certain metadata about a
#' table, such as the location of blocks. When those change outside of Spark
#' SQL, users should call this function to invalidate the cache. If this table
#' is cached as an `InMemoryRelation`, drop the original cached version and make
#' the new version cached lazily.
#'
#' @param sc A `spark_connection`.
#' @param table `character(1)`. The name of the table.
#'
#' @return
#' `NULL`, invisibly. These functions are mostly called for their side effects.
#'
#' @seealso
#' [cache_table()], [create_table()], [get_table()], [list_tables()],
#' [table_exists()], [uncache_table()]
#'
#' @name refresh
#' @export
recover_partitions <- function(sc, table) {
  check_character_one(x = table)
  invisible(invoke_catalog(sc = sc, method = "recoverPartitions", table))
}

#' @param path `character(1)`. The path to refresh.
#' @rdname refresh
#' @export
refresh_by_path <- function(sc, path) {
  check_character_one(x = path)
  invisible(invoke_catalog(sc = sc, method = "refreshByPath", path))
}

#' @rdname refresh
#' @export
refresh_table <- function(sc, table) {
  check_character_one(table)
  invisible(invoke_catalog(sc = sc, method = "refreshTable", table))
}

#' Check If A Table Exists
#'
#' Check if the table or view with the specified name exists in the specified
#' database. This can either be a temporary view or a table/view.
#'
#' @inheritParams get_table
#'
#' @details
#' If `database` is `NULL`, `table_exists` refers to a table in the current
#' database (see [current_database()]).
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)
#' table_exists(sc = sc, table = "mtcars")
#' }
#'
#' @seealso
#' [cache_table()], [create_table()], [get_table()], [list_tables()],
#' [refresh_table()], [uncache_table()]
#'
#' @return
#' A `logical(1)` vector indicating `TRUE` if the table exists within the
#' specified database and `FALSE` otherwise.
#'
#' @export
table_exists <- function(sc, table, database = NULL) {
  check_character_one(table)
  if (!is.null(database)) {
    check_character_one(database)
    db_exists <- database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "tableExists", database, table)
  } else {
    invoke_catalog(sc = sc, method = "tableExists", table)
  }
}

#' @return
#' * `uncache_table()`: `NULL`, invisibly.
#' @rdname cache_table
#' @export
uncache_table <- function(sc, table) {
  check_character_one(x = table)
  if (!table_exists(sc = sc, table = table)) {
    stop(sQuote(table), " does not exist")
  }
  if (!is_cached(sc = sc, table = table)) {
    message(sQuote(table), " is not cached.")
    return(FALSE)
  }
  invoke_catalog(sc = sc, method = "uncacheTable", table)
  !is_cached(sc = sc, table = table)
}
