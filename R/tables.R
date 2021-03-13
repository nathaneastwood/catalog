#' Cache And Uncache Tables
#'
#' Spark SQL can cache tables using an in-memory columnar format by calling
#' `catalog_cache_table(sc, "tableName")`. Spark SQL will scan only required
#' columns and will automatically tune compression to minimize memory usage and
#' GC pressure. You can call `catalog_uncache_table(sc, "tableName")` to remove
#' the table from memory. Similarly you can call `catalog_clear_cache(sc)` to
#' remove all cached tables from the in-memory cache.
#'
#' @inheritParams catalog_get_table
#'
#' @seealso
#' [catalog_create_table()], [catalog_get_table()], [catalog_list_tables()],
#' [catalog_refresh_table()], [catalog_table_exists()],
#' [catalog_uncache_table()]
#'
#' @return
#' * `catalog_cache_table()`: `NULL`, invisibly.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)
#'
#' # By default the table is not cached
#' catalog_is_cached(sc = sc, table = "mtcars")
#'
#' # We can manually cache the table
#' catalog_cache_table(sc = sc, table = "mtcars")
#' # And now the table is cached
#' catalog_is_cached(sc = sc, table = "mtcars")
#'
#' # We can uncache the table
#' catalog_uncache_table(sc = sc, table = "mtcars")
#' catalog_is_cached(sc = sc, table = "mtcars")
#' }
#'
#' @export
catalog_cache_table <- function(sc, table) {
  check_character_one(x = table)
  invisible(invoke_catalog(sc = sc, method = "cacheTable", table))
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
#' @inheritParams catalog_get_table
#' @param table `character(1)`. The name of the table to create.
#' @param path `character(1)`. The path to use to create the table.
#' @param source `character(1)`. The data source to use to create the table such
#' as `"parquet"`, `"csv"`, etc.
#' @param ... Additional options to be passed to the `createTable` method.
#'
#' @seealso
#' [catalog_cache_table()], [catalog_get_table()], [catalog_list_tables()],
#' [catalog_refresh_table()], [catalog_table_exists()],
#' [catalog_uncache_table()]
#'
#' @export
catalog_create_table <- function(sc, table, path, source, ...) {
  check_character_one(table)
  check_character_one(path)
  check_character_one(source)
  table <- invoke_catalog(
    sc = sc,
    method = "createTable",
    table, path, source, ...
  )
  sparklyr::collect(table)
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
#' [catalog_cache_table()], [catalog_create_table()], [catalog_list_tables()],
#' [catalog_refresh_table()], [catalog_table_exists()],
#' [catalog_uncache_table()]
#'
#' @export
catalog_get_table <- function(sc, table, database = NULL) {
  check_character_one(table)
  if (!is.null(database)) {
    check_character_one(database)
    db_exists <- catalog_database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "getTable", database, table)
  } else {
    invoke_catalog(sc = sc, method = "getTable", table)
  }
}

#' List Tables In A Spark Connection
#'
#' Returns a list of tables/views in the current database. The result includes
#' the name, database, description, table type and whether the table is
#' temporary or not.
#'
#' @inheritParams catalog_get_table
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
#' catalog_list_tables(sc = sc)
#' }
#'
#' @seealso
#' [catalog_cache_table()], [catalog_create_table()], [catalog_get_table()],
#' [catalog_refresh_table()], [catalog_table_exists()],
#' [catalog_uncache_table()]
#'
#' @importFrom sparklyr collect
#' @export
catalog_list_tables <- function(sc, database = NULL) {
  tables <- if (!is.null(database)) {
    invoke_catalog(sc = sc, method = "listTables", database)
  } else {
    invoke_catalog(sc = sc, method = "listTables")
  }
  sparklyr::collect(tables)
}

#' Refresh a Table
#'
#' Invalidates and refreshes all the cached data and metadata of the given
#' table. For performance reasons, Spark SQL or the external data source library
#' it uses might cache certain metadata about a table, such as the location of
#' blocks. When those change outside of Spark SQL, users should call this
#' function to invalidate the cache. If this table is cached as an
#' `InMemoryRelation`, drop the original cached version and make the new version
#' cached lazily.
#'
#' @inheritParams catalog_get_table
#'
#' @seealso
#' [catalog_cache_table()], [catalog_create_table()], [catalog_get_table()],
#' [catalog_list_tables()], [catalog_table_exists()], [catalog_uncache_table()]
#'
#' @return
#' `NULL`
#'
#' @export
catalog_refresh_table <- function(sc, table) {
  check_character_one(table)
  invoke_catalog(sc = sc, method = "refreshTable", table)
}

#' Check If A Table Exists
#'
#' Check if the table or view with the specified name exists in the specified
#' database. This can either be a temporary view or a table/view.
#'
#' @inheritParams catalog_get_table
#'
#' @details
#' If `database` is `NULL`, `catalog_table_exists` refers to a table in the
#' current database (see [catalog_current_database()]).
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)
#' catalog_table_exists(sc = sc, table = "mtcars")
#' }
#'
#' @seealso
#' [catalog_cache_table()], [catalog_create_table()], [catalog_get_table()],
#' [catalog_list_tables()], [catalog_refresh_table()], [catalog_uncache_table()]
#'
#' @return
#' A `logical(1)` vector indicating `TRUE` if the table exists within the
#' specified database and `FALSE` otherwise.
#'
#' @export
catalog_table_exists <- function(sc, table, database = NULL) {
  check_character_one(table)
  if (!is.null(database)) {
    check_character_one(database)
    db_exists <- catalog_database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "tableExists", database, table)
  } else {
    invoke_catalog(sc = sc, method = "tableExists", table)
  }
}

#' @return
#' * `catalog_uncache_table()`: `NULL`, invisibly.
#' @rdname catalog_cache_table
#' @export
catalog_uncache_table <- function(sc, table) {
  check_character_one(x = table)
  invisible(invoke_catalog(sc = sc, method = "uncacheTable", table))
}

#' @return
#' * `catalog_clear_cache()`: `NULL`, invisibly.
#' @rdname catalog_cache_table
#' @export
catalog_clear_cache <- function(sc) {
  invisible(invoke_catalog(sc = sc, method = "clearCache"))
}

#' @return
#' * `catalog_is_cached()`: A `logical(1)` vector indicating `TRUE` if the table
#' is cached and `FALSE` otherwise.
#' @rdname catalog_cache_table
#' @export
catalog_is_cached <- function(sc, table) {
  check_character_one(x = table)
  invoke_catalog(sc = sc, method = "isCached", table)
}
