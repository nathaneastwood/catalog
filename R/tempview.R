#' Temporary View
#'
#' * `drop_global_temp_view()`: Drops the global temporary view with the given
#' view name in the catalog.
#' * `drop_temp_view()`: Drops the local temporary view with the given view name
#' in the catalog. Local temporary view is session-scoped. Its lifetime is the
#' lifetime of the session that created it, i.e. it will be automatically
#' dropped when the session terminates. It's not tied to any databases.
#'
#' @param sc A `spark_connection`.
#' @param view `character(1)`. The name of the temporary view to be dropped.
#'
#' @return
#' A `logical(1)` vector indicating whether the temporary view was dropped
#' (`TRUE`) or not (`FALSE`).
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)
#'
#' # We can check which temporary tables are in scope
#' list_tables(sc = sc)
#'
#' # And then drop those we wish to drop
#' drop_temp_view(sc = sc, view = "mtcars")
#' }
#'
#' @seealso
#' [list_tables()]
#'
#' @export
drop_global_temp_view <- function(sc, view) {
  check_character_one(x = view)
  invoke_catalog(sc = sc, method = "dropGlobalTempView", view)
}

#' @rdname drop_global_temp_view
#' @export
drop_temp_view <- function(sc, view) {
  check_character_one(x = view)
  if (!table_exists(sc = sc, table = view)) {
    message(sQuote(x = view), " does not exist.")
    return(FALSE)
  }
  invoke_catalog(sc = sc, method = "dropTempView", view)
}
