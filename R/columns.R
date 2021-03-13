#' List Columns
#'
#' Returns a list of columns for the given table/view in the specified database.
#' The result includes the name, description, dataType, whether it is nullable
#' or if it is partitioned and if it is broken in buckets.
#'
#' @param sc A `spark_connection`.
#' @param table `character(1)`. The name of the table.
#' @param database `character(1)`. The name of the database for which the
#' functions should be listed (default: `NULL`).
#'
#' @return
#' A `tibble` with 6 columns:
#' * `name` - The name of the column.
#' * `description` - Description of the column.
#' * `dataType` - The column data type.
#' * `nullable` - Whether the column is nullable or not.
#' * `isPartition` - Whether the column is partitioned or not.
#' * `isBucket` - Whether the column is broken in buckets.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)
#' list_columns(sc = sc, table = "mtcars")
#' }
#'
#' @importFrom sparklyr collect
#' @export
list_columns <- function(sc, table, database = NULL) {
  check_character_one(table)
  columns <- if (!is.null(database)) {
    check_character_one(database)
    db_exists <- database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "listColumns", database, table)
  } else {
    invoke_catalog(sc = sc, method = "listColumns", table)
  }
  sparklyr::collect(columns)
}
