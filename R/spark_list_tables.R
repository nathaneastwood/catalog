#' List Tables In A Spark Connection
#'
#' Returns a list of tables/views in the current database. The result includes
#' the name, database, description, table type and whether the table is
#' temporary or not.
#'
#' @param sc A `spark_connection`.
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
#' @export
catalog_list_tables <- function(sc, database = NULL) {
  catalog <- spark_catalog(sc)
  tables <- if (!is.null(database)) {
    sparklyr::invoke(catalog, "listTables", database)
  } else {
    sparklyr::invoke(catalog, "listTables")
  }
  sparklyr::collect(tables)
}
