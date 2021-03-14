#' Get The Current Database
#'
#' Returns the current database in this session. By default your session will be
#' connected to the "default" database (named "default") and to change database
#' you can use [set_current_database()].
#'
#' @param sc A `spark_connection`.
#'
#' @return
#' `character(1)`, the current database name.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' current_database(sc = sc)
#' }
#'
#' @seealso
#' [set_current_database()], [database_exists()], [list_databases()]
#'
#' @importFrom sparklyr invoke
#' @export
current_database <- function(sc) {
  invoke_catalog(sc = sc, method = "currentDatabase")
}

#' Check If A Database Exists
#'
#' Check if the database with the specified name exists. This will check the
#' list of hive databases in the current session to see if the database exists.
#'
#' @param sc A `spark_connection`.
#' @param name `character(1)`. The name of the database to set the current
#' database to.
#'
#' @return
#' `character(1)`, the current database name.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' database_exists(sc = sc, name = "default")
#' database_exists(sc = sc, name = "fake_database")
#' }
#'
#' @seealso
#' [current_database()], [set_current_database()], [list_databases()]
#'
#' @return
#' A `logical(1)` vector indicating `TRUE` if the database exists and `FALSE`
#' otherwise.
#'
#' @importFrom sparklyr invoke
#' @export
database_exists <- function(sc, name) {
  check_character_one(x = name)
  invoke_catalog(sc = sc, method = "databaseExists", name)
}

#' List Databases
#'
#' Returns a list of databases available across all sessions. The result
#' contains the name, description and locationUri of each database.
#'
#' @param sc A `spark_connection`.
#'
#' @return
#' A `tibble` containing 3 columns:
#' * `name` - The name of the database.
#' * `description` - Description of the database.
#' * `locationUri` - Path (in the form of a uri) to data files.
#'
#' @seealso
#' [current_database()], [database_exists()], [set_current_database()]
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' list_databases(sc = sc)
#' }
#'
#' @export
list_databases <- function(sc) {
  databases <- invoke_catalog(sc = sc, method = "listDatabases")
  sparklyr::collect(x = databases)
}

#' Set The Current Database
#'
#' Sets the current default database in this session.
#'
#' @param sc A `spark_connection`.
#' @param name `character(1)`. The name of the database to set the current
#' database to.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' set_current_database(sc = sc, name = "new_db")
#' }
#'
#' @seealso
#' [current_database()], [database_exists()], [list_databases()]
#'
#' @return
#' If successful, `TRUE`, otherwise errors.
#'
#' @importFrom sparklyr invoke
#' @export
set_current_database <- function(sc, name) {
  check_character_one(x = name)
  db_exists <- database_exists(sc = sc, name = name)
  if (isFALSE(db_exists)) stop("Database ", sQuote(name), " does not exist.")
  invoke_catalog(sc = sc, method = "setCurrentDatabase", name)
  if (current_database(sc = sc) == name) TRUE else FALSE
}
