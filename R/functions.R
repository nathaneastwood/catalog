#' Check If A Function Exists
#'
#' Check if the function with the specified name exists in the specified
#' database.
#'
#' @details
#' `catalog_function_exists()` includes in-built functions such as `abs`. To see
#' if a built-in function exists you must use the unqualified name. If you
#' create a function you can use the qualified name. If you want to check if a
#' built-in function exists specify the `database` as `NULL`.
#'
#' @param sc A `spark_connection`.
#' @param fn `character(1)`. The name of the function.
#' @param database `character(1)`. The name of the database for which the
#' functions should be listed (default: `NULL`).
#'
#' @return
#' A `logical(1)` vector indicating `TRUE` if the function exists within the
#' specified database and `FALSE` otherwise.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' catalog_function_exists(sc = sc, fn = "abs")
#' }
#'
#' @export
catalog_function_exists <- function(sc, fn, database = NULL) {
  check_character_one(fn)
  if (!is.null(database)) {
    check_character_one(database)
    db_exists <- catalog_database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "functionExists", database, fn)
  } else {
    invoke_catalog(sc = sc, method = "functionExists", fn)
  }
}

#' Get A Function
#'
#' Get the function with the specified name.
#'
#' @details
#' If you are trying to get an in-built function then use the unqualified name
#' and pass `NULL` as the `database` name.
#'
#' @inheritParams catalog_function_exists
#'
#' @return
#' A `spark_jobj` which includes the class name, database, description, whether
#' it is temporary and the name of the function.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' catalog_get_function(sc = sc, fn = "Not")
#' }
#'
#' @seealso
#' [catalog_function_exists()], [catalog_list_functions()]
#'
#' @export
catalog_get_function <- function(sc, fn, database = NULL) {
  check_character_one(fn)
  if (!is.null(database)) {
    check_character_one(database)
    db_exists <- catalog_database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "getFunction", database, fn)
  } else {
    invoke_catalog(sc = sc, method = "getFunction", fn)
  }
}

#' List Functions
#'
#' Returns a list of functions registered in the specified database. This
#' includes all temporary functions. The result contains the class name,
#' database, description, whether it is temporary and the name of each function.
#'
#' @inheritParams catalog_function_exists
#'
#' @return
#' A `tibble` containing 5 columns:
#' * `name` - Name of the function.
#' * `database` - Name of the database the function belongs to.
#' * `description` - Description of the function.
#' * `className` - The fully qualified class name of the function.
#' * `isTemporary` - Whether the function is temporary or not.
#'
#' @examples
#' \dontrun{
#' sc <- sparklyr::spark_connect(master = "local")
#' catalog_list_functions(sc = sc)
#' catalog_list_functions(sc = sc, database = "default")
#' }
#'
#' @seealso
#' [catalog_function_exists()], [catalog_get_function()]
#'
#' @importFrom sparklyr collect
#' @export
catalog_list_functions <- function(sc, database = NULL) {
  functions <- if (!is.null(database)) {
    check_character_one(database)
    db_exists <- catalog_database_exists(sc = sc, name = database)
    if (isFALSE(db_exists)) {
      stop("Database ", sQuote(database), " does not exist.")
    }
    invoke_catalog(sc = sc, method = "listFunctions", database)
  } else {
    invoke_catalog(sc = sc, method = "listFunctions")
  }
  sparklyr::collect(functions)
}
