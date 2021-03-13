#' Invoke A Catalog Method
#'
#' @param sc A `spark_connection`.
#' @param method `character(1)`. The name of the method to invoke.
#' @param ... Additional parameters to be passed to `method`.
#'
#' @importFrom sparklyr invoke
#' @noRd
invoke_catalog <- function(sc, method, ...) {
  check_character_one(x = method)
  catalog <- spark_catalog(sc = sc)
  sparklyr::invoke(jobj = catalog, method = method, ...)
}
