#' Access the Metastore Management Interface
#'
#' @param sc A `spark_connection`.
#'
#' @return
#' A `spark_jobj`.
#'
#' @importFrom sparklyr connection_is_open invoke spark_session
#' @noRd
catalog <- function(sc) {
  stopifnot(sparklyr::connection_is_open(sc))
  ss <- sparklyr::spark_session(sc)
  sparklyr::invoke(ss, "catalog")
}

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
  catalog <- catalog(sc = sc)
  sparklyr::invoke(jobj = catalog, method = method, ...)
}
