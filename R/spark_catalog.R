#' Access the Metastore Management Interface
#'
#' @param sc A `spark_connection`.
#'
#' @return
#' A `spark_jobj`.
#'
#' @importFrom sparklyr connection_is_open invoke spark_session
#' @noRd
spark_catalog <- function(sc) {
  stopifnot(sparklyr::connection_is_open(sc))
  ss <- sparklyr::spark_session(sc)
  sparklyr::invoke(ss, "catalog")
}
