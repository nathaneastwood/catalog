#' Access the Metastore Management Interface
#'
#' @param sc A `spark_connection`.
#'
#' @return
#' A `spark_jobj`.
#'
#' @importFrom sparklyr invoke spark_session
#' @noRd
spark_catalog <- function(sc) {
  ss <- sparklyr::spark_session(sc)
  sparklyr::invoke(ss, "catalog")
}
