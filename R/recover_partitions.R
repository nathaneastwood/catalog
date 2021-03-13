#' Recover Partitions
#'
#' Recovers all the partitions in the directory of a table and update the
#' catalog. This only works for partitioned tables and not un-partitioned tables
#' or views.
#'
#' @param sc A `spark_connection`.
#' @param table `character(1)`. The name of the table.
#'
#' @export
recover_partitions <- function(sc, table) {
  check_character_one(x = table)
  invoke_catalog(sc = sc, method = "recoverPartitions", table)
}
