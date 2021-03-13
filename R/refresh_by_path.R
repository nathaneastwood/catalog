#' Refresh By Path
#'
#' Invalidates and refreshes all the cached data (and the associated metadata)
#' for any Dataset that contains the given data source path. Path matching is by
#' prefix, i.e. "/" would invalidate everything that is cached.
#'
#' @param sc A `spark_connection`.
#' @param path `character(1)`. The path to refresh.
#'
#' @export
refresh_by_path <- function(sc, path) {
  check_character_one(x = path)
  invoke_catalog(sc = sc, method = "refreshByPath", path)
}
