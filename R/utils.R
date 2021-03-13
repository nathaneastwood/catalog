#' Check Parameter Is A `character(1)`
#'
#' @param x An R object
#'
#' @return
#' If successful, `invisible(NULL)`, otherwise errors.
#'
#' @noRd
check_character_one <- function(x) {
  stopifnot(is.character(x = x), length(x = x) == 1L)
}
