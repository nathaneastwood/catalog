if (at_home()) {
  sc <- suppressMessages(sparklyr::spark_connect(master = "local"))
  mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)

  lc <- catalog::list_columns(sc = sc, table = "mtcars")
  expect_inherits(
    current = lc,
    class = c("tbl_df", "tbl", "data.frame"),
    info = "list_columns() returns a tibble"
  )
  expect_equal(
    current = colnames(lc),
    target = c(
      "name", "description", "dataType", "nullable", "isPartition", "isBucket"
    ),
    info = "list_columns() returns the expected columns"
  )
  expect_equal(
    current = lc[["name"]],
    target = colnames(mtcars),
    info = "list_columns() returns a single row representing each column"
  )

  sparklyr::spark_disconnect(sc = sc)
}
