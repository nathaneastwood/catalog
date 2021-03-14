if (at_home()) {
  sc <- suppressMessages(sparklyr::spark_connect(master = "local"))

  tmp <- tempfile()
  write.csv(x = mtcars, file = tmp, row.names = FALSE)

  mtcars_spark <- catalog::create_table(
    sc = sc,
    table = "mtcars",
    path = tmp,
    source = "csv"
  )
  catalog::cache_table(sc = sc, table = "mtcars")
  nrows <- sparklyr::sdf_nrow(mtcars_spark)

  write.csv(x = rbind(mtcars, mtcars), file = tmp, row.names = FALSE)
  nrows_after_rewrite <- sparklyr::sdf_nrow(mtcars_spark)

  expect_equal(
    current = nrows_after_rewrite,
    target = nrows,
    info = "By default, the data are not refreshed"
  )

  catalog::refresh_by_path(sc = sc, path = tmp)

  expect_equal(
    current = sparklyr::sdf_nrow(mtcars_spark),
    target = 65,
    info = "refresh_by_path() refreshes the cached tables in that path"
  )

  sparklyr::spark_disconnect(sc = sc)
}
