if (at_home()) {
  sc <- suppressMessages(sparklyr::spark_connect(master = "local"))

  expect_true(
    current = catalog::function_exists(sc = sc, fn = "Not"),
    info = "function_exists() can correctly identify a function"
  )
  expect_false(
    current = catalog::function_exists(sc = sc, fn = "catalog_fake_function"),
    info = "function_exists() returns FALSE when it cannot find a function"
  )

  expect_inherits(
    current = catalog::get_function(sc = sc, fn = "Abs"),
    class = c("spark_jobj", "shell_jobj"),
    info = "get_function() returns the expected spark_jobj"
  )

  lf <- catalog::list_functions(sc = sc)
  expect_inherits(
    current = lf,
    class = c("tbl_df", "tbl", "data.frame"),
    info = "list_functions() returns a tibble"
  )
  expect_equal(
    current = colnames(lf),
    target = c("name", "database", "description", "className", "isTemporary"),
    info = "list_functions() returns the expected columns"
  )

  sparklyr::spark_disconnect(sc = sc)
}
