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
  expect_inherits(
    current = mtcars_spark,
    class = c("tbl_spark", "tbl_sql", "tbl_lazy", "tbl"),
    info = "create_table() returns a tbl_spark object"
  )

  expect_false(
    current = catalog::is_cached(sc, "mtcars"),
    info = "is_cached() returns FALSE when a table is not cached"
  )

  expect_true(
    current = catalog::cache_table(sc = sc, table = "mtcars"),
    info = "cache_table() can successfully cache tables"
  )
  expect_false(
    current = suppressMessages(catalog::cache_table(sc = sc, table = "mtcars")),
    info = "cache_table() will not attempt to recache a cached table"
  )
  expect_message(
    current = catalog::cache_table(sc = sc, table = "mtcars"),
    pattern = "'mtcars' is already cached. Maybe you want `refresh_table()`?",
    info = "cache_table() will message the user when caching a cached table"
  )
  expect_error(
    current = catalog::cache_table(sc = sc, table = "catalog_fake_table"),
    pattern = "'catalog_fake_table' does not exist",
    info = "cache_table() errors when the table doesn't exist"
  )

  expect_true(
    current = catalog::is_cached(sc = sc, table = "mtcars"),
    info = "is_cached() returns TRUE when a table is cached"
  )

  expect_true(
    current = catalog::uncache_table(sc = sc, table = "mtcars"),
    info = "uncache_table() returns TRUE when it successfully uncaches a table"
  )
  expect_error(
    current = catalog::uncache_table(sc = sc, table = "catalog_fake_table"),
    pattern = "'catalog_fake_table' does not exist",
    info = "uncache_table() errors when the table doesn't exist"
  )

  lt <- catalog::list_tables(sc = sc)
  expect_inherits(
    current = lt,
    class = c("tbl_df", "tbl", "data.frame"),
    info = "list_tables() returns a tibble"
  )
  expect_equal(
    current = colnames(x = lt),
    target = c("name", "database", "description", "tableType", "isTemporary"),
    info = "list_tables() returns the expected columnar information"
  )

  expect_inherits(
    current = catalog::get_table(sc = sc, table = "mtcars"),
    class = c("spark_jobj", "shell_jobj"),
    info = "get_table() returns the expected spark_jobj"
  )

  expect_true(
    current = catalog::table_exists(sc = sc, table = "mtcars"),
    info = "table_exists() returns TRUE when a table exists"
  )
  expect_false(
    current = catalog::table_exists(sc = sc, table = "catalog_fake_table"),
    info = "table_exists() returns FALSE when a table does not exist"
  )

  catalog::cache_table(sc = sc, table = "mtcars")
  sparklyr::copy_to(dest = sc, df = airquality)
  catalog::cache_table(sc = sc, table = "airquality")
  expect_true(
    current = catalog::is_cached(sc = sc, table = "mtcars") &
      catalog::is_cached(sc = sc, table = "airquality"),
    info = "Check that both tables are cached"
  )
  catalog::clear_cache(sc = sc)
  expect_false(
    current = catalog::is_cached(sc = sc, table = "mtcars") &
      catalog::is_cached(sc = sc, table = "airquality"),
    info = "clear_cache() clears all cached tables"
  )

  write.csv(x = rbind(mtcars, mtcars), file = tmp, row.names = FALSE)
  expect_equal(
    current = sparklyr::sdf_nrow(mtcars_spark),
    target = 34,
    info = "The source is updated but the Spark table is not"
  )
  catalog::refresh_table(sc = sc, table = "mtcars")
  expect_equal(
    current = sparklyr::sdf_nrow(mtcars_spark),
    target = 65,
    info = "refresh_table() updates the Spark object"
  )

  sparklyr::spark_disconnect(sc = sc)
}
