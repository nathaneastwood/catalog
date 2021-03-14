if (at_home()) {
  sc <- suppressMessages(sparklyr::spark_connect(master = "local"))

  expect_equal(
    current = catalog::current_database(sc = sc),
    target = "default",
    info = "current_database() returns the name of the current database"
  )

  expect_true(
    current = catalog::database_exists(sc = sc, name = "default"),
    info = "database_exists() correctly identifies if a database exists"
  )
  expect_false(
    current = catalog::database_exists(sc = sc, name = "fake_database"),
    info = "database_exists() correctly identifies if a database does not exist"
  )

  ld <- catalog::list_databases(sc = sc)
  expect_inherits(
    current = ld,
    class = c("tbl_df", "tbl", "data.frame"),
    info = "list_databases() returns a tibble"
  )
  expect_equal(
    current = colnames(ld),
    target = c("name", "description", "locationUri"),
    info = "list_databases() returns the expected columns"
  )

  DBI::dbGetQuery(
    conn = sc,
    statement = dplyr::sql("CREATE DATABASE catalog_test_database_db")
  )
  expect_true(
    current = catalog::set_current_database(
      sc = sc,
      name = "catalog_test_database_db"
    ),
    info = "set_current_database() correctly sets the current database"
  )
  catalog::set_current_database(sc = sc, name = "default")
  DBI::dbGetQuery(
    conn = sc,
    statement = dplyr::sql("DROP DATABASE catalog_test_database_db")
  )

  sparklyr::spark_disconnect(sc = sc)
}
