if (at_home()) {
  sc <- suppressMessages(sparklyr::spark_connect(master = "local"))
  mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)

  expect_true(
    current = catalog::drop_temp_view(sc = sc, view = "mtcars"),
    info = "drop_temp_view() returns TRUE when it drops a table"
  )
  expect_false(
    current = catalog::table_exists(sc = sc, table = "mtcars"),
    info = "Confirmation that drop_temp_view() dropped the table"
  )
  expect_false(
    current = suppressMessages(
      catalog::drop_temp_view(sc = sc, view = "catalog_fake_table")
    ),
    info = "drop_temp_view() returns FALSE when the table does not exist"
  )
  expect_message(
    current = catalog::drop_temp_view(sc = sc, view = "catalog_fake_table"),
    pattern = "'catalog_fake_table' does not exist.",
    info = "drop_temp_view() messages the user when the table does not exist"
  )

  sparklyr::spark_disconnect(sc = sc)
}
