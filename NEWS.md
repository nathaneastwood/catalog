# catalog 0.1.0

This is the initial release of the {catalog} package. This release adds the full suite of functionality from the Catalog API including:

- Caching
  - `cache_table()`
  - `clear_cache()`
  - `is_cached()`
  - `refresh_by_path()`
  - `refresh_table()`
  - `uncache_table()`
- Columns
  - `list_columns()`
- Database
  - `current_database()`
  - `database_exists()`
  - `list_databases()`
  - `set_current_database()`
- Functions
  - `function_exists()`
  - `get_function()`
  - `list_functions()`
- Partitioning
  - `recover_partitions()`
- Tables
  - `create_table()`
  - `get_table()`
  - `list_tables()`
  - `table_exists()`
- Temporary Views
  - `drop_global_temp_view()`
  - `drop_temp_view()`
