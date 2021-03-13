<!-- README.md is generated from README.Rmd. Please edit that file -->

# {catalog}

[![CRAN
status](https://www.r-pkg.org/badges/version/catalog)](https://cran.r-project.org/package=catalog)
[![Dependencies](https://tinyverse.netlify.com/badge/catalog)](https://cran.r-project.org/package=catalog)
![CRAN downloads](https://cranlogs.r-pkg.org/badges/catalog)
[![codecov](https://codecov.io/gh/nathaneastwood/catalog/branch/master/graph/badge.svg)](https://codecov.io/gh/nathaneastwood/catalog)

## Overview

{catalog} gives the user access to the Spark Catalog API making use of
the ‘sparklyr’ API. Catalog is the interface for managing a metastore
(aka metadata catalog) of relational entities (e.g. database(s), tables,
functions, table columns and temporary views).

## Installation

You can install:

-   the development version from
    [GitHub](https://github.com/nathaneastwood/catalog) with

``` r
# install.packages("remotes")
remotes::install_github("nathaneastwood/catalog")
```

## Usage

{catalog} provides an API matching that of the Catalog API and provides
full access to all methods. Below is a small example of some of the
functionality.

``` r
sc <- sparklyr::spark_connect(master = "local")
mtcars_spark <- sparklyr::copy_to(dest = sc, df = mtcars)

library(catalog)

list_tables(sc)
# # A tibble: 1 x 5
#   name   database description tableType isTemporary
#   <chr>  <chr>    <chr>       <chr>     <lgl>      
# 1 mtcars <NA>     <NA>        TEMPORARY TRUE

list_columns(sc, "mtcars")
# # A tibble: 11 x 6
#    name  description dataType nullable isPartition isBucket
#    <chr> <chr>       <chr>    <lgl>    <lgl>       <lgl>   
#  1 mpg   <NA>        double   TRUE     FALSE       FALSE   
#  2 cyl   <NA>        double   TRUE     FALSE       FALSE   
#  3 disp  <NA>        double   TRUE     FALSE       FALSE   
#  4 hp    <NA>        double   TRUE     FALSE       FALSE   
#  5 drat  <NA>        double   TRUE     FALSE       FALSE   
#  6 wt    <NA>        double   TRUE     FALSE       FALSE   
#  7 qsec  <NA>        double   TRUE     FALSE       FALSE   
#  8 vs    <NA>        double   TRUE     FALSE       FALSE   
#  9 am    <NA>        double   TRUE     FALSE       FALSE   
# 10 gear  <NA>        double   TRUE     FALSE       FALSE   
# 11 carb  <NA>        double   TRUE     FALSE       FALSE

list_functions(sc)
# # A tibble: 298 x 5
#    name  database description className                              isTemporary
#    <chr> <chr>    <chr>       <chr>                                  <lgl>      
#  1 !     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  2 %     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  3 &     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  4 *     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  5 +     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  6 -     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  7 /     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  8 <     <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
#  9 <=    <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
# 10 <=>   <NA>     <NA>        org.apache.spark.sql.catalyst.express… TRUE       
# # … with 288 more rows

drop_temp_view(sc, "mtcars")
# [1] TRUE
```
