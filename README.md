# Archived, use https://github.com/pavlospt/rust-pgdatadiff, it's much better.
# PGDataDiff

[![asciicast](https://asciinema.org/a/281974.svg)](https://asciinema.org/a/281974)

## Introduction

This is a small utility that given 2 PostgreSQL databases, will tell you what tables have different data. Specificaly it was developed to test replication is working correctly.

It compares table data and sequences. It won't tell you _exactly_ what rows are different but a range of rows that are different (depending on `--chunk-size` parameter)

## What it does not

Doesn't check that the schemas are the same.. i.e. stored procedures, indexes, contraints.. For this use a tool like https://www.postgrescompare.com/ or diff a schema dump from both databases.

### How does it determine if table data is different?

Firstly it compares the row count in both tables.

If the row count is the same, it instructs postgres to create MD5 sums of "chunks" of the table in both DBs and compares them. This way no data is actually read directly by `pgdatadiff`, it also means that `pgdatadiff` is relatively fast but is puts a moderate amount of pressure on the DB as it calculates the MD5 sums of large amounts of data. The MD5 sums are based on the data being cast to `varchar`. If you have data types that don't cast to `varchar` properly then the behaviour probably wont be reliable.

If you have tables that have many columns, perhaps consider using a smaller `--chunk-size`, the default is 10000. Conversely if your tables have a small amount of columns with 100000's of rows, perhaps increase this value(it can increase the speed significantly).

## Installation

The latest version can be installed using `pip install pgdatadiff`. Python 3.6+ is required.

## Usage

Check `pgdatadiff --help`

## Docker Images

Docker images are available.

`docker run -it davidjmarkey/pgdatadiff:0.2.1 /usr/bin/pgdatadiff`


