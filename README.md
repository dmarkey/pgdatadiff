# PGDataDiff
Small python utility to diff the data between 2 postgres databases.

## Introduction

This is a small utility that given 2 postgres databases, will tell you what tables have different data. Specificaly is was developed to test replication is working correctly between 2 postgres databases.

Specifically is compares tables data and sequences.

### How does it determine if table data is different?

Firstly it compares the row count in both tables.

If the row count is the same, it gets postgres to create MD5 sums of "chunks" of the table. This way no data is actually read directly by `pgdatadiff`, it also means that `pgdatadiff` is relatively fast but is puts the databases a moderate amount of pressure as it calculates the MD5 sums of large amounts of data.

# Installation

The latest version can be installed using `pip install pgdatadiff`. Python 3.6+ is required.

# Usage

TODO.
