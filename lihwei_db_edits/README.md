`deletions.sql`: put in SQL code to REMOVE desired rows from tables
`insertions.xlsx`: fill out the spreadsheet to ADD row into tables

To replace / update values, first remove the rows using `deletions.sql`, then add rows of new data with insertions.xlsx

Turn `insertions.xlsx` into an sqlite database with `xlsx_to_sqlite()` in `lihwei_db_edits/lihwei_db_edits.py`

Update a desired database with insertions and deletions with `replace_subset()` in `db_processing\update_database\subset_replacement.py`.