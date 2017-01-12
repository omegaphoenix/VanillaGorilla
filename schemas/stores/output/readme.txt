Dummy Data:
===========

This data is good for exercising NanoDB and seeing how fast it is on
different join operations.

stores-10K.sql
--------------

This file is generated with 60 stores, and between 40 and 300 employees
per store.  This is more like a WalMart-type store.

The 'employees' table is the largest table of this data-set, containing
10331 rows.

stores-28K.sql
--------------

This file is generated with 2000 stores, and between 5 and 20 employees
per store.  This is more like a Starbucks-type store.

The 'stores' table now has a bit of heft, containing 2000 records and 4
data pages (at 8KB/page, and with 1 header page).

The 'employees' table is nearly 1MB in size, and contains 24932 records.
