The tiny file is best to start with, since it only has 20 rows.

After that, the small_*.sql through huge_*.sql files have data-sets
of varying sizes for exercising the index behavior:

* small_*.sql should only require one leaf page in the index

* large_*.sql should require multiple leaves, but only one inner page

* huge_*.sql should require multiple inner pages and many leaves

Note that some of the data files may take a minute or more to run on
some machines.
