This project demonstrates a behavior change with the spark `grouping_id()` function between 3.1.3 and 3.2.1

# 3.1.3 output

The grouping_id bitmaps between the two queries are the same.

```----------------------
Start test: Grouping sets in same order as group by

SELECT 'col1' as col1,
       'col2' as col2,
       'col3' as col3,
       grouping_id()            as grouping_id,
       count(1)                 as rowCount
from values(1)
GROUP BY col1, col2, col3

GROUPING SETS (
    (col1),
    (col2, col3)
)

+----+----+----+-----------+--------+
|col1|col2|col3|grouping_id|rowCount|
+----+----+----+-----------+--------+
|col1|null|null|          3|       1|
|col1|col2|col3|          4|       1|
+----+----+----+-----------+--------+

Grouping bitmap and associated dimensions: 3 col1
Grouping bitmap and associated dimensions: 4 col2, col3
End test: Grouping sets in same order as group by
----------------------
Start test: Grouping sets in different order as group by

SELECT 'col1' as col1,
       'col2' as col2,
       'col3' as col3,
       grouping_id()            as grouping_id,
       count(1)                 as rowCount
from values(1)
GROUP BY col1, col2, col3

GROUPING SETS (
    (col2, col3),
    (col1)
)

+----+----+----+-----------+--------+
|col1|col2|col3|grouping_id|rowCount|
+----+----+----+-----------+--------+
|col1|null|null|          3|       1|
|col1|col2|col3|          4|       1|
+----+----+----+-----------+--------+

Grouping bitmap and associated dimensions: 3 col1
Grouping bitmap and associated dimensions: 4 col2, col3
End test: Grouping sets in different order as group by
```

# 3.2.1 output

The grouping_id bitmap changes between the two queries based on the order columns appear in the grouping sets clause.

```----------------------
Start test: Grouping sets in same order as group by

SELECT 'col1' as col1,
       'col2' as col2,
       'col3' as col3,
       grouping_id()            as grouping_id,
       count(1)                 as rowCount
from values(1)
GROUP BY col1, col2, col3

GROUPING SETS (
    (col1),
    (col2, col3)
)

+----+----+----+-----------+--------+
|col1|col2|col3|grouping_id|rowCount|
+----+----+----+-----------+--------+
|col1|null|null|          3|       1|
|col1|col2|col3|          4|       1|
+----+----+----+-----------+--------+

Grouping bitmap and associated dimensions: 3 col1
Grouping bitmap and associated dimensions: 4 col2, col3
End test: Grouping sets in same order as group by
----------------------
Start test: Grouping sets in different order as group by

SELECT 'col1' as col1,
       'col2' as col2,
       'col3' as col3,
       grouping_id()            as grouping_id,
       count(1)                 as rowCount
from values(1)
GROUP BY col1, col2, col3

GROUPING SETS (
    (col2, col3),
    (col1)
)

+----+----+----+-----------+--------+
|col1|col2|col3|grouping_id|rowCount|
+----+----+----+-----------+--------+
|col1|col2|col3|          1|       1|
|col1|null|null|          6|       1|
+----+----+----+-----------+--------+

Grouping bitmap and associated dimensions: 1 col1, col2
Grouping bitmap and associated dimensions: 6 col3
End test: Grouping sets in different order as group by
```