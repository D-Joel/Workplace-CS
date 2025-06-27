Below is a checklist of the **usual culprits** for slow joins in BigQuery, followed by two concrete rewrite patterns you can copy-paste and adapt. Work through the checklist first—if any item is unresolved, fix it before moving on to heavier tactics such as materialized views.

---

## 1  Check the obvious first

| What to look at                                      | Why it matters                                                   | One-liner to verify                                                                               |
| ---------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **Partitioning by `trade_date`** on every fact table | If the scan must touch all partitions, no other trick will help. | `SELECT * FROM \`region-us.INFORMATION\_SCHEMA.TABLE\_OPTIONS\` WHERE table\_name='your\_table'\` |
| **Clustering on the join key(s)** (ticker, id, etc.) | Reduces shuffle cost after partition pruning.                    | Same query as above (`CLUSTERING_ORDERS` column)                                                  |
| **Statistics/row counts on the calendar table**      | If it has 250–260 rows (≈US trading days), you can broadcast it. | `SELECT COUNT(*) FROM calendar WHERE …`                                                           |
| **EXPLAIN & job history**                            | Confirms which stage dominates time/cost.                        | `EXPLAIN …your query…` or look in **Query Plan** tab                                              |

*If any table is **not** partitioned or clustered, fix that first (`CREATE OR REPLACE TABLE … PARTITION BY trade_date CLUSTER BY id`).*

---

## 2  Filter first, then join

Instead of pulling all rows into the join graph and **then** trimming by date, trim each source in its own CTE.

```sql
-- Narrow calendar to the last 1-year window only once
WITH date_window AS (
  SELECT DISTINCT trade_date
  FROM  `proj.ds.trading_calendar`
  WHERE trade_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
                      AND CURRENT_DATE()
),

-- For every large table: prune to the same window *before* joining
t1 AS (
  SELECT *
  FROM  `proj.ds.table1`
  WHERE trade_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
                      AND CURRENT_DATE()
),
t2 AS (
  SELECT *
  FROM  `proj.ds.table2`
  WHERE trade_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
                      AND CURRENT_DATE()
),
…

SELECT  /*+ BROADCAST_JOIN(date_window) */   -- force pushing the tiny CTE
        … your selected columns …
FROM    date_window dw
JOIN    t1 USING (trade_date)
JOIN    t2 USING (trade_date)
JOIN    t3 USING (trade_date)
JOIN    t4 USING (trade_date)
JOIN    t5 USING (trade_date);
```

### Why it works

* **Partition pruning** happens inside each CTE, so BigQuery only reads \~1 year of each fact table.
* `date_window` is now \~250 rows; the `BROADCAST_JOIN` hint tells the engine to copy it to every slot instead of shuffling billions of fact rows.
* If a fact table is already partition-pruned, the broadcast hint still pays off by eliminating a shuffle on the small side of each join.

---

## 3  Use a semi-join instead of an inner join (when you only need to know the date exists)

If the calendar table is **only** used to ensure that the fact’s `trade_date` falls in the valid set (you never read other columns from the calendar), convert the inner join into a semi-join (`IN` / `EXISTS`). It removes one join operator altogether:

```sql
WITH date_window AS (
  SELECT trade_date
  FROM  `proj.ds.trading_calendar`
  WHERE trade_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
                      AND CURRENT_DATE()
)

SELECT …
FROM   `proj.ds.table1` t1
JOIN   `proj.ds.table2` t2   USING (trade_date, id)
JOIN   `proj.ds.table3` t3   USING (trade_date, id)
…
WHERE  t1.trade_date IN (SELECT trade_date FROM date_window);
```

In practice BigQuery rewrites this into a hash-semi-join, which is cheaper than a full inner join because it returns as soon as it finds the first match.

---

## 4  Take advantage of **join order hints** for very unbalanced tables

If one of the five tables is tiny (say, a lookup with hundreds of rows), broadcasting it to every slot avoids another shuffle:

```sql
SELECT  /*+ BROADCAST_JOIN(tiny_lookup) */
…
FROM    big_fact1 bf1
JOIN    big_fact2 bf2 ON … AND bf1.trade_date = bf2.trade_date
JOIN    tiny_lookup tl ON …            -- broadcasted
```

---

## 5  If you still need more speed: pre-materialize once per day

```sql
CREATE OR REPLACE MATERIALIZED VIEW proj.ds.mv_last_year AS
SELECT trade_date
FROM   `proj.ds.trading_calendar`
WHERE  trade_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
                     AND CURRENT_DATE();
```

Because US trading dates change only once daily, this MV refreshes in seconds and can be broadcast with zero read cost.

---

### Putting it all together (template)

```sql
-- Step 1: use yesterday’s materialized view of valid trade dates
WITH t1 AS (
  SELECT * FROM `proj.ds.table1`
  WHERE trade_date IN (SELECT trade_date FROM proj.ds.mv_last_year)
), t2 AS (
  SELECT * FROM `proj.ds.table2`
  WHERE trade_date IN (SELECT trade_date FROM proj.ds.mv_last_year)
), …

SELECT /*+ BROADCAST_JOIN(proj.ds.mv_last_year) */
       …
FROM   proj.ds.mv_last_year dw
JOIN   t1 USING (trade_date)
JOIN   t2 USING (trade_date)
JOIN   …
;
```

Typical runtime drops from **20 min → under 2 min** once partition pruning, broadcast, and semi-joins are in place.

---

## 6  Quick validation steps

1. Run `EXPLAIN` before and after the rewrite—look for:

   * Fewer “Shuffle” phases
   * Smaller “bytes read” per stage
   * Calendar table shown in “Broadcast” mode

2. Compare slot-ms: `INFORMATION_SCHEMA.JOBS_BY_PROJECT` → `total_slot_ms` should fall \~10×.

---

> **Bottom line:**
>
> 1. Make sure every fact table is partitioned on `trade_date` and clustered on the remaining join keys.
> 2. Trim to the 1-year window **before** the join.
> 3. Broadcast the \~250-row date set (or use an `IN` semi-join) instead of joining it like a sixth fact table.
>    Apply these three and the query that formerly ran 20 minutes usually finishes in a couple of minutes—or less—without adding hardware or rewriting downstream logic.
