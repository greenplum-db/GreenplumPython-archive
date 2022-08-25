---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.1
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

### Selecting the Database of Your Data

To begin with, we need to select the database that contains the data we want:

```python pycharm={"name": "#%%\n"}
import greenplumpython as gp

db = gp.database(host="localhost", dbname="gpadmin")
```

### Accessing a Table in the Database

After selecting the database, we can access a table in the database by specifying its name:

```python pycharm={"name": "#%%\n"}
t = gp.table("demo", db=db)
t
```

And of course, we can `SELECT` the first ordered N rows of a table, like this:

```python pycharm={"name": "#%%\n"}
t.order_by(t["i"]).head(10)
```

### Basic Data Manipulation

Now we have a table. We can do basic data manipulation on it, just like in SQL.

For example, we can `SELECT` a subset of its columns:

```python pycharm={"name": "#%%\n"}
t_ij = t[["i", "j"]]
t_ij
```

And we can also `SELECT` a subset of its rows. Say we want all the even numbers:

```python pycharm={"name": "#%%\n"}
t_even = t_ij[t_ij["i"] % 2 == 0]
t_even
```

For a quick glance, we can `SELECT` the first unordered N rows of a table, like this:

```python pycharm={"name": "#%%\n"}
t_n = t_even[:3]
t_n
```

Finally when we are done, we can save the resulting table to the database, either temporarily or persistently:

```python pycharm={"name": "#%%\n"}
t_n.save_as(table_name="t_n", temp=True)
```

<!-- #region pycharm={"name": "#%% md\n"} -->
### `JOIN`-ing Two Tables

We can also `JOIN` two tables with GreenplumPython. For example, suppose we have two tables like this:
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
rows = [(1, "'a'",), (2, "'b'",), (3, "'c'",), (4, "'d'")]
t1 = gp.values(rows, db=db, column_names=["id, val"])
t1
```

```python pycharm={"name": "#%%\n"}
rows = [(1, "'a'",), (2, "'b'",), (3, "'a'",), (4, "'b'")]
t2 = gp.values(rows, db=db, column_names=["id, val"])
t2
```

<!-- #region pycharm={"name": "#%% md\n"} -->
We can `JOIN` the two table like this:
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
t_join = t1.inner_join(
    t2,
    cond=t1["val"] == t2["val"],
    targets=[
        t1["id"].rename("t1_id"),
        t1["val"].rename("t1_val"),
        t2["id"].rename("t2_id"),
        t2["val"].rename("t2_val"),
    ],
)
t_join
```

<!-- #region pycharm={"name": "#%% md\n"} -->
### Creating and Calling Functions

Calling functions is essential for data analytics. GreenplumPython supports creating Greenplum UDFs and UDAs from Python functions and calling them in Python.

Suppose we have a table of numbers:
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
rows = [(i,) for i in range(10)]
numbers = gp.values(rows, db=db, column_names=["val"])
numbers
```

<!-- #region pycharm={"name": "#%% md\n"} -->
If we want to get the square of each number, we can write a function to do that:
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
@gp.create_function
def square(a: int) -> int:
    return a ** 2

square(numbers["val"], as_name="result", db=db).to_table()
```

<!-- #region pycharm={"name": "#%% md\n"} -->
Note that this function is called in exactly the same way as ordinary Python functions.

If we also want to get the sum of these numbers, what we need is to write an aggregate function like this:
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
@gp.create_aggregate
def my_sum(result: int, val: int) -> int:
    if result is None:
        return val
    return result + val

my_sum(numbers["val"], as_name="result", db=db).to_table()
```

```python

```
