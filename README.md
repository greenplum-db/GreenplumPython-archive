<img src="./doc/images/gppython_logo_text.svg">

GreenplumPython is a Python library that enables the user to interact with Greenplum in a Pythonic way.

GreenplumPython provides a [pandas](https://pandas.pydata.org/)-like table API that
1. looks familiar and intuitive to Python users
2. is powerful to do complex analytics, such as statistical analysis, with UDFs and UDAs
3. encapsulates common best practices and avoids common pitfalls in Greenplum, compared to writing SQL directly

## Getting Started

### Installation

To install the latest development version, do

```bash
pip3 install --user git+https://github.com/greenplum-db/GreenplumPython
```

NOTE: This version is considered UNSTABLE. DON'T use it in the production environment!

Stable version will be released soon.

### Build Doc Locally

```bash
pip3 install tox
tox -e docs
```

### Selecting the Database of Your Data

To begin with, we need to select the database that contains the data we want:


```python
import greenplumpython as gp


db = gp.database(host="localhost", dbname="gpadmin")
```

We will use the following utility function to display a table in HTML:
```python
from tabulate import tabulate


def display(t: gp.Table):
    return tabulate(t.fetch(), headers="keys", tablefmt="html")
```

### Accessing a Table in the Database

After selecting the database, we can access a table in the database by specifying its name:

```python
t = gp.table("demo", db=db)
display(t)
```

<table>
<thead>
<tr><th style="text-align: right;">  i</th><th style="text-align: right;">  j</th><th style="text-align: right;">  k</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">  3</td><td style="text-align: right;">  3</td><td style="text-align: right;">  3</td></tr>
<tr><td style="text-align: right;"> 10</td><td style="text-align: right;"> 10</td><td style="text-align: right;"> 10</td></tr>
<tr><td style="text-align: right;">  1</td><td style="text-align: right;">  1</td><td style="text-align: right;">  1</td></tr>
<tr><td style="text-align: right;">  4</td><td style="text-align: right;">  4</td><td style="text-align: right;">  4</td></tr>
<tr><td style="text-align: right;">  8</td><td style="text-align: right;">  8</td><td style="text-align: right;">  8</td></tr>
<tr><td style="text-align: right;">  2</td><td style="text-align: right;">  2</td><td style="text-align: right;">  2</td></tr>
<tr><td style="text-align: right;">  5</td><td style="text-align: right;">  5</td><td style="text-align: right;">  5</td></tr>
<tr><td style="text-align: right;">  6</td><td style="text-align: right;">  6</td><td style="text-align: right;">  6</td></tr>
<tr><td style="text-align: right;">  7</td><td style="text-align: right;">  7</td><td style="text-align: right;">  7</td></tr>
<tr><td style="text-align: right;">  9</td><td style="text-align: right;">  9</td><td style="text-align: right;">  9</td></tr>
</tbody>
</table>

### Basic Data Manipulation

Now we have a table. We can do basic data manipulation on it, just like in SQL.

For example, we can `SELECT` a subset of its columns:

```python
t_ij = t[["i", "j"]]
display(t_ij)
```

<table>
<thead>
<tr><th style="text-align: right;">  i</th><th style="text-align: right;">  j</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">  3</td><td style="text-align: right;">  3</td></tr>
<tr><td style="text-align: right;"> 10</td><td style="text-align: right;"> 10</td></tr>
<tr><td style="text-align: right;">  1</td><td style="text-align: right;">  1</td></tr>
<tr><td style="text-align: right;">  4</td><td style="text-align: right;">  4</td></tr>
<tr><td style="text-align: right;">  8</td><td style="text-align: right;">  8</td></tr>
<tr><td style="text-align: right;">  2</td><td style="text-align: right;">  2</td></tr>
<tr><td style="text-align: right;">  5</td><td style="text-align: right;">  5</td></tr>
<tr><td style="text-align: right;">  6</td><td style="text-align: right;">  6</td></tr>
<tr><td style="text-align: right;">  7</td><td style="text-align: right;">  7</td></tr>
<tr><td style="text-align: right;">  9</td><td style="text-align: right;">  9</td></tr>
</tbody>
</table>

And we can also `SELECT` a subset of its rows. Say we want all the even numbers:

```python
t_even = t_ij[t_ij["i"] % 2 == 0]
display(t_even)
```

<table>
<thead>
<tr><th style="text-align: right;">  i</th><th style="text-align: right;">  j</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;"> 10</td><td style="text-align: right;"> 10</td></tr>
<tr><td style="text-align: right;">  4</td><td style="text-align: right;">  4</td></tr>
<tr><td style="text-align: right;">  8</td><td style="text-align: right;">  8</td></tr>
<tr><td style="text-align: right;">  2</td><td style="text-align: right;">  2</td></tr>
<tr><td style="text-align: right;">  6</td><td style="text-align: right;">  6</td></tr>
</tbody>
</table>

For a quick glance, we can `SELECT` the first unordered N rows of a table, like this:

```python
t_n = t_even[:3]
display(t_n)
```

<table>
<thead>
<tr><th style="text-align: right;">  i</th><th style="text-align: right;">  j</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;"> 10</td><td style="text-align: right;"> 10</td></tr>
<tr><td style="text-align: right;">  2</td><td style="text-align: right;">  2</td></tr>
<tr><td style="text-align: right;">  6</td><td style="text-align: right;">  6</td></tr>
</tbody>
</table>

Finally when we are done, we can save the resulting table to the database, either temporarily or persistently:

```python
t_n.save_as(table_name="t_n", temp=True)
```

### `JOIN`-ing Two Tables

We can also `JOIN` two tables with GreenplumPython. For example, suppose we have two tables like this:

```python
rows = [(1, "'a'",), (2, "'b'",), (3, "'c'",), (4, "'d'")]
t1 = gp.values(rows, db=db, column_names=["id, val"])
display(t1)
```

<table>
<thead>
<tr><th style="text-align: right;">  id</th><th>val  </th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">   1</td><td>a    </td></tr>
<tr><td style="text-align: right;">   2</td><td>b    </td></tr>
<tr><td style="text-align: right;">   3</td><td>c    </td></tr>
<tr><td style="text-align: right;">   4</td><td>d    </td></tr>
</tbody>
</table>

```python
rows = [(1, "'a'",), (2, "'b'",), (3, "'a'",), (4, "'b'")]
t2 = gp.values(rows, db=db, column_names=["id, val"])
display(t2)
```

<table>
<thead>
<tr><th style="text-align: right;">  id</th><th>val  </th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">   1</td><td>a    </td></tr>
<tr><td style="text-align: right;">   2</td><td>b    </td></tr>
<tr><td style="text-align: right;">   3</td><td>a    </td></tr>
<tr><td style="text-align: right;">   4</td><td>b    </td></tr>
</tbody>
</table>

We can `JOIN` the two table like this:

```python
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
display(t_join)

```

<table>
<thead>
<tr><th style="text-align: right;">  t1_id</th><th>t1_val  </th><th style="text-align: right;">  t2_id</th><th>t2_val  </th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">      1</td><td>a       </td><td style="text-align: right;">      3</td><td>a       </td></tr>
<tr><td style="text-align: right;">      1</td><td>a       </td><td style="text-align: right;">      1</td><td>a       </td></tr>
<tr><td style="text-align: right;">      2</td><td>b       </td><td style="text-align: right;">      4</td><td>b       </td></tr>
<tr><td style="text-align: right;">      2</td><td>b       </td><td style="text-align: right;">      2</td><td>b       </td></tr>
</tbody>
</table>

### Creating and Calling Functions

Calling functions is essential for data analytics. GreenplumPython supports creating Greenplum UDFs and UDAs from Python functions and calling them in Python.

Suppose we have a table of numbers:

```python
rows = [(i,) for i in range(10)]
numbers = gp.values(rows, db=db, column_names=["val"])
display(numbers)
```

<table>
<thead>
<tr><th style="text-align: right;">  val</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">    0</td></tr>
<tr><td style="text-align: right;">    1</td></tr>
<tr><td style="text-align: right;">    2</td></tr>
<tr><td style="text-align: right;">    3</td></tr>
<tr><td style="text-align: right;">    4</td></tr>
<tr><td style="text-align: right;">    5</td></tr>
<tr><td style="text-align: right;">    6</td></tr>
<tr><td style="text-align: right;">    7</td></tr>
<tr><td style="text-align: right;">    8</td></tr>
<tr><td style="text-align: right;">    9</td></tr>
</tbody>
</table>

If we want to get the square of each number, we can write a function to do that:

```python
@gp.create_function
def square(a: int) -> int:
    return a ** 2

display(square(numbers["val"], as_name="result", db=db).to_table())
```

<table>
<thead>
<tr><th style="text-align: right;">  result</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">       0</td></tr>
<tr><td style="text-align: right;">       1</td></tr>
<tr><td style="text-align: right;">       4</td></tr>
<tr><td style="text-align: right;">       9</td></tr>
<tr><td style="text-align: right;">      16</td></tr>
<tr><td style="text-align: right;">      25</td></tr>
<tr><td style="text-align: right;">      36</td></tr>
<tr><td style="text-align: right;">      49</td></tr>
<tr><td style="text-align: right;">      64</td></tr>
<tr><td style="text-align: right;">      81</td></tr>
</tbody>
</table>

Note that this function is called in exactly the same way as ordinary Python functions.

If we also want to get the sum of these numbers, what we need is to write an aggregate function like this:

```python
@gp.create_aggregate
def my_sum(result: int, val: int) -> int:
    if result is None:
        return val
    return result + val

display(my_sum(numbers["val"], as_name="result", db=db).to_table())

```

<table>
<thead>
<tr><th style="text-align: right;">  result</th></tr>
</thead>
<tbody>
<tr><td style="text-align: right;">      45</td></tr>
</tbody>
</table>
