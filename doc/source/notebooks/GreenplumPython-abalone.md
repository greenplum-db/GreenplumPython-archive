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

<!-- #region pycharm={"name": "#%% md\n"} -->
# Context
**Background:**

Predicting the age of abalone from physical measurements.  The age of abalone is determined by cutting the shell through the cone, staining it, and counting the number of rings through a microscope -- a boring and time-consuming task.  Other measurements, which are easier to obtain, are used to predict the age.

**Problem:**

Build regression models by ‘sex’ which can predict ‘the number of rings’.
<!-- #endregion -->

<!-- #region pycharm={"name": "#%% md\n"} -->
**Fetch data from ML data repository:**

We can fetch data to Greenplum Using following steps.
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
%load_ext sql
%sql postgresql://gpadmin:***@localhost:7000/postgres
```

```sql pycharm={"name": "#%%\n"}
-- External Table
DROP EXTERNAL TABLE IF EXISTS abalone_external;
CREATE EXTERNAL WEB TABLE abalone_external(
    sex text
    , length float8
    , diameter float8
    , height float8
    , whole_weight float8
    , shucked_weight float8
    , viscera_weight float8
    , shell_weight float8
    , rings integer -- target variable to predict
) location('http://archive.ics.uci.edu/ml/machine-learning-databases/abalone/abalone.data')
format 'CSV'
(null as '?');
```

```sql pycharm={"name": "#%%\n"}
-- Create abalone table from an external table
DROP TABLE IF EXISTS abalone;
CREATE TABLE abalone AS (
    SELECT ROW_NUMBER() OVER() AS id, *
    FROM abalone_external
) DISTRIBUTED BY (sex);
```

<!-- #region pycharm={"name": "#%% md\n"} -->
**Train Test set split**

Before proceeding data exploration, let's split our dataset to train and test set without using MADlib.

Firstly, we fetch a random value between 0 and 1 to each row.
Then we create a percentile table that stores percentile values for each sex.
Finally, we join those 2 tables to obtain our training or test tables.

But since Ordered-Set Aggregate Function is not yet supported with Beta version 1, we will skip this step with GreenplumPython and implement it with SQL.
<!-- #endregion -->

```sql pycharm={"name": "#%%\n"}
CREATE TEMP TABLE temp_abalone_label AS
    (SELECT *, random() AS __samp_out_label FROM abalone);

CREATE TEMP TABLE train_percentile_disc AS
    (SELECT sex, percentile_disc(0.8) within GROUP (ORDER BY __samp_out_label) AS __samp_out_label
    FROM temp_abalone_label GROUP BY sex);
CREATE TEMP TABLE test_percentile_disc AS
    (SELECT sex, percentile_disc(0.2) within GROUP (ORDER BY __samp_out_label) AS __samp_out_label
    FROM temp_abalone_label GROUP BY sex);

DROP TABLE IF EXISTS abalone_train;
CREATE TABLE abalone_train AS
    (SELECT temp_abalone_label.*
        FROM temp_abalone_label
        INNER JOIN train_percentile_disc
        ON temp_abalone_label.__samp_out_label <= train_percentile_disc.__samp_out_label
        AND temp_abalone_label.sex = train_percentile_disc.sex
    );
DROP TABLE IF EXISTS abalone_test;
CREATE TABLE abalone_test AS
    (SELECT temp_abalone_label.*
        FROM temp_abalone_label
        INNER JOIN test_percentile_disc
        ON temp_abalone_label.__samp_out_label <= test_percentile_disc.__samp_out_label
        AND temp_abalone_label.sex = test_percentile_disc.sex
    )
```

<!-- #region pycharm={"name": "#%% md\n"} -->
Note that these features could be supported by GreenplumPython in future release.
<!-- #endregion -->

# Import preparation

We connect to Greenplum database named "postgres"

```python pycharm={"name": "#%%\n"}
import greenplumpython as gp
```

```python pycharm={"name": "#%%\n"}
db = gp.database(host="localhost", dbname="postgres")
```

# Data Exploration

Get access to existed table "abalone"

```python pycharm={"name": "#%%\n"}
abalone = gp.table("abalone", db)
```

Take a look on table

```python pycharm={"name": "#%%\n"}
# SELECT * FROM abalone ORDER BY id LIMIT 5;

abalone.order_by(abalone["id"]).head(5)
```

Observe the distribution of data on different segments

```python pycharm={"name": "#%%\n"}
# SELECT gp_segment_id, COUNT(*) 
# FROM abalone
# GROUP BY 1
# ORDER BY gp_segment_id;

count = gp.aggregate("count") # -- Get access to existing aggregate in Greenplum
count(abalone["id"], group_by=abalone.group_by("gp_segment_id"), db=db).to_table()
```

Since we already have table "abalone_train" ad "abalone_test" in the database, we can get access to them.

```python pycharm={"name": "#%%\n"}
abalone_train = gp.table("abalone_train", db)
abalone_test = gp.table("abalone_test", db)
```

# Execute the OLS Linear Regression Function by 'sex'

**Creation of training function**

```python pycharm={"name": "#%%\n"}
from typing import List

# CREATE TYPE plc_linreg_type AS (
#    col_nm text[]
#    , coef float8[]
#    , intercept float8
#    , serialized_linreg_model bytea
#    , created_dt text
# );


class PlcLinregType:
    col_nm: List[str]
    coef: List[float]
    intercept: float
    serialized_linreg_model: bytes
    created_dt: str


# -- Create function
# -- Need to specify the return type -> API will create the corresponding type in Greenplum to return a row
# -- Will add argument to change language extensions, currently plpython3u by default


@gp.create_array_function
def plc_linreg_func(
    length: List[float], shucked_weight: List[float], rings: List[int]
) -> PlcLinregType:
    import numpy as np
    from sklearn.linear_model import LinearRegression

    X = np.array([length, shucked_weight]).T
    y = np.array([rings]).T

    # OLS linear regression with length, shucked_weight
    linreg_fit = LinearRegression().fit(X, y)
    linreg_coef = linreg_fit.coef_
    linreg_intercept = linreg_fit.intercept_

    # Serialization of the fitted model
    import datetime

    import six

    pickle = six.moves.cPickle
    serialized_linreg_model = pickle.dumps(linreg_fit, protocol=2)

    return {
        "col_nm": ["length", "shucked_weight"],
        "coef": linreg_coef[0],
        "intercept": linreg_intercept[0],
        "serialized_linreg_model": serialized_linreg_model,
        "created_dt": str(datetime.datetime.now()),
    }
```

<!-- #region pycharm={"name": "#%% md\n"} -->
**Apply "plc_linreg_fitted" function to our train set**
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
# DROP TABLE IF EXISTS plc_linreg_fitted;
# CREATE TABLE plc_linreg_fitted AS (
#    SELECT
#        a.sex
#        , (plc_linreg_func(
#            a.length_agg
#            , a.shucked_weight_agg
#            , a.rings_agg)
#        ).*
#    FROM (
#        SELECT
#            sex
#            , ARRAY_AGG(length) AS length_agg
#            , ARRAY_AGG(shucked_weight) AS shucked_weight_agg
#            , ARRAY_AGG(rings) AS rings_agg
#        FROM abalone_split
#        WHERE split = 1
#        GROUP BY sex
#    ) a
#) DISTRIBUTED BY (sex);

plc_linreg_fitted = plc_linreg_func(
                            abalone_train["length"],
                            abalone_train["shucked_weight"],
                            abalone_train["rings"],
                            group_by=abalone_train.group_by("sex")
).to_table()
```

<!-- #region pycharm={"name": "#%% md\n"} -->
**Take a look at models built**
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
plc_linreg_fitted[["sex", "col_nm", "coef", "intercept", "created_dt"]]
```

**Get summary of parameter's coefficient for three sex**

```python pycharm={"name": "#%%\n"}
# SELECT sex, UNNEST(col_nm) AS col_nm, UNNEST(coef) AS coef
# FROM plc_linreg_fitted
# UNION
# SELECT sex, 'intercept' AS col_nm, intercept AS coef
# FROM plc_linreg_fitted;
unnest = gp.function("unnest")

plc_linreg_fitted_1 = plc_linreg_fitted[[
                        plc_linreg_fitted["sex"],
                        unnest(plc_linreg_fitted["col_nm"]).rename("col_nm"),
                        unnest(plc_linreg_fitted["coef"]).rename("coef")]
                ]
plc_linreg_fitted_2 = plc_linreg_fitted[[
                        plc_linreg_fitted["sex"],
                        "'intercept' AS col_name",
                        plc_linreg_fitted["intercept"].rename("coef")]
                ]

plc_linreg_fitted_1.union(
        plc_linreg_fitted_2
)
```

<!-- #region pycharm={"name": "#%% md\n"} -->
# Prediction

**Currently can't support, because function can take care only one table at the time**
<!-- #endregion -->

```python pycharm={"name": "#%%\n"}
@gp.create_array_function
def plc_linreg_pred_func(serialized_model: bytes, features: List[float]) -> List[float]:
        # Deserialize the serialized model
        import six
        pickle = six.moves.cPickle
        model = pickle.loads(serialized_model)

        # Predict the target variable
        y_pred = model.predict([features])

        return y_pred[0]
```

```python pycharm={"name": "#%%\n", "is_executing": true}
plc_linreg_pred = plc_linreg_pred_func(
                                plc_linreg_fitted["serialized_linreg_model"],
                                abalone_test["length"],
                                abalone_test["shucked_weight"],
                                group_by=["sex"]
).to_table()

## Error expected: Cannot pass arguments from more than one tables
```
