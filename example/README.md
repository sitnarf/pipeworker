# pipeworker-example
This repository serves as an example of use of [pipeworker](https://github.com/sitnarf/pipeworker), simplistic framework for structuring data mining and machine learning pipelines. 

## Installation and running

[Pipenv](https://pipenv.readthedocs.io/en/latest/) is useful tool which prepares python virtual environemt  and install  dependencies in one step. 

```
git clone git@github.com:sitnarf/pipeworker-example.git
cd pipework-example
pipenv install
pipenv shell
```

Then simply run

```
python example.py
```

## Requirements

Pipeworker requires Python >= 3.7

## Description

It  contains simple models with example dataset. In `example.py` is defined the pipeline and in `nodes.py` corresponding nodes used in the pipeline.

```python
pipe = Pipeline(
    LoadData() |
    FillNaN() |
    TrainTestSplit(shuffle=False) |
    (
            SES().set_name("SES") &
            (SARIMA((2, 1, 1), (0, 1, 0, 12)).set_name("SARIMA 1")) &
            (SARIMA((2, 1, 1), (0, 0, 0, 12)).set_name("SARIMA 2"))
    ) |
    Map(Measure(measurements=[mae, mape], column="passengers")) |
    Map(PrintMeasurements()) |
    CompareMeasurementAndPrint(which="mape")
)

result = pipe.execute()
```

LoadData node loads `air_passengers.csv` dataset. The pipeline trains three models, perform forecast and then evaluates their performence with two error measures (mae and mape). Last node compares mape among the three models. 

Output:

```
Measurements for SES
====================
╒══════╤═══════╕
│ mae  │ 94.94 │
├──────┼───────┤
│ mape │  0.20 │
╘══════╧═══════╛

Measurements for SARIMA 1
=========================
╒══════╤═══════╕
│ mae  │ 17.81 │
├──────┼───────┤
│ mape │  0.04 │
╘══════╧═══════╛

Measurements for SARIMA 2
=========================
╒══════╤═══════╕
│ mae  │ 74.11 │
├──────┼───────┤
│ mape │  0.16 │
╘══════╧═══════╛

Absolute difference in mape
============================
╒══════════╤══════════╤══════════╤═══════╤═══════╕
│          │ SARIMA 1 │ SARIMA 2 │ SES   │ AVG   │
├──────────┼──────────┼──────────┼───────┼───────┤
│ SARIMA 1 │          │ -0.11    │ -0.16 │ -0.14 │
├──────────┼──────────┼──────────┼───────┼───────┤
│ SARIMA 2 │ 0.11     │          │ -0.04 │ 0.04  │
├──────────┼──────────┼──────────┼───────┼───────┤
│ SES      │ 0.16     │ 0.04     │       │ 0.10  │
╘══════════╧══════════╧══════════╧═══════╧═══════╛
```

 