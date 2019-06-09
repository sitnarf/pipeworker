# Pipeworker

This library serves as a framework for creating data processing blocks, which can be then interconnected to create a machine learning and data mining pipelines.

## Basic concepts

### Blocks

```python
class MyBlock(Block):
    def execute(data):
        # do some magic
        # ...
        return transformed_data
        
```

Basic building blocks of your pipeline. The only thing you have to implement is the `execute` method. Its argument is the input and the returned value is the output of the block.

## Example

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
```

For full example, see [pipeworker-example](https://github.com/sitnarf/pipeworker-example) repository.  

## Roadmap

1. **Caching**
   Important feature. This will allow skipping blocks, which haven't changed and use the cached version instead.
2. **Parallelism**
   Parallel blocks are well-suited for implicit concurrent execution.
3. **Functions as blocks**
   Currently, you have to create a class to define a new block. This improvement of API will allow using also functions. 
