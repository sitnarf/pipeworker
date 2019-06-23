# Pipeworker

This library servers as framework for creating data processing blocks, which can be then interconnected to create machine learning and data mining pipelines.

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

## Basic concepts

Preprocessing data, training model and evaluation can be expressed as sequence of blocks. Every block has input and output. Blocks can be connected serially (| operator) or in parallel (& operator). 

### Blocks

```python
class MyBlock(Block):
	def execute(data):
		# do some magic
		return transformed_data
		
```

Basic building blocks of your pipeline. Only thing you have to implement is `execute` method. The argument is an input and the returned value is an output of the block.

### Connecting

When serial operator (|) is used, output of previous block is simple passed to subsequent structure. 

## Roadmap

1. **Caching**
   Important feature. This will allow skip blocks, which haven't changed, and use cached version instead.
2. **Parallelism**
   Parallel blocks are well-suited for implicit concurrent execution.
3. **Functions as blocks**
   Currently, you have to create class to define a new block. This improvement of API will allow to use also functions. 