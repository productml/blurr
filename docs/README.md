```
We believe in a world where everyone is a data engineer. Or a data scientist. Or an ML engineer.
The distinction is increasingly blurry (*cough*).
Like development and operations merged into DevOps over time.
```

Work directly with raw data to construct a production ML pipeline, in minutes - without writing code.

Blurr provides a `high-level expressive YAML-based language` called the Data Transform Configuration (DTC). The DTC defines custom data transformations and aggregations from a `data source` (S3), to `transform the data` (in lambdas) and output to a `data store` (S3). The data in the store can be used for any application dependent on real-time transformations like analytics or predictions.

The Streaming DTC aggregates the raw data into blocks and the Window DTC drops an anchor on a block and generates model features relative to that block.

![Blocks](docs/images/blocks-intro.png)

![Window](docs/images/window.png)

Blurr has first class support for serverless data processing which means near zero standing costs, great for spiky usage, infinite scale and zero maintenance headache.
