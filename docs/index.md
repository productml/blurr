Blurr provides a `high-level expressive YAML-based language` called the Blurr Transform Spec (BTS). The BTS defines custom data transformations and aggregations from a `data source`, to `transform the data` and output to a `data store`. The data in the store can be used for any application dependent on real-time transformations like analytics or predictions.

The Streaming BTS aggregates the raw data into blocks.

![Blocks](images/blocks-intro.png)

Window BTS drops an anchor on a block and generates model features relative to that block.

![Window](images/window.png)
