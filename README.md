# Blurr

[![CircleCI](https://circleci.com/gh/productml/blurr/tree/master.svg?style=svg)](https://circleci.com/gh/productml/blurr/tree/master)

```
We believe in a world where everyone is a data engineer. Or a data scientist. Or an ML engineer.
The distinction is increasingly blurry (*cough*).
Like development and operations merged into DevOps over time.
```

Work directly with raw data to construct a production ML pipeline, in minutes - without writing code.

Blurr enables data scientists and engineers to build and deploy real-time predictive models by:

1. Providing a data pipeline that transforms `raw data` into `features` for model training and prediction
2. Defining a high level authoring language that executes data transforms without writing code

For production ML applications, `experimentation and iteration speed` is important. Working directly with raw data provides the most flexibility. Blurr allows product teams to iterate quickly during ML dev and provides a self-service way to take experiments to production.

![Data Transformer](examples/offer-ai/images/data-transformer.png)

Blurr provides a `high-level expressive YAML-based language` called the Data Transform Configuration (DTC). The DTC defines custom data transformations and aggregations from a `data source` (S3), to `transform the data` (in lambdas) and output to a `data store` (DynamoDB). The data in the store can be used for any application dependent on real-time transformations like analytics or predictions.

![2steps](examples/offer-ai/images/2steps.png)

Blurr has first class support for serverless data processing which means near zero standing costs, great for spiky usage, infinite scale and zero maintenance headache.

# Examples

We're putting together examples for how to use Blurr to build models for specific use cases.

[Dynamic in-game offers (Offer AI)](examples/offer-ai/offer-ai-walkthrough.md)

More to come!

# Status and License

Blurr is currently in closed development. We're targeting a first release in early April, 2018. Email blurr@productml.com to get notified when it is out! Even better, star or watch this GitHub project!

We intend to license Blurr as an open source project. We will update the license + contribution guidelines with the first release.

# Data Science 'Joel Test'

Inspired by the (old school) [Joel Test](https://www.joelonsoftware.com/2000/08/09/the-joel-test-12-steps-to-better-code/) to rate software teams, here's our version for data science teams. What's your score? We'd love to know!

1. Data pipelines are versioned and reproducible
2. Pipelines (re)build in one step
3. Deploying to production needs minimal engineering help
4. Successful ML is a long game. You play it like it is
5. Kaizen. Experimentation and iterations are a way of life
