![Blurr](docs/images/logo.png)

[![CircleCI](https://circleci.com/gh/productml/blurr/tree/master.svg?style=svg)](https://circleci.com/gh/productml/blurr/tree/master)
[![Documentation Status](https://readthedocs.org/projects/productml-blurr/badge/?version=latest)](http://productml-blurr.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://coveralls.io/repos/github/productml/blurr/badge.svg?branch=master)](https://coveralls.io/github/productml/blurr?branch=master)
[![PyPI version](https://badge.fury.io/py/blurr.svg)](https://badge.fury.io/py/blurr)
[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/productml/blurr/master?filepath=examples%2Ftutorial)

# What is Blurr?

Blurr transforms structured, streaming `raw data` into `features` for model training and prediction using a `high-level expressive YAML-based language` called the Data Transform Configuration (DTC).

The DTC is a __data transform definition__ for structured data. The DTC encapsulates the *business logic* of data transforms and Blurr orchestrates the *execution* of data transforms. Blurr is runner-agnostic, so DTCs can be run by event processors such as Spark, Spark Streaming or Flink.

![Blurr Training](docs/images/blurr-in-training.png)

This looks like any other ETL pipeline. At this point, Blurr doesn't do anything special that you cannot do with Spark, for instance. Blurr shines when an offline model pipeline needs to be turned into an online scoring pipeline.

![Blurr Production](docs/images/blurr-in-prod.png)

## Blurr is MLOps

Blurr is a collection of components built for MLOps, the Blurr Core library is one of them. **Blurr Core ⊆ Blurr**

>We believe in a world where everyone is a data engineer. Or a data scientist. Or an ML engineer. The lines are blurred (*cough*). Just like development and operations became DevOps over time

We see a future where MLOps means teams putting together various technologies to suit their needs. For production ML applications, the __speed of experimentation__ and __iterations__ is the difference between success and failure. The __DTC helps teams iterate on features faster__. The vision for Blurr is to build MLOps components to help ML teams experiment at high speed.

# Table of contents

- [Tutorial & Docs](#tutorial-and-docs)
- [Try Blurr](#try-blurr)
- [Contribute](#contribute-to-blurr)
- [Data Science 'Joel Test'](#data-science-joel-test)
- [Roadmap](#roadmap)

>Coming up with features is difficult, time-consuming, requires expert knowledge. 'Applied machine learning' is basically feature engineering --- Andrew Ng

# Tutorial and Docs

[Read the docs](http://productml-blurr.readthedocs.io/en/latest/)

[Streaming DTC Tutorial](http://productml-blurr.readthedocs.io/en/latest/Streaming%20DTC%20Tutorial/) |
[Window DTC Tutorial](http://productml-blurr.readthedocs.io/en/latest/Window%20DTC%20Tutorial/)

Preparing data for specific use cases using Blurr

[Dynamic in-game offers (Offer AI)](docs/examples/offer-ai/offer-ai-walkthrough.md) | [Frequently Bought Together](docs/examples/frequently-bought-together/fbt-walkthrough.md)

# Try Blurr

We interact with Blurr using a Command Line Interface (CLI). Blurr is installed via pip:

`$ pip install blurr`

Transform data

```
$ blurr transform \
     --streaming-dtc ./dtcs/sessionize-dtc.yml \
     --window-dtc ./dtcs/windowing-dtc.yml \
     --source file://path
```

[CLI documentation](http://productml-blurr.readthedocs.io/en/latest/Blurr%20CLI/)

# Contribute to Blurr

Welcome to the Blurr community! We are so glad that you share our passion for building MLOps!

Please create a [new issue](https://github.com/productml/blurr/issues/new) to begin a discussion. Alternatively, feel free to pick up an existing issue!

Please sign the [Contributor License Agreement](https://docs.google.com/forms/d/e/1FAIpQLSeUP5RFuXH0Kbi4CnV6V3IZ-xyJmd3KQP_2Ij-pTvN-_h7wUg/viewform) before raising a pull request.

# Data Science 'Joel Test'

Inspired by the (old school) [Joel Test](https://www.joelonsoftware.com/2000/08/09/the-joel-test-12-steps-to-better-code/) to rate software teams, here's our version for data science teams. What's your score? We'd love to know!

1. Data pipelines are versioned and reproducible
2. Pipelines (re)build in one step
3. Deploying to production needs minimal engineering help
4. Successful ML is a long game. You play it like it is
5. Kaizen. Experimentation and iterations are a way of life

# Roadmap

Blurr is currently in Developer Preview. __Stay in touch!__: Star this project or email hello@blurr.ai

- ~~Local transformations only~~
- ~~Support for custom functions and other python libraries in the DTC~~
- ~~Spark runner~~
- S3 support for data sink
- DynamoDB as an Intermediate Store
- Features server
