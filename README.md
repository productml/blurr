![Blurr](docs/images/logo.png)

[![CircleCI](https://circleci.com/gh/productml/blurr/tree/master.svg?style=svg)](https://circleci.com/gh/productml/blurr/tree/master)
[![Documentation Status](https://readthedocs.org/projects/productml-blurr/badge/?version=latest)](http://productml-blurr.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://coveralls.io/repos/github/productml/blurr/badge.svg?branch=master)](https://coveralls.io/github/productml/blurr?branch=master)
[![PyPI version](https://badge.fury.io/py/blurr.svg)](https://badge.fury.io/py/blurr)

# Table of contents

- [What is Blurr](#what-is-blurr)
- [Is Blurr for you?](#is-blurr-for-you)
- [Playground](#playground)
- [Tutorial & Docs](#tutorial-and-docs)
- [Contribute](#contribute-to-blurr)
- [Data Science 'Joel Test'](#data-science-joel-test)
- [Roadmap](#roadmap)

# What is Blurr?

Blurr transforms structured, streaming `raw data` into `features` for model training and prediction using a `high-level expressive YAML-based language` called the Blurr Transform Spec (BTS). The BTS merges the schema and computation model for data processing.

The BTS is a __data transform definition__ for structured data. The BTS encapsulates the *business logic* of data transforms and Blurr orchestrates the *execution* of data transforms. Blurr is runner-agnostic, so BTSs can be run by event processors such as Spark, Spark Streaming or Flink.

# Is Blurr for you?

Yes, if: you are well on your way on the ML 'curve of enlightenment', and are thinking about how to do online scoring

![Curve](docs/images/curve.png)

# Playground

[Launch playground](https://colab.research.google.com/drive/1XU8G7as4cuPYqcoV5rJAd8yMuXUPXU8Q)

# Tutorial and Docs

>Coming up with features is difficult, time-consuming, requires expert knowledge. 'Applied machine learning' is basically feature engineering --- Andrew Ng

[Read the docs](http://productml-blurr.readthedocs.io/en/latest/)

[Streaming BTS Tutorial](http://productml-blurr.readthedocs.io/en/latest/Streaming%20BTS%20Tutorial/) |
[Window BTS Tutorial](http://productml-blurr.readthedocs.io/en/latest/Window%20BTS%20Tutorial/)

Preparing data for specific use cases using Blurr:

* [Dynamic in-game offers (Offer AI)](docs/examples/offer-ai/offer-ai-walkthrough.md)
* [Frequently Bought Together](docs/examples/frequently-bought-together/fbt-walkthrough.md)

# Contribute to Blurr

Welcome to the Blurr community! We are so glad that you share our passion for building MLOps!

Please create a [new issue](https://github.com/productml/blurr/issues/new) to begin a discussion. Alternatively, feel free to pick up an existing issue!

Please sign the [Contributor License Agreement](https://docs.google.com/forms/d/e/1FAIpQLSeUP5RFuXH0Kbi4CnV6V3IZ-xyJmd3KQP_2Ij-pTvN-_h7wUg/viewform) before raising a pull request.

# Data Science 'Joel Test'

Inspired by the (old school) [Joel Test](https://www.joelonsoftware.com/2000/08/09/the-joel-test-12-steps-to-better-code/) to rate software teams, here's our version for data science teams. What's your score?

1. Data pipelines are versioned and reproducible
2. Pipelines (re)build in one step
3. Deploying to production needs minimal engineering help
4. Successful ML is a long game. You play it like it is
5. Kaizen. Experimentation and iterations are a way of life

# Roadmap

Blurr is currently in Developer Preview. __Stay in touch!__: Star this project or email hello@blurr.ai

- ~~Local transformations only~~
- ~~Support for custom functions and other python libraries in the BTS~~
- ~~Spark runner~~
- S3 support for data sink
- DynamoDB as an Intermediate Store
- Features server