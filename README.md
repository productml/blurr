![Blurr](logo.png)

[![CircleCI](https://circleci.com/gh/productml/blurr/tree/master.svg?style=svg)](https://circleci.com/gh/productml/blurr/tree/master)
[![Documentation Status](https://readthedocs.org/projects/productml-blurr/badge/?version=latest)](http://productml-blurr.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://coveralls.io/repos/github/productml/blurr/badge.svg?branch=master)](https://coveralls.io/github/productml/blurr?branch=master)
[![PyPI version](https://badge.fury.io/py/blurr.svg)](https://badge.fury.io/py/blurr)
[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/productml/blurr/master?filepath=examples%2Ftutorial)

# What is Blurr?

Blurr transforms structured, streaming `raw data` into `features` for model training and prediction using a `high-level expressive YAML-based language` called the Data Transform Configuration (DTC).

## Blurr vs. stream/batch processors

The DTC is a __data transform definition__ for structured data. The DTC encapsulates the *business logic* of data transforms and Blurr orchestrates the *execution* of data transforms.

Blurr is processor-agnostic, so DTCs can be run by event processors such as Spark.

1. Blurr is to Spark as [Hibernate](http://hibernate.org/) is to databases
2. Blurr can be used on Spark in the same way as SparkSQL
3. Blurr is WORAIS (Write Once, Run on Any Infrastructure Stack)

Because real world infrastructure is extremely diverse, Blurr is designed to run on virtually any infrastructure stack that runs Python 3.6+.

[Give us feedback on the metaphors](https://docs.google.com/forms/d/e/1FAIpQLSf5wqW7M4IibJU-NYDEZ-rx0TvJYMkTiV_hehZgKV6a6HvXaA/viewform) and help improve Blurr!

## The future of MLOps

>We believe in a world where everyone is a data engineer. Or a data scientist. Or an ML engineer. The lines are blurred (*cough*). Just like development and operations became DevOps over time

>--- Blurr authors

Blurr is a collection of components built for MLOps, the DTC is one of them. **DTC ⊆ Blurr**

We see a future where MLOps means teams putting together various technologies to suit their needs. For production ML applications, the __speed of experimentation__ and __iterations__ is the difference between success and failure. The DTC helps teams iterate on features faster. The vision for Blurr is to build MLOps components to help ML teams experiment at high speed.

# Table of contents

- [DTC at a glance](#dtc-at-a-glance)
- [Tutorial & Docs](#tutorial-and-docs)
- [Install](#use-blurr)
- [Contribute](#contribute-to-blurr)
- [Data Science 'Joel Test'](#data-science-joel-test)
- [Roadmap](#roadmap)

>Coming up with features is difficult, time-consuming, requires expert knowledge. 'Applied machine learning' is basically feature engineering

>--- Andrew Ng

# DTC at a glance

Raw data like this

```javascript
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_start" }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_end", "won": 1 }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_start" }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_end", "won": 1 }
{ "user_id": "B6FA", "session_id": "D043", "country" : "US", "event_id": "game_start" }
{ "user_id": "B6FA", "session_id": "D043", "country" : "US", "event_id": "game_end", "won": 1 }
{ "user_id": "09C1", "session_id": "T8KA", "country" : "UK", "event_id": "game_start" }
{ "user_id": "09C1", "session_id": "T8KA", "country" : "UK", "event_id": "game_end", "won": 1 }
```

turns into

session_id |  user_id | games_played | games_won
--- | ------------ | -------------- | --------
915D | 09C1 | 2 | 2
D043 | B6FA | 1 | 1
T8KA | 09C1 | 1 | 1

using this DTC

```yaml

Type: Blurr:Transform:Streaming
Version: '2018-03-01'
Name : sessions

Stores:
   - Type: Blurr:Store:MemoryStore
     Name: hello_world_store

Identity: source.user_id

Time: parser.parse(source.timestamp)

Aggregates:

 - Type: Blurr:Aggregate:BlockAggregate
   Name: session_stats
   Store: hello_world_store

   Split: source.session_id != session_stats.session_id

   Fields:

     - Name: session_id
       Type: string
       Value: source.session_id

     - Name: games_played
       Type: integer
       When: source.event_id == 'game_start'
       Value: session_stats.games_played + 1

     - Name: games_won
       Type: integer
       When: source.event_id == 'game_end' and source.won == '1'
       Value: session_stats.games_won + 1

```

# Tutorial and Docs

[Read the docs](http://productml-blurr.readthedocs.io/en/latest/)

[Streaming DTC Tutorial](http://productml-blurr.readthedocs.io/en/latest/Streaming%20DTC%20Tutorial/) |
[Window DTC Tutorial](http://productml-blurr.readthedocs.io/en/latest/Window%20DTC%20Tutorial/)

Preparing data for specific use cases using Blurr

[Dynamic in-game offers (Offer AI)](examples/offer-ai/offer-ai-walkthrough.md) | [Frequently Bought Together](examples/frequently-bought-together/fbt-walkthrough.md)

# Use Blurr

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

Welcome to the Blurr community! We are so glad that you share our passion for making data management and machine learning accessible to everyone.

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

Blurr is currently in developer preview. __Stay in touch!__: Star this project or email hello@blurr.ai

- ~~Local transformations only~~
- ~~Support for custom functions and other python libraries in the DTC~~
- Spark runner
- S3-S3 data transformations
- Add DynamoDB as a Store
- Features server
