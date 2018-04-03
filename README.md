![Blurr](logo.png)

[![CircleCI](https://circleci.com/gh/productml/blurr/tree/master.svg?style=svg)](https://circleci.com/gh/productml/blurr/tree/master)
[![Documentation Status](https://readthedocs.org/projects/productml-blurr/badge/?version=latest)](http://productml-blurr.readthedocs.io/en/latest/?badge=latest)

>We believe in a world where everyone is a data engineer. Or a data scientist. Or an ML engineer. The lines are blurred (*cough*). Just like development and operations became DevOps over time

>-- Blurr authors


# What is Blurr?

>Raw data to features for machine learning with serverless data transforms

Blurr transforms `raw data` into `features` for model training and prediction using a `high-level expressive YAML-based language` called the Data Transform Configuration (DTC).

For production ML applications, __experimentation__ and __iteration speed__ is important. Working directly with raw data provides the most flexibility. Blurr allows product teams to iterate quickly during ML dev and provides a self-service way to take experiments to production.

![Data Transformer](docs/images/data-transformer.png)

# Table of contents

- [DTC at a glance](#dtc-at-a-glance)
- [Tutorial & Docs](#tutorial-and-docs)
- [Install](#use-blurr)
- [Contribute](#contribute-to-blurr)
- [Data Science 'Joel Test'](#data-science-joel-test)
- [Get in touch](#get-in-touch)
- [Roadmap](#roadmap)

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

Type: Blurr:Streaming
Version: '2018-03-07'

Store:
   - Type: Blurr:Store:MemoryStore
     Name: hello_world_store

Identity: source.user_id

DataGroups:

 - Type: Blurr:DataGroup:BlockAggregate
   Name: session_stats
   Store: hello_world_store
   Split: source.session_id != session_stats.session_id

   Fields:

     - Name: session_id
       Type: string
       Value: source.session_id

     - Name: games_played
       Type: integer
       Value: session_stats.games_played + 1
       When: source.event_id == 'game_start'

     - Name: games_won
       Type: integer
       Value: session_stats.games_won + 1
       When: source.event_id == 'game_end' and source.won == '1'

```

# Tutorial and Docs

[Read the docs](http://productml-blurr.readthedocs.io/en/latest/)

[Streaming DTC Tutorial](http://productml-blurr.readthedocs.io/en/latest/Streaming%20dtc%20tutorial/) |
[Window DTC Tutorial](http://productml-blurr.readthedocs.io/en/latest/Window%20dtc%20tutorial/)

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

# Get in touch

Email us at blurr@productml.com or star this project to stay in touch!

# Roadmap

Blurr is all about enabling machine learning and AI teams to run faster.

**Developer Preview 0**: Local transformations only

**Developer Preview 1**: S3-S3 data transformations

**Developer Preview 2**: Add DynamoDB as a Store + Features server for ML production use

Ingestion connectors to Kafka and Spark
