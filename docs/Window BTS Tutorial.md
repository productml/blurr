# Tutorial 2: Window Aggregation

In this tutorial we'll introduce a new type of BTS: the __Window BTS__, and will learn how a Window BTS consumes session data produced in the first tutorial in order to generate __time aggregated data__.

Try the code from this example [launching a Jupyter Notebook](https://mybinder.org/v2/gh/productml/blurr/master?filepath=examples%2Ftutorial).

## 1. Game Boosts

For this second tutorial we'll introduce the concept of __boosts__ to the game we defined previously.

In order to make the game more exciting a user can now activate a __boost__ when starting a play session. While the boost is active, games become easier. Chances of winning a game are increased.

When the player activates a boost, the `game_start` event will include a `"boost": true` property:

```json
{ "user_id": "09C1", "session_id": "T8KA", "country" : "UK", "event_id": "game_start", "boost" : true, "timestamp" : "2018/03/04 08:32:12" }
```

There are a couple of restrictions with boosts though:

* A player can activate a boost only once.
* The boost will be active for just 3 days.

We expect our players will be encouraged to play more often during the next 3 days after activating the boost.

The following change is made to the streaming BTS from the previous tutorial to take into account the boost information.

```yaml
- Name: boost
       Type: boolean
       Value: source.boost
       When: source.boost == True
```

This will set the value of boost in the aggregate to True if __any__ of the events in the aggregate have the boost set to true.

The goal of this tutorial is to __collect aggregated session data__ that __validates our assumption__.


## 2. Aggregation Result

In order to confirm our hypothesis we're interested in comparing two figures:

1. The average number of games played by session __before activating the boost__
1. The average number of games played by session __while the boost is active__

We will obtain this data by aggregating the __original session data__ obtained in the first tutorial into an series of records containing the desired information:

| user_id  | last_7_days.avg_games_per_session | next_3_days.avg_games_per_session |
| -------- | --------------------------------- | --------------------------------- |
| 09C1     | 4.82                              | 5.61                              |
| B6FA     | 2.73                              | 3.09                              |
| NV9T     | 8.11                              | 12.52                             |
| 6CF3     | 9.89                              | 14.74                             |

This result shows our players have increased the games played per session after activating the boost.

## 3. Window BTS

In order to obtain the output described before, Blurr will perform __time-based aggregation__ over the historic session data obtained with the Streaming BTS in the first tutorial. This transformation is defined in a __Window BTS__:

```yaml
Type: Blurr:Transform:Window
Version: '2018-03-01'
Name: boost_data

SourceBTS: sessions

Anchor:
  Condition: sessions.session_stats.boost == True

Aggregates:
  - Type: Blurr:Aggregate:Window
    Name: last_7_days
    WindowType: day
    WindowValue: -7
    Source: sessions.session_stats

    Fields:
     - Name: avg_games_per_session
       Type: float
       Value: sum(source.games_played) / len(source.session_id)

  - Type: Blurr:Aggregate:Window
    Name: next_3_days
    WindowType: day
    WindowValue: +3
    Source: sessions.session_stats

    Fields:
     - Name: avg_games_per_session
       Type: float
       Value: sum(source.games_played) / len(source.session_id)
```

As we can see, the structure of a Window BTS is pretty similar to the Streaming BTS. There are 2 new elements though: `SourceBTS` and `Anchor`

### 3.1. SourceBTS

As we mentioned before, a Window BTS will use session data produced by a Streaming BTS as __data input__. This is indicated in `SourceBTS`:

```yaml
SourceBTS: sessions
```

`sessions` is the `Name` given to the Streaming BTS in its header:

```yaml
# excerpt from Streaming BTS
Type: Blurr:Transform:Streaming
Version: '2018-03-07'
Name : sessions
```

### 3.2. Anchor Points

In time-based aggregations, data is aggregated around __Anchor Points__. This a key concept in time-based transformations. In our example, an Anchor Point is __the session in which the boost is activated__ for a user:

```yaml
Anchor:
  Condition: sessions.session_stats.boost == True
```

The `Name` given to source BTS above is used to access the BTS's properties. i.e in this case `sessions.sessions_stats.boost`

### 3.3. Identity

It's time to bring back the concept of Identity introduced in the previous tutorial:

```yaml
# excerpt from Streaming BTS
Identity: source.user_id
```

So far, we've thought of the Identity as a mandatory field that is part of both the original events and session data.

In a Window BTS the Identity also has a role: __grouping data__ that is aggregated around Anchor Points. The Identity ensures that our output has __one record per user__.


### 3.4. Window Aggregates

Our Window BTS performs 2 different aggregations:

* Over all sessions 7 days __before__ the Anchor Point.
* Over all sessions 3 days __after__ the Anchor Point.

How each aggregate is calculated is defined in the `Window Aggregates`:


```yaml
- Type: Blurr:Aggregate:Window
  Name: last_7_days
  WindowType: day
  WindowValue: -7
  Source: sessions.session_stats

  Fields:
    - Name: avg_games_per_session
      Type: float
      Value: sum(source.games_played) / len(source.session_id)
```

This `Window Aggregate` is responsible for aggregating data over the __previous 7 days__ before the boost activation.

`WindowType` and `WindowValue` are used to indicate the how many days/hours of data from/since the Anchor Point are being collected:

`Source` is used to __lookup input data__ from the Streaming BTS.

In this case the input is session data produced in `session_stats` Aggregate in `sessions` Streaming BTS:

```yaml
# excerpt from Streaming BTS
Aggregates:
 - Type: Blurr:Aggregate:Block
   Name: session_stats
```


#### Fields

Data is aggregated using the `Value` expression of a `Field`:

```yaml
Fields:
  - Name: avg_games_per_session
    Type: float
    Value: sum(source.games_played) / len(source.session_id)

```

We're interested in the name of games played by session, which is the result of dividing the number of games played in all sessions by the number of sessions:

```
avg_games_by_session = total_games_played_count / session_count
```

This is calculated with the following Python expression:

```yaml
Value: sum(source.games_played) / len(source.session_id)
```

It's important to note that in context, `source` __is not__ a list of sessions, but __an object containing list of session_fields__ instead.

For example, the value of `games_played` for the first session collected is accessed as:

```python
source.games_played[0]
```

Instead of

```python
source[0].games_played.
```

The shape of `source` object therefore looks like this:

```json
{
  "source": {
    "session_id": ["915D", "T8KA"],
    "games_played": ["2", "1"],
    "games_won": ["2", "1"]
  }
}
```

Within expressions you can use any Python function applicable to lists, such as `len(source.session_id)` `sum(source.games_played)` or even more complex operations like

```python
sum([i for i in source.games_played if i >= 2])
```

## 4. Previewing the transformation using Blurr CLI

We can preview the result of the transformation using `blurr transform` command.

To preview a window transformation we need to pass both the Streaming and Window BTS as arguments:

```bash
$ blurr transform --streaming-bts tutorial2-streaming-bts.yml --window-bts tutorial2-window-bts.yml tutorial2-data.log

["7d49b5ef-0555-535c-8f53-1daff259e8fe", []]
["e0093eec-44a2-b781-fca1-794edccae965", [{"last_7_days._identity": "e0093eec-44a2-b781-fca1-794edccae965", "last_7_days.avg_games_per_session": 32.0, "next_3_days._identity": "e0093eec-44a2-b781-fca1-794edccae965", "next_3_days.avg_games_per_session": 3.0}]]
["df54d39f-a4a7-03f8-48e2-00bf755fe31c", [{"last_7_days._identity": "df54d39f-a4a7-03f8-48e2-00bf755fe31c", "last_7_days.avg_games_per_session": 21.0, "next_3_days._identity": "df54d39f-a4a7-03f8-48e2-00bf755fe31c", "next_3_days.avg_games_per_session": 28.0}]]
["dfaa4419-5859-f2c8-4087-01b2b5738ae4", [{"last_7_days._identity": "dfaa4419-5859-f2c8-4087-01b2b5738ae4", "last_7_days.avg_games_per_session": 13.0, "next_3_days._identity": "dfaa4419-5859-f2c8-4087-01b2b5738ae4", "next_3_days.avg_games_per_session": 8.0}]]
["8a2a81bc-c7ed-c09c-5a43-4a8694412db8", []]
["b5cf93ba-7642-a7c7-f553-8e8a254fe92c", [{"last_7_days._identity": "b5cf93ba-7642-a7c7-f553-8e8a254fe92c", "last_7_days.avg_games_per_session": 19.0, "next_3_days._identity": "b5cf93ba-7642-a7c7-f553-8e8a254fe92c", "next_3_days.avg_games_per_session": 9.5}]]
["14c2c283-32a6-43e1-a000-b5d7be6d9cba", [{"last_7_days._identity": "14c2c283-32a6-43e1-a000-b5d7be6d9cba", "last_7_days.avg_games_per_session": 1.0, "next_3_days._identity": "14c2c283-32a6-43e1-a000-b5d7be6d9cba", "next_3_days.avg_games_per_session": 9.5}]]
["fba1f6bc-73f6-db6a-9b9e-2a848b8f6ee3", []]
["4ac60a7c-fcd4-351b-f6cf-1bb868f813fa", []]
["8c400eb3-3612-9b3f-cb15-f4947f0af565", []]
["e728a2ed-8fd7-e81b-3ce1-851a5580c5bd", []]
["32cec9c5-42ba-3329-3831-af5b8d432937", []]
```

Each entry consists of an array with 2 items:

* `user_id`, the __Identity__ from the Streaming BTS. 
* An object with the remaining values of the record.

Note:- It will print once for each user in the raw events file wether the window aggregates exist or not.
