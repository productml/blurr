# Tutorial 1: Event Aggregation : Streaming DTC

In this tutorial we'll learn how Blurr performs basic data aggregation. The following concepts will be introduced:

* The _Data Transform Configuration_ document (DTC)
* The basic blocks of a DTC: `Header`, `Store`, `Identity` and `Aggregates`
* How events are processed and aggregated one by one by a `Block Aggregate`
* How `Identity` and `Split` are used to create new records.

Try the code from this example [launching a Jupyter Notebook](https://mybinder.org/v2/gh/productml/blurr/master?filepath=examples%2Ftutorial).

## 1. Events

Our sample application is a fairly simple game in which the player can either win or lose.

Users can play as many games as they want in one sitting, what we call a __session__. Each event will have a `session_id` to identify the session in which the game was played.

This app collects 2 types of events:

* `game_start`: sent when a user starts a new game.
* `game_end`: sent when a user finishes a game. Contains a `won` field that marks whether the user won the game (`1` for a win, `0` for a loss).

Example:

```json
{
  "user_id": "09C1", # unique user identifier
  "session_id": "915D", # the session the game is played on
  "event_id": "game_start", # type of the event
  "country" : "US", # demographic data
  "timestamp": "2018/03/04 09:01:03" # time of the occurrence of the event
}
```

Events are stored as `JSON` entries, split by a new line character `\n`:

```json
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_start", "timestamp": "2018/03/04 09:01:03" }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_end", "won": 1, "timestamp": "2018/03/04 09:03:04"  }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_start", "timestamp": "2018/03/04 09:04:31"  }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_end", "won": 1, "timestamp": "2018/03/04 09:10:22"  }
{ "user_id": "B6FA", "session_id": "D043", "country" : "US", "event_id": "game_start", "timestamp": "2018/03/04 09:11:03"  }
{ "user_id": "B6FA", "session_id": "D043", "country" : "US", "event_id": "game_end", "won": 1, "timestamp": "2018/03/04 09:21:55"  }
{ "user_id": "09C1", "session_id": "T8KA", "country" : "UK", "event_id": "game_start", "timestamp": "2018/03/04 09:22:13"  }
{ "user_id": "09C1", "session_id": "T8KA", "country" : "UK", "event_id": "game_end", "won": 1, "timestamp": "2018/03/04 09:25:24"  }
```

Our goal is to __collect session statistics__, such as games played in a session by a user, or the total games won.

## 2. The Transformation

For the sequence of events listed before we're interested in the __number of games played__ and __number of games won__ by player and session.

We will transform the original sequence of events into an series of records containing the desired information:


| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 2            | 2         |
| D043        | B6FA       | 1            | 1         |
| T8KA        | 09C1       | 1            | 1         |


In order to obtain this transformation, Blurr will process the events sequentially one by one using this __Data Transform Configuration (DTC)__ file.

```yaml
Type: Blurr:Transform:Streaming
Version: '2018-03-01'
Name : sessions

Store:
   - Type: Blurr:Store:Memory
     Name: hello_world_store

Identity: source.user_id

Time: parser.parse(source.timestamp)

Aggregates:

 - Type: Blurr:Aggregate:Block
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

Let's have a quick look at the five main blocks of this DTC: `Header`, `Store`, `Time`, `Identity` and `Aggregates`.

### 2.1. Header

```yaml
Type: Blurr:Transform:Streaming
Version: '2018-03-07'
Name : sessions
```

`Type` and `Version` identify the capabilities of the DTC.

Further in this series of tutorials we'll introduce different types of DTCs, such as `Window` DTC. We'll also learn how DTCs are combined, the reason why every DTC must have a unique `Name`.

### 2.2. Store

```yaml
Store:
   - Type: Blurr:Store:Memory
     Name: hello_world_store
```

The output of a transformation is a collection of __records__ persisted in a datastore. For this example we'll be using an in-memory datastore.

### 2.3. Identity

Every DTC has an __Identity__, which is always a property of the events being processed. In our example, the Identity is the property `user_id`:

```yaml
Identity: source.user_id
```

> In a DTC we can access the properties of the event being processed using the `source` keyword, as in `source.user_id` or `source.won`

The Identity is the main dimension around which events are aggregated. At this stage, let's just think on the Identity as a mandatory field that is part of both the original events and the output.

### 2.4. Time

Used to parse timestamp expressions from events.

```yaml
Time: parser.parse(source.timestamp, 'YYYY/mm/dd HH:MM:SS')
```

Among other things, Blurr uses `Time` to internally generates `start_time` and `end_time` values for each session. We'll see in the next tutorial why this is critical to certain aggregation features.


### 2.5. Aggregates

This is where the magic happens. Aggregates define the nature of the transformation. Our example has a single Aggregate of type `Block Aggregate`. Different types of Aggregates will be introduced in the next tutorials.

We'll learn how the transformation happens in the next section by examining the flow of data event by event.


## 3. Data Flow

Events are processed one by one, and then aggregated as defined in the `Block Aggregate`:

```yaml
Aggregates:
 - Type: Blurr:Aggregate:Block
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

In order to understand how `Block Aggregate` aggregates data we'll use the sequence of events from the initial section.

### 3.1. First Event : `game_start`

The first event is processed when the first user starts playing the game:

```json
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_start" }
```

Aggregates are calculated taking into account the historical series of events. In this case, `games_played` is increased by `1` every time a new game starts:

```yaml
- Name: games_played
  Type: integer
  When: source.event_id == 'game_start'
  Value: session_stats.games_played + 1
```

Whenever a `game_start` event is received, the existing `session_stats.games_played` record is increased by one.

> You can always access a field in the previously saved record by using the name of the Aggregate and the name of the field, such as in `session_stats.games_played` or `session_stats.games_won`.

Since this is the first historic event, the following will happen:

1. A new record is created in the store with the default values for each field (`""` for `string`, `0` for `integer`)
2. The event is processed, updating the record using the `Value` expressions for the field. The content of `Value` can be __any Python expression__.

The resulting record is added to the store:

| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 1            | 0         |


### 3.2. Second Event : `game_end`

The user from the 1st event wins a game:

```json
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_end", "won": 1 }
```

Processing this event results in the existing record having `games_won` increased by one:

```yaml
- Name: games_won
  Type: integer
  When: source.event_id == 'game_end' and source.won == '1'
  Value: session_stats.games_won + 1
```


| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 1            | 1         |

### 3.3. 3rd and 4th Event : user plays a new game

The same user plays and wins a new game in the same session:
```json
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_start" }
{ "user_id": "09C1", "session_id": "915D", "country" : "US", "event_id": "game_end", "won": 1 }
```

After processing both events,`games_played` and `games_won` are increased by one.

| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 2            | 2         |

### 3.4. 5th and 6th Event : a new user plays a game

A second user starts a new game:

```json
{ "user_id": "B6FA", "session_id": "D043", "country" : "US", "event_id": "game_start" }
```

Previously we defined `source.user_id` as the __Identity__ of the DTC:

```yaml
Identity: source.user_id
```

Here we introduce one of the roles of the Identity: whenever an event is received and the Identity value doesn't exist in the store (like when a new user plays a game), a new record is added:

| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 2            | 2         |
| D043        | B6FA       | 1            | 0         |

After the `game_end` event is received, the record is updated with the win result:

```json
{ "user_id": "B6FA", "session_id": "D043", "country" : "US", "event_id": "game_end", "won": 1 }
```

| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 2            | 2         |
| D043        | B6FA       | 1            | 1         |


### 3.5. 7th Event : a user starts a new session

After some time, the user decides to play again. This is considered a new session from the game perspective:

```json
{ "user_id": "09C1", "session_id": "T8KA", "country" : "UK", "event_id": "game_start" }
```

There's an element of the Aggregate we haven't covered yet, `Split`:

```yaml
Split: source.session_id != session_stats.session_id
```

Splitting is a key component of event aggregation. A `Block Aggregate` always contains a `Split` expression, defining __when a new record has to be created__ in the store.

`Split` is evaluated for every event. In all the previous events the result of this evaluation was `False`,  but for this case it evaluates to `True`:

* `source.session_id` is the value of the property `session_id` in the event being processed (`T8KA`).
* `session_stats.session_id` is the value of `session_id` __in the last record saved for the same Identity__ (i.e. the last session played by the user, `915D`)

```python
source.session_id != session_stats.session_id
"T8KA" != "915D" # True
```

As a result of the evaluation of `Split` a new record is created in the store:


| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 2            | 2         |
| D043        | B6FA       | 1            | 1         |
| T8KA        | 09C1       | 1            | 0         |


### 3.6. 8th Event : `game_end`

The previous user finishes the game:

```json
{ "user_id": "09C1", "session_id": "T8KA", "country" : "US", "event_id": "game_end", "won": 1 }
```

`Split` evaluates to `False`, since `session_id` is the same for the last record saved from the same user (created after the previous event):

```python
source.session_id != session_stats.session_id
"T8KA" != "T8KA" # False
```

No record is created. The last record for that user is updated instead:

| session_id  | user_id    | games_played | games_won |
| ----------- | ---------- | ------------ | --------- |
| 915D        | 09C1       | 2            | 2         |
| D043        | B6FA       | 1            | 1         |
| T8KA        | 09C1       | 1            | 1         |


## 4. Previewing the transformation using Blurr CLI

We can preview the result of the transformation using `blurr transform` command:

```bash
$ blurr transform --streaming-dtc sessions-dtc.yml events.log

["09C1/session_stats/2018-03-04T09:10:22", {"session_id" : "915D", "games_played": "2", "games_won": "2"}]
["B6FA/session_stats", {"session_id" : "D043", "games_played": "1", "games_won": "1"}]
["09C1/session_stats", {"session_id" : "T8KA", "games_played": 1, "games_won": "1"}]
```

`transform` prints the result of the transform in JSON format, which is slightly different from the table representation.

Each entry consists of an array with 2 items:

* A `session_id/Aggregate_name/timestamp` string. This represents the __Identity__ (`user_id`) in the tables.
* An object with the remaining values of the record.

As we can see, not all the Identity representations have a timestamp. A "session end" timestamp is added after a new record is added for an Identity (after the `Split` condition is fulfilled), so only the last session of a user will not have a timestamp.
