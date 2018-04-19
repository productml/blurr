# Splits

## One block per day

```YAML
Split: datetime.strptime('source.start_time', "%d %m %Y") != datetime.strptime('start_time', "%d %m %Y")      
```

## One block per session_id

```yaml
Split: source.session_id != BlockAggregateName.session_id
```

## Inactivity based split

```YAML
Split: (time - end_time).minutes > 30
# Calculates the distance in minutes between event time and the end time (time last event was received) of the current block.  Split happens when the difference is greater than 30 minutes.
```

# Advanced DTC functions

## Parsing UNIX timestamp
```YAML
Import:
  - { Module: datetime, Identifiers: [ datetime, timezone ]}

Time: datetime.fromtimestamp(source.timestamp, timezone.utc)
```

## Parsing ISO datetime string
```YAML
Import:
  - { Module: dateutil.parser, Identifiers: [ parse ]}

Time: parse(source.event_time)
```

## Create a map

```yaml
  - Name: id_to_value
    Type: map
    Value: BlockAggregateName.id_to_value.set(source.id, source.value)
```

## Merge 2 strings with comma separated values in source data to create a list of tuples

```yaml
Fields:
  - Name: products_bought
    Type: list
    Value: item_stats.products_bought.extend(list(zip([val.strip() for val in source.item_id.split(',')], [float(val.strip()) for val in source.value.split(',')])))
    # This creates a list of tuples [(item_id1, value1), (item_id2, value2)...]
    When: source.event_name = 'ecommerce_purchase' and source.currency = 'usd'
```

# Data cleansing

## Create a boolean value from a condition on a text field in raw data

```yaml
- Name: fb_connected
  Type: boolean
  Value: True if source.signin_method == 'fb' else False
```

## Convert strings in raw data to a float

If the raw data has a field `"txn_amount": "9.99"`, which is a float but formatted as a string, it can be converted to a float to perform operations in a field value

```yaml
- Name: purchase_amount
  Type: float
  Value: float(source.txn_amount) + game_stats.purchase_amount
  When: source.event_id == 'purchase' and source.purchase_source == 'offer'
```

## Flatten a list of list of tuples

```yaml
Fields:
  - Name: products_bought
    Type: list
    Value: [item for sublist in source.products_bought for item in sublist]
    # Flattens the list of list of tuples of products_bought
```
