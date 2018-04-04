# Splits

## One block per day

```YAML
Split: datetime.strptime('source.start_time', "%d %m %Y") != datetime.strptime('start_time', "%d %m %Y")      
```

## One block per session_id

```yaml
Split: source.session_id != session_id
```

## Inactivity based split

```YAML
Split: (time - end_time).minutes > 30
# Calculates the distance in minutes between event time and the end time (time last event was received) of the current block.  Split happens when the difference is greater than 30 minutes.
```

# Advanced DTC functions

## Create a map

```yaml
  - Name: id_to_value
    Type: map
    Value: BlockAggregateName.id_to_value.set(source.id, source.value)
```

<<<<<<< HEAD
## Merge 2 lists in source data to create a list of tuples
=======
## Merge 2 strings with comma separated values in source data to create a list of tuples
>>>>>>> master

```yaml
Fields:
  - Name: products_bought
    Type: list
<<<<<<< HEAD
    Value: zip(BlockAggregateName.products_bought.append(source.item_id), BlockAggregateName.products_bought.append(source.value))
=======
    Value: item_stats.products_bought.extend(list(zip([val.strip() for val in source.item_id.split(',')], [float(val.strip()) for val in source.value.split(',')])))
>>>>>>> master
    # This creates a list of tuples [(item_id1, value1), (item_id2, value2)...]
    When: source.event_name = 'ecommerce_purchase' and source.currency = 'usd'
```

# Data cleansing

## Convert strings in raw data to a float

If the raw data has a field `"txn_amount": "9.99"`, which is a float but formatted as a string, it can be converted to a float to perform operations in a field value

```
- Name: purchase_amount
  Type: float
  Value: float(source.txn_amount) + game_stats.purchase_amount
  When: source.event_id == 'purchase' and source.purchase_source == 'offer'
```
