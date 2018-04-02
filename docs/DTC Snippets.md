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

## Merge 2 lists in source data to create a list of tuples

```yaml
Fields:
  - Name: products_bought
    Type: list
    Value: zip(BlockAggregateName.products_bought.append(source.item_id), BlockAggregateName.products_bought.append(source.value))
    # This creates a list of tuples [(item_id1, value1), (item_id2, value2)...]
```
