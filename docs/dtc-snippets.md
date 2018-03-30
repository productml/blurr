DTC snippets

# Splits

## One block per day

```YAML
Split: datetime.strptime('source.start_time', date_format) != datetime.strptime('start_time', date_format)      
```

## One block per session

```yaml
Split: source.session_id != session_id
```

## Inactivity based split

```YAML
Split: (time - end_time).minutes > 30
# Calculates the distance in minutes between event time and the end time (time last event was received) of the current rollup.  Split happens when the difference is greater than 30 minutes.
```

# Advanced DTC functions
