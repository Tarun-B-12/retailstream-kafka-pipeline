def transform_event(event):
    """Add derived fields to each event."""
    event['revenue'] = round(event['total_amount'], 2)
    event['is_high_value'] = event['total_amount'] > 1000
    event['hour_of_day'] = int(event['timestamp'][11:13])
    return event