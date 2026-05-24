REQUIRED_FIELDS = [
    'event_id', 'timestamp', 'store_id', 'product_category',
    'quantity', 'unit_price', 'total_amount', 'payment_method', 'customer_segment'
]

def detect_anomalies(event):
    """Flag events with data quality issues."""
    anomalies = []

    # Missing fields
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            anomalies.append(f"Missing field: {field}")

    # Negative or zero amounts
    if event.get('total_amount', 0) <= 0:
        anomalies.append(f"Invalid total_amount: {event.get('total_amount')}")

    # Negative quantity
    if event.get('quantity', 0) <= 0:
        anomalies.append(f"Invalid quantity: {event.get('quantity')}")

    # Outlier: extremely high transaction
    if event.get('total_amount', 0) > 5000:
        anomalies.append(f"Outlier amount: {event.get('total_amount')}")

    event['anomaly_flags'] = '; '.join(anomalies) if anomalies else None
    event['has_anomaly'] = len(anomalies) > 0

    return event