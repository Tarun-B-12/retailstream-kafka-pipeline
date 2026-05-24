import sqlite3

DB_PATH = "storage/retailstream.db"

def initialize_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_events (
            event_id TEXT,
            timestamp TEXT,
            store_id TEXT,
            product_category TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            payment_method TEXT,
            customer_segment TEXT,
            revenue REAL,
            is_high_value INTEGER,
            hour_of_day INTEGER,
            anomaly_flags TEXT,
            has_anomaly INTEGER
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            event_id TEXT,
            timestamp TEXT,
            store_id TEXT,
            total_amount REAL,
            anomaly_flags TEXT
        )
    """)
    con.commit()
    con.close()
    print("Database initialized.")

def insert_event(event):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO raw_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        event.get('event_id'),
        event.get('timestamp'),
        event.get('store_id'),
        event.get('product_category'),
        event.get('quantity'),
        event.get('unit_price'),
        event.get('total_amount'),
        event.get('payment_method'),
        event.get('customer_segment'),
        event.get('revenue'),
        int(event.get('is_high_value', False)),
        event.get('hour_of_day'),
        event.get('anomaly_flags'),
        int(event.get('has_anomaly', False)),
    ])
    if event.get('has_anomaly'):
        con.execute("""
            INSERT INTO anomalies VALUES (?, ?, ?, ?, ?)
        """, [
            event.get('event_id'),
            event.get('timestamp'),
            event.get('store_id'),
            event.get('total_amount'),
            event.get('anomaly_flags'),
        ])
    con.commit()
    con.close()