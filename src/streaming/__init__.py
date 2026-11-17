"""Streaming component (Final Delivery): Kafka producer, consumer, event contract and metrics.

The batch pipeline (Airflow + Great Expectations + MySQL DW) lives in the rest of ``src`` and is
reused unchanged from the Second Delivery. Everything here only READS the Data Warehouse and writes
to its own ``stream_*`` tables.
"""
