#!/usr/bin/env python3
"""
HTTP to Kafka Forwarder
Receives CDC events from Debezium HTTP sink → forwards to Kafka

Usage:
    python http_to_kafka_forwarder.py --port 8888 --kafka localhost:9092
"""

import argparse
import json
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCForwarder(BaseHTTPRequestHandler):
    """HTTP handler that forwards CDC events to Kafka."""

    kafka_producer = None

    def do_POST(self):
        """Handle POST request from Debezium."""
        try:
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)

            # Parse CDC event
            event = json.loads(body)

            # Extract topic from event metadata
            # Debezium sends topic in the event payload
            topic = event.get('topic') or self._extract_topic_from_event(event)

            if not topic:
                logger.warning("No topic found in event, using default")
                topic = "debezium.cdc.events"

            # Forward to Kafka
            self.kafka_producer.send(
                topic=topic,
                value=body,
                key=event.get('key', '').encode('utf-8') if event.get('key') else None
            )

            logger.info(f"✅ Forwarded event to Kafka topic: {topic}")

            # Send 200 OK
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            self.send_response(500)
            self.end_headers()

    def _extract_topic_from_event(self, event):
        """Extract topic name from Debezium event structure."""
        # Debezium event structure: {"payload": {"source": {...}}}
        if 'payload' in event:
            payload = event['payload']
            if 'source' in payload:
                source = payload['source']
                # Build topic name from source metadata
                connector = source.get('name', 'unknown')
                schema = source.get('schema', 'public')
                table = source.get('table', 'unknown')
                return f"{connector}.{schema}.{table}"
        return None

    def log_message(self, format, *args):
        """Suppress default HTTP server logs."""
        pass


def main():
    parser = argparse.ArgumentParser(description="HTTP to Kafka Forwarder for Debezium")
    parser.add_argument("--port", type=int, default=8888,
                        help="HTTP port to listen on (default: 8888)")
    parser.add_argument("--kafka", type=str, default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")

    args = parser.parse_args()

    # Initialize Kafka producer
    logger.info(f"🔌 Connecting to Kafka: {args.kafka}")
    producer = KafkaProducer(
        bootstrap_servers=args.kafka,
        value_serializer=lambda v: v if isinstance(v, bytes) else v.encode('utf-8')
    )

    # Set Kafka producer on handler class
    CDCForwarder.kafka_producer = producer

    # Start HTTP server
    server = HTTPServer(('0.0.0.0', args.port), CDCForwarder)
    logger.info(f"🚀 HTTP to Kafka Forwarder running on port {args.port}")
    logger.info(f"📡 Forwarding CDC events to Kafka: {args.kafka}")
    logger.info(f"   Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutting down...")
        producer.close()


if __name__ == "__main__":
    main()
