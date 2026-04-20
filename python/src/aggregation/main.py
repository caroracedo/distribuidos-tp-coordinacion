import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

EXPECTED_DATA_FIELDS_LENGTH = 3


class AggregationFilter:

    def __init__(self):
        """
        Initialize the filter by setting up input and output messaging infrastructure, internal state, and signal handling for graceful shutdown.
        """
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_amounts_by_client = {}  # {client_id: {fruit: amount}}
        self.client_eof_counts = {}  # {client_id: int}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    # --- Signal Handling Methods --- #

    def _handle_sigterm(self, signum, frame):
        """
        Handle the SIGTERM signal for graceful shutdown.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    # --- Message Processing Methods --- #

    def _process_data(self, client_id, fruit, amount):
        """
        Process a data message by updating the fruit amounts for the client.
        """
        logging.info(f"Processing fruit amount for client: {client_id}")
        fruit_top = self.fruit_amounts_by_client.setdefault(client_id, {})
        fruit_top[fruit] = fruit_top.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        """
        Process an EOF message by computing and sending the top fruits when all EOF messages are received.
        """
        logging.info(f"Received EOF for client: {client_id}")
        self.client_eof_counts[client_id] = self.client_eof_counts.get(client_id, 0) + 1
        if self.client_eof_counts[client_id] == SUM_AMOUNT:
            fruit_chunk = sorted(self.fruit_amounts_by_client[client_id].values())[
                -TOP_SIZE:
            ]
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, fruit_top])
            )
            del self.fruit_amounts_by_client[client_id]
            del self.client_eof_counts[client_id]

    # --- Callback Methods --- #

    def process_message(self, message, ack, nack):
        """
        Process a message by determining if it is a data or EOF message and handling accordingly.
        """
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == EXPECTED_DATA_FIELDS_LENGTH:
                self._process_data(*fields)
            else:
                self._process_eof(*fields)
            ack()
        except Exception:
            nack()
            raise

    # --- Lifecycle Methods --- #

    def start(self):
        """
        Start the filter by consuming messages from the input exchange.
        """
        self.input_exchange.start_consuming(self.process_message)

    def stop(self):
        """
        Stop consuming messages and close all connections.
        """
        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        self.output_queue.close()


def main():
    """
    Main function that initializes and runs the AggregationFilter.
    """
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except Exception as e:
        logging.error(f"Error executing AggregationFilter: {e}")
    finally:
        try:
            aggregation_filter.stop()
        except Exception as e:
            logging.error(f"Error while stopping AggregationFilter: {e}")
    return 0


if __name__ == "__main__":
    main()
