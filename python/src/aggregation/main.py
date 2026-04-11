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
        Initializes the AggregationFilter by setting up the input exchange and output queue.
        A signal handler for SIGTERM is also registered to ensure graceful shutdown of the filter.
        """
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top_by_client = {}  # {client_id: [fruit]}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the AggregationFilter.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    def _process_data(self, client_id, fruit, amount):
        """
        Processes a data message by updating the top fruits for the given client ID.
        """
        logging.info(f"Processing data message for client: {client_id}")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                fruit_top[i] = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        """
        Processes an EOF message by sending the top fruits for the given client ID to the output queue and then removing the client ID from the fruit top.
        """
        logging.info(f"Received EOF for client: {client_id}")
        if client_id in self.fruit_top_by_client:
            fruit_chunk = list(self.fruit_top_by_client[client_id][-TOP_SIZE:])
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
            del self.fruit_top_by_client[client_id]

    def process_messsage(self, message, ack, nack):
        """
        Processes a message by determining if it's a data message or an EOF message and calling the appropriate processing function.
        """
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == EXPECTED_DATA_FIELDS_LENGTH:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        """
        Starts consuming messages from the input exchange.
        """
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        """
        Stop consuming messages, close the input exchange and close the output queue.
        """
        self.input_exchange.stop_consuming()
        self.input_exchange.close()
        self.output_queue.close()


def main():
    """
    Main function that initializes the AggregationFilter and starts the filter.
    """
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
