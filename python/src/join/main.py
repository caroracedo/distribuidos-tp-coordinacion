import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        """
        Initialize the filter by setting up input and output queues, internal state, and signal handling for graceful shutdown.
        """
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top_by_client = {}  # {client_id: [(fruit, amount)]}
        self.completed_count_by_client = {}  # {client_id: int}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    # --- Signal Handling Methods --- #

    def _handle_sigterm(self, signum, frame):
        """
        Handle the SIGTERM signal for graceful shutdown.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    # --- Auxiliary Methods --- #

    def _update_fruit_top(self, client_id, partial_top):
        """
        Update the fruit top for a client by adding the partial top received from an aggregator and updating the count of completed aggregators.
        """
        self.fruit_top_by_client[client_id] = self.fruit_top_by_client.get(
            client_id, []
        ) + [fruit_item.FruitItem(fruit, amount) for fruit, amount in partial_top]
        self.completed_count_by_client[client_id] = (
            self.completed_count_by_client.get(client_id, 0) + 1
        )

    def _flush_fruit_top(self, client_id):
        """
        Flush the aggregated top fruits for a client, sending the result and cleaning up internal state.
        """
        fruit_chunk = sorted(self.fruit_top_by_client[client_id])[-TOP_SIZE:]
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
        del self.completed_count_by_client[client_id]

    # --- Message Processing Methods --- #

    def _process_data(self, client_id, partial_top):
        """
        Process a data message by updating the fruit top for the client and flushing the result if all aggregators have reported.
        """
        logging.info(f"Processing partial top for client: {client_id}")
        self._update_fruit_top(client_id, partial_top)
        if self.completed_count_by_client.get(client_id, 0) == AGGREGATION_AMOUNT:
            self._flush_fruit_top(client_id)

    # --- Callback Methods --- #

    def process_message(self, message, ack, nack):
        """
        Process a message by handling the corresponding data.
        """
        try:
            fields = message_protocol.internal.deserialize(message)
            self._process_data(*fields)
            ack()
        except Exception:
            nack()
            raise

    # --- Lifecycle Methods --- #

    def start(self):
        """
        Start the filter by consuming messages from the input queue.
        """
        self.input_queue.start_consuming(self.process_message)

    def stop(self):
        """
        Stop consuming messages and close all connections.
        """
        self.input_queue.stop_consuming()
        self.input_queue.close()
        self.output_queue.close()


def main():
    """
    Main function that initializes and runs the JoinFilter.
    """
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except Exception as e:
        logging.error(f"Error executing JoinFilter: {e}")
    finally:
        try:
            join_filter.stop()
        except Exception as e:
            logging.error(f"Error while stopping JoinFilter: {e}")
    return 0


if __name__ == "__main__":
    main()
