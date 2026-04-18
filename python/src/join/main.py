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
        self.client_eof_counts = {}  # {client_id: int}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handle the SIGTERM signal for graceful shutdown.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    def _process_data(self, client_id, partial_top):
        """
        Process a data message by aggregating top fruits and sending the final result when all aggregators have sent data.
        """
        logging.info(f"Processing partial top for client: {client_id}")
        self.fruit_top_by_client[client_id] = (
            self.fruit_top_by_client.get(client_id, []) + partial_top
        )
        self.client_eof_counts[client_id] = self.client_eof_counts.get(client_id, 0) + 1
        if self.client_eof_counts[client_id] == AGGREGATION_AMOUNT:
            final_top = sorted(
                self.fruit_top_by_client[client_id], key=lambda x: x[1], reverse=True
            )[:TOP_SIZE]
            self.output_queue.send(
                message_protocol.internal.serialize([client_id, final_top])
            )
            del self.fruit_top_by_client[client_id]
            del self.client_eof_counts[client_id]

    def process_message(self, message, ack, nack):
        """
        Process a message by handling the corresponding data.
        """
        fields = message_protocol.internal.deserialize(message)
        self._process_data(*fields)
        ack()

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
