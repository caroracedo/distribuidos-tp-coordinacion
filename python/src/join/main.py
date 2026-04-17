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
        Initializes the JoinFilter by setting up the input queue and output queue.
        A signal handler for SIGTERM is also registered to ensure graceful shutdown of the filter.
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
        Handles the SIGTERM signal by stopping the JoinFilter.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    def _process_data(self, client_id, partial_top):
        """
        Processes a data message by updating the top fruits for the given client ID and sending the top fruits to the output queue if all EOF messages have been received for the client ID.
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

    def process_messsage(self, message, ack, nack):
        """
        Processes a message by deserializing it and processing the data.
        """
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        self._process_data(*fields)
        ack()

    def start(self):
        """
        Starts consuming messages from the input queue.
        """
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        """
        Stop consuming messages, close the input queue and close the output queue.
        """
        self.input_queue.stop_consuming()
        self.input_queue.close()
        self.output_queue.close()


def main():
    """
    Main function that initializes the JoinFilter and starts the filter.
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
