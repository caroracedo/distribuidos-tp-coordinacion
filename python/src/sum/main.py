import os
import logging
import signal
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

EXPECTED_DATA_FIELDS_LENGTH = 3
EOF_WAIT_TIMEOUT = 1


class SumFilter:
    def __init__(self):
        """
        Initialize the filter by setting up input and output messaging infrastructure, internal state, and signal handling for graceful shutdown.
        """
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.input_control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.fruit_amounts_by_client = {}  # {client_id: {fruit: amount}}

        self.dict_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self.control_thread = None

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handle the SIGTERM signal for graceful shutdown.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    def _get_aggregator_index(self, fruit):
        """
        Get a deterministic aggregator index for a fruit.
        """
        return (
            int(hashlib.md5(fruit.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT
        )

    def _process_data(self, client_id, fruit, amount):
        """
        Process a data message by updating the fruit amounts for the client.
        """
        with self.dict_lock:
            logging.info(f"Processing fruit amount for client: {client_id}")
            client_fruit_amounts = self.fruit_amounts_by_client.setdefault(
                client_id, {}
            )
            client_fruit_amounts[fruit] = client_fruit_amounts.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof_from_gateway(self, client_id):
        """
        Process an EOF message from the gateway by propagating it after a short wait.
        """
        logging.info(f"Processing EOF from gateway for client: {client_id}")
        self.shutdown_event.wait(timeout=EOF_WAIT_TIMEOUT)
        if not self.shutdown_event.is_set():
            self.output_control_exchange.send(
                message_protocol.internal.serialize([client_id])
            )

    def _process_eof_from_control(self, client_id):
        """
        Process an EOF message from control by sending final fruit amounts to respective aggregators and broadcasting EOF to all of them.
        """
        logging.info(f"Processing EOF from control for client: {client_id}")
        with self.dict_lock:
            if client_id in self.fruit_amounts_by_client:
                for final_fruit_item in self.fruit_amounts_by_client[
                    client_id
                ].values():
                    self.data_output_exchanges[
                        self._get_aggregator_index(final_fruit_item.fruit)
                    ].send(
                        message_protocol.internal.serialize(
                            [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                        )
                    )
                del self.fruit_amounts_by_client[client_id]

        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_message_from_gateway(self, message, ack, nack):
        """
        Process a message from the gateway by determining if it is a data or EOF message and handling accordingly.
        """
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == EXPECTED_DATA_FIELDS_LENGTH:
            self._process_data(*fields)
        else:
            self._process_eof_from_gateway(*fields)
        ack()

    def process_data_message_from_control(self, message, ack, nack):
        """
        Process a message from control by handling the corresponding EOF.
        """
        fields = message_protocol.internal.deserialize(message)
        self._process_eof_from_control(*fields)
        ack()

    def start(self):
        """
        Start the filter by consuming messages from the input queue and control exchange.
        """
        self.control_thread = threading.Thread(
            target=self.input_control_exchange.start_consuming,
            args=(self.process_data_message_from_control,),
            daemon=True,
        )
        self.control_thread.start()
        self.input_queue.start_consuming(self.process_data_message_from_gateway)

    def stop(self):
        """
        Stop consuming messages and close all connections.
        """
        self.shutdown_event.set()
        self.input_queue.stop_consuming()
        self.input_control_exchange.stop_consuming()
        self.input_queue.close()
        self.input_control_exchange.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()
        if self.control_thread and self.control_thread.is_alive():
            self.control_thread.join()


def main():
    """
    Main function that initializes and runs the SumFilter.
    """
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
    except Exception as e:
        logging.error(f"Error executing SumFilter: {e}")
    finally:
        try:
            sum_filter.stop()
        except Exception as e:
            logging.error(f"Error while stopping SumFilter: {e}")
    return 0


if __name__ == "__main__":
    main()
