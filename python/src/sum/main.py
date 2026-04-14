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


class SumFilter:
    def __init__(self):
        """
        Initializes the SumFilter by setting up the input queue, output control exchange, input control exchange, and data output exchanges.
        A signal handler for SIGTERM is also registered to ensure graceful shutdown of the filter.
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

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the SumFilter.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    def _get_aggregator_index(self, fruit):
        """
        Gets the index of the aggregator for the given fruit by hashing the fruit name and taking the modulus with the number of aggregators.
        """
        return (
            int(hashlib.md5(fruit.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT
        )

    def _process_data(self, client_id, fruit, amount):
        """
        Processes a data message by updating the fruit amounts for the given client ID.
        """
        logging.info(f"Process data for client: {client_id}")
        client_fruit_amounts = self.fruit_amounts_by_client.setdefault(client_id, {})
        client_fruit_amounts[fruit] = client_fruit_amounts.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof_from_gateway(self, client_id):
        """
        Processes an EOF message from the gateway by sending a message to the output control exchange to indicate that the given client ID has sent an EOF message.
        """
        logging.info(f"Processing EOF from gateway for client: {client_id}")
        self.output_control_exchange.send(
            message_protocol.internal.serialize([client_id])
        )

    def _process_eof_from_control(self, client_id):
        """
        Processes an EOF message from the control by sending the final fruit amounts for the given client ID to the appropriate data output exchanges and then sending an EOF message for the client ID to all data output exchanges.
        """
        logging.info(f"Processing EOF from control for client: {client_id}")
        if client_id in self.fruit_amounts_by_client:
            for final_fruit_item in self.fruit_amounts_by_client[client_id].values():
                self.data_output_exchanges[
                    self._get_aggregator_index(final_fruit_item.fruit)
                ].send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )
            del self.fruit_amounts_by_client[client_id]

        logging.info(f"Broadcasting EOF message for client: {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def process_data_messsage_from_gateway(self, message, ack, nack):
        """
        Processes a message from the gateway by deserializing it and determining whether it's a data message or an EOF message, and then calling the appropriate processing function.
        """
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == EXPECTED_DATA_FIELDS_LENGTH:
            self._process_data(*fields)
        else:
            self._process_eof_from_gateway(*fields)
        ack()

    def process_data_messsage_from_control(self, message, ack, nack):
        """
        Processes a message from the control by deserializing it and calling the function to process an EOF message.
        """
        fields = message_protocol.internal.deserialize(message)
        self._process_eof_from_control(*fields)
        ack()

    def start(self):
        """
        Starts consuming messages from the input queue and the input control exchange in separate threads.
        """
        control_thread = threading.Thread(
            target=self.input_control_exchange.start_consuming,
            args=(self.process_data_messsage_from_control,),
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage_from_gateway)

    def stop(self):
        """
        Stop consuming messages from the input queue and the input control exchange, close the input queue, close the input control exchange, and close all data output exchanges.
        """
        self.input_queue.stop_consuming()
        self.input_control_exchange.stop_consuming()
        self.input_queue.close()
        self.input_control_exchange.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()


def main():
    """
    Main function that initializes the SumFilter and starts the filter.
    """
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
