import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        """
        Initializes the SumFilter by setting up the input queue and data output exchanges.
        A signal handler for SIGTERM is also registered to ensure graceful shutdown of the filter.
        """
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_fruit = {}

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """
        Handles the SIGTERM signal by stopping the SumFilter.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    def _process_data(self, fruit, amount):
        """
        Processes a data message by updating the total amount for the given fruit.
        """
        logging.info(f"Process data")
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self):
        """
        Processes an EOF message by broadcasting the final fruit items and an EOF message.
        """
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([]))

    def process_data_messsage(self, message, ack, nack):
        """
        Processes a message by deserializing it and determining whether it's a data message or an EOF message.
        """
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        """
        Start consuming messages from the input queue.
        """
        self.input_queue.start_consuming(self.process_data_messsage)

    def stop(self):
        """
        Stop consuming messages, close the input queue, and close all data output exchanges.
        """
        self.input_queue.stop_consuming()
        self.input_queue.close()
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
