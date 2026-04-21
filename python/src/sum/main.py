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

EXPECTED_GATEWAY_DATA_FIELDS_LENGTH = 3
EXPECTED_CONTROL_EOF_FIELDS_LENGTH = 2


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
        self.control_output_control_exchange = (
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SUM_CONTROL_EXCHANGE,
                [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
            )
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.fruit_amounts_by_client = {}  # {client_id: {fruit: FruitItem}}

        self.local_count_by_client = {}  # {client_id: count}
        self.node_counts_by_client = {}  # {client_id: {node_id: count}}
        self.expected_total_by_client = {}  # {client_id: expected_total}

        self.client_state_lock = threading.Lock()

        self.control_thread = None

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    # --- Signal Handling Methods --- #

    def _handle_sigterm(self, signum, frame):
        """
        Handle the SIGTERM signal for graceful shutdown.
        """
        logging.info("SIGTERM received, stopping the filter...")
        self.stop()

    # --- Auxiliary Methods --- #

    def _update_fruit_amounts(self, client_id, fruit, amount):
        """
        Update the fruit amounts for a client.
        """
        client_fruit_amounts = self.fruit_amounts_by_client.setdefault(client_id, {})
        client_fruit_amounts[fruit] = client_fruit_amounts.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _update_local_count(self, client_id):
        """
        Update and return the local count of processed messages for a client.
        """
        self.local_count_by_client[client_id] = (
            self.local_count_by_client.get(client_id, 0) + 1
        )
        return self.local_count_by_client[client_id]

    def _should_broadcast_update(self, client_id):
        """
        Determine if a local count update should be broadcast.
        """
        return client_id in self.expected_total_by_client

    def _has_reached_expected_total(self, client_id):
        """
        Check if the total processed count (local + global) for a client has reached the expected total.
        """
        local_count = self.local_count_by_client.get(client_id, 0)
        total_node_counts = sum(
            count
            for node_id, count in self.node_counts_by_client.get(client_id, {}).items()
            if node_id != ID
        )
        expected_total = self.expected_total_by_client.get(client_id)
        return (
            expected_total is not None
            and total_node_counts + local_count >= expected_total
        )

    def _pop_client_data(self, client_id):
        """
        Pop the data for a client.
        """
        self.local_count_by_client.pop(client_id, None)
        self.node_counts_by_client.pop(client_id, None)
        self.expected_total_by_client.pop(client_id, None)
        return self.fruit_amounts_by_client.pop(client_id, {})

    def _get_aggregator_index(self, fruit):
        """
        Get a deterministic aggregator index for a fruit.
        """
        return (
            int(hashlib.md5(fruit.encode("utf-8")).hexdigest(), 16) % AGGREGATION_AMOUNT
        )

    def _flush_fruit_amounts(self, client_id, fruit_amounts):
        """
        Flush the fruit amounts for a client, sending them to the appropriate aggregator and notifying the aggregators of the flush.
        """
        for fruit_item in fruit_amounts.values():
            self.data_output_exchanges[
                self._get_aggregator_index(fruit_item.fruit)
            ].send(
                message_protocol.internal.serialize(
                    [client_id, fruit_item.fruit, fruit_item.amount]
                )
            )
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    # --- Message Processing Methods --- #

    def _process_data(self, client_id, fruit, amount):
        """
        Process a data message by updating the internal state and broadcasting the local count if necessary.
        """
        logging.info(f"Processing fruit amount for client: {client_id}")
        with self.client_state_lock:
            self._update_fruit_amounts(client_id, fruit, amount)
            local_count = self._update_local_count(client_id)
            should_broadcast_update = self._should_broadcast_update(client_id)

        if should_broadcast_update:
            self.output_control_exchange.send(
                message_protocol.internal.serialize([client_id, ID, local_count])
            )

    def _process_eof_from_gateway(self, client_id, expected_total):
        """
        Process an EOF message from the gateway by propagating it.
        """
        logging.info(f"Processing EOF from gateway for client: {client_id}")
        self.output_control_exchange.send(
            message_protocol.internal.serialize([client_id, expected_total])
        )

    def _process_eof_from_control(self, client_id, expected_total):
        """
        Process an EOF message from control by storing the expected total and broadcasting the local count.
        """
        logging.info(f"Processing EOF from control for client: {client_id}")
        with self.client_state_lock:
            self.expected_total_by_client[client_id] = expected_total
            local_count = self._update_local_count(client_id)

        self.control_output_control_exchange.send(
            message_protocol.internal.serialize([client_id, ID, local_count])
        )

    def _process_status_update(self, client_id, node_id, reported_count):
        """
        Process a status update message from control by updating the node count and flushing the client data if the expected total has been reached.
        """
        logging.info(f"Processing status update for client: {client_id}")
        with self.client_state_lock:
            self.node_counts_by_client.setdefault(client_id, {})[
                node_id
            ] = reported_count
            fruit_amounts = (
                self._pop_client_data(client_id)
                if self._has_reached_expected_total(client_id)
                else None
            )

        if fruit_amounts is not None:
            self._flush_fruit_amounts(client_id, fruit_amounts)

    # --- Callback Methods --- #

    def process_data_message_from_gateway(self, message, ack, nack):
        """
        Process a message from the gateway by determining if it is a data or EOF message and handling accordingly.
        """
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == EXPECTED_GATEWAY_DATA_FIELDS_LENGTH:
                self._process_data(*fields)
            else:
                self._process_eof_from_gateway(*fields)
            ack()
        except Exception:
            nack()
            raise

    def process_data_message_from_control(self, message, ack, nack):
        """
        Process a message from control exchange by determining if it is an EOF or status update message and handling accordingly.
        """
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == EXPECTED_CONTROL_EOF_FIELDS_LENGTH:
                self._process_eof_from_control(*fields)
            else:
                self._process_status_update(*fields)
            ack()
        except Exception:
            nack()
            raise

    # --- Lifecycle Methods --- #

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
        self.input_queue.stop_consuming()
        self.input_control_exchange.stop_consuming()
        self.input_queue.close()
        self.input_control_exchange.close()
        self.output_control_exchange.close()
        self.control_output_control_exchange.close()
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
