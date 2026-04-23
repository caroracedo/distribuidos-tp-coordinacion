import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


class MessageMiddlewareRabbitMQBase:

    def start_consuming(self, on_message_callback):
        """
        Starts consuming messages from the queue or exchange and invokes the callback for each message received.
        The callback receives the message body, an ack function, and a nack function as parameters.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """

        def ack_nack_callback_adapter(ch, method, properties, body):
            def ack_message():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack_message():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack_message, nack_message)

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=ack_nack_callback_adapter
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while consuming: {e}"
            )

    def stop_consuming(self):
        """
        Stops consuming messages from the queue or exchange.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        """
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )

    def stop_consuming_threadsafe(self):
        """
        Stops consuming messages from the queue or exchange in a thread-safe manner.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        """
        try:
            self.connection.add_callback_threadsafe(self.channel.stop_consuming)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )

    def close(self):
        """
        Closes the connection to the RabbitMQ server.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareCloseError.
        """
        try:
            self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(
                f"An error occurred while closing the connection: {e}"
            )


class MessageMiddlewareQueueRabbitMQ(
    MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue
):

    def __init__(self, host, queue_name):
        """
        Initializes the connection to the RabbitMQ server and declares the queue.
        """
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        self.channel.queue_declare(queue=self.queue_name, durable=True)

    def send(self, message):
        """
        Sends a message to the queue.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while sending: {e}"
            )


class MessageMiddlewareExchangeRabbitMQ(
    MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange
):

    def __init__(self, host, exchange_name, routing_keys):
        """
        Initializes the connection to the RabbitMQ server, declares the exchange and binds a temporary queue to the exchange with the specified routing keys.
        """
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="direct", durable=True
        )

        result = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )

    def send(self, message):
        """
        Sends a message to the exchange with the specified routing keys.
        If the connection to the middleware is lost, it raises MessageMiddlewareDisconnectedError.
        If an internal error occurs that cannot be resolved, it raises MessageMiddlewareMessageError.
        """
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent
                    ),
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"The connection to the middleware was lost: {e}"
            )
        except Exception as e:
            raise MessageMiddlewareMessageError(
                f"An internal error occurred while sending: {e}"
            )
