import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        """
        Initialize the MessageHandler by generating a unique client ID.
        """
        self.client_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        """
        Serialize a data message with the client ID.
        """
        return message_protocol.internal.serialize([self.client_id, *message])

    def serialize_eof_message(self, message):
        """
        Serialize an EOF message with the client ID.
        """
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        """
        Deserialize a result message and return the fruit top if the client ID matches.
        """
        client_id, fruit_top = message_protocol.internal.deserialize(message)
        return fruit_top if client_id == self.client_id else None
