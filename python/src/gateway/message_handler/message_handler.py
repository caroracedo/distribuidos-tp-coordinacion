import uuid

from common import message_protocol

EOF = "EOF"


class MessageHandler:

    def __init__(self):
        """
        Initializes the MessageHandler by generating a unique client ID for the instance.
        """
        self.client_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        """
        Serializes a data message by combining the client ID with the message content and using the internal serialization method.
        """
        return message_protocol.internal.serialize([self.client_id, *message])

    def serialize_eof_message(self, message):
        """
        Serializes an EOF message by including the client ID and using the internal serialization method.
        """
        return message_protocol.internal.serialize([self.client_id, EOF])

    def deserialize_result_message(self, message):
        """
        Deserializes a result message using the internal deserialization method and checks if the client ID in the message matches the instance's client ID. If it matches, the fruit top is returned; otherwise, None is returned.
        """
        client_id, fruit_top = message_protocol.internal.deserialize(message)
        return fruit_top if client_id == self.client_id else None
