import unittest

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from main import CONNECTION_STR
from main import QUEUE_NAME


class QueueTest(unittest.TestCase):
    servicebus_client = None

    @classmethod
    def setUpClass(cls):
        cls.servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

    @classmethod
    def tearDownClass(cls):
        cls.servicebus_client = None

    def test_queue_is_empty(self):
        with self.servicebus_client:
            receiver = self.servicebus_client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=1)

            with receiver:
                for msg in receiver:
                    self.assertIsNone(msg)

    def test_sending_single_message(self):
        with self.servicebus_client:
            sender = self.servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
            with sender:
                message = ServiceBusMessage("{}")
                sender.send_messages(message)

    def test_sending_list_of_messages(self):
        with self.servicebus_client:
            sender = self.servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
            with sender:
                messages = [ServiceBusMessage("{}") for _ in range(5)]
                sender.send_messages(messages)

    def test_sending_batch_message(self):
        with self.servicebus_client:
            sender = self.servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
            with sender:
                batch_message = sender.create_message_batch()
                for _ in range(5):
                    batch_message.add_message(ServiceBusMessage("{}"))
                sender.send_messages(batch_message)


if __name__ == '__main__':
    unittest.main()
