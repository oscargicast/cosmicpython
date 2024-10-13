import unittest

from unittest.mock import Mock


class DeliveryService:
    def __init__(self, email_service):
        self.email_service = email_service

    def deliver(self):
        self.email_service.send_email("sending email... Delivery is done")
        return "Delivery done"


class TestDeliveryService(unittest.TestCase):

    def test_process_with_mock_logger(self):
        mock_email_service = Mock()
        service = DeliveryService(mock_email_service)

        result = service.deliver()

        mock_email_service.send_email.assert_called_with(
            "sending email... Delivery is done"
        )
        self.assertEqual(result, "Delivery done")
