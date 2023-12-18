import json
import boto3
from botocore.exceptions import ClientError
from tdigest import TDigest
import threading
import os
import random
import time


class EdgeClient:
    """
    A modular class to handle all the matters regarding integration with the framework
    manages the needed objects for representing the distribution
    manages the communication with AWS SQS.
    Configuration is read from a file named 'configs'.
    """

    def __init__(self, config_file='client_configs.json'):
        """
        Initialize the SQS client with configurations from the provided file.
        """
        self.configs = self.read_config(config_file)
        self.sqs = boto3.client('sqs', endpoint_url=self.configs["endpoint_url"])
        self.digest = TDigest()
        self.lock = threading.Lock()

    @staticmethod
    def read_config(file_path):
        """
        Read configuration from a file.
        """
        try:
            dir_path = os.path.dirname(os.path.abspath(__file__))
            abs_file_path = os.path.join(dir_path, file_path)

            with open(abs_file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            raise Exception("Configuration file not found.")
        except json.JSONDecodeError:
            raise Exception("Error parsing the configuration file.")

    def update_digest_with_vals(self, new_values: list):
        with self.lock:
            self.digest.batch_update(new_values)

    def send_t_digest(self):
        """
        Sends the T-Digest to the SQS queue.
        """
        try:
            with self.lock:
                response = self.sqs.send_message(
                    QueueUrl=self.configs['queue_url'],
                    MessageBody=json.dumps(self.digest.to_dict())
                )

                # after sending the digest, we reset it.
                self.digest = TDigest()

                return response

        except ClientError as e:
            raise Exception(f"An error occurred: {e}")

    def send_any_object(self, item_to_send):
        """
        Sends any given object to the SQS queue.
        :param item_to_send: the item to be sent, has to be JSON serializable
        """
        try:
            with self.lock:
                response = self.sqs.send_message(
                    QueueUrl=self.configs['queue_url'],
                    MessageBody=json.dumps(item_to_send)
                )

                return response

        except ClientError as e:
            raise Exception(f"An error occurred: {e}")

    def clear_queue_from_all_messages(self):
        self.sqs.purge_queue(QueueUrl=self.configs['queue_url'])


class MockEdgeClient:
    """
    A mock class for EdgeClient that simulates interactions with AWS SQS.
    Instead of actual communication, it waits for a brief period and returns simulated responses.
    """

    def __init__(self, config_file='client_configs.json'):
        """
        Initialize the mock client with configurations from the provided file.
        """
        self.configs = self.read_config(config_file)
        self.digest = TDigest()
        self.lock = threading.Lock()

    @staticmethod
    def read_config(file_path):
        """
        Simulated read configuration from a file.
        """
        return {"endpoint_url": "mock_url", "queue_url": "mock_queue_url"}

    def update_digest_with_vals(self, new_values: list):
        """
        Simulated update of the T-Digest with new values.
        """
        with self.lock:
            self.digest.batch_update(new_values)

    def send_t_digest(self):
        """
        Simulated sending of the T-Digest to the SQS queue.
        """
        self.wait_randomly()
        return {"Response": "MockDigestSent"}

    def send_any_object(self, item_to_send):
        """
        Simulated sending of any given object to the SQS queue.
        """
        self.wait_randomly()
        return {"Response": "MockObjectSent"}

    def clear_queue_from_all_messages(self):
        """
        Simulated clearing of all messages from the SQS queue.
        """
        pass

    @staticmethod
    def wait_randomly():
        """
        Waits for 0.3 seconds with a small random deviation.
        """
        time.sleep(0.3 + random.uniform(-0.01, 0.01))
