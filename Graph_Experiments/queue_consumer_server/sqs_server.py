import json
import boto3
from botocore.exceptions import ClientError
from tdigest import TDigest
import threading
import os
import redis


class SQSServer:
    """
    handles consumption of messages from the SQS - edge clients publish to SQS
    The server reads configurations from a file and continuously polls the queue.
    """

    def __init__(self, config_file='server_configs.json'):
        """
        Initialize the SQS server with configurations from the provided file.
        """
        self.configs = self.read_config(config_file)
        self.sqs = boto3.client('sqs', endpoint_url=self.configs["endpoint_url"])
        self.combined_digest = TDigest()
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

    def poll_messages(self):
        """
        Continuously poll the SQS queue for new messages and process them.
        """
        try:
            messages = self.sqs.receive_message(
                QueueUrl=self.configs['queue_url'],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            ).get('Messages', [])

            for message in messages:
                self.process_message(message)
                self.delete_message(message['ReceiptHandle'])

        except KeyboardInterrupt:
            print("Stopping the SQS server.")
        except ClientError as e:
            raise Exception(f"An error occurred: {e}")

    def process_message(self, message):
        """
        Process a single message from the queue.
        """
        # here it is possible to add logic for different monitors and conditions...
        with self.lock:
            # extract the dictionary that was sent and use it to update the digest.
            self.combined_digest.update_from_dict(json.loads(message['Body']))

    def delete_message(self, receipt_handle):
        """
        Delete a processed message from the queue.
        """
        self.sqs.delete_message(
            QueueUrl=self.configs['queue_url'],
            ReceiptHandle=receipt_handle
        )

    def print_combined_digest_dict(self):
        with self.lock:
            print(self.combined_digest.to_dict())


class RedisHandler:

    def __init__(self, digest_name="default_digest", restart_digest=True, batch_size=5000):
        self.redis_client = redis.Redis(host='localhost', port=32769, password='redispw', db=0)
        if restart_digest:
            self.restart_digest(digest_name)
        self.batch_size = batch_size

    def update_digest(self, new_digest_dict, digest_name="default_digest"):
        curr_digest_dict = json.loads(self.redis_client.get(digest_name))
        curr_digest = TDigest()
        curr_digest.update_from_dict(curr_digest_dict)
        curr_digest.update_from_dict(new_digest_dict)
        self.redis_client.set(digest_name, json.dumps(curr_digest.to_dict()))

    def update_digest_w_value(self, value, digest_name="default_digest"):
        curr_digest_dict = json.loads(self.redis_client.get(digest_name))
        curr_digest = TDigest()
        curr_digest.update_from_dict(curr_digest_dict)
        curr_digest.update(value)
        self.redis_client.set(digest_name, json.dumps(curr_digest.to_dict()))

    def update_digest_w_value_using_batching(self, value, reset_batch=False, digest_name="default_digest"):
        if reset_batch:
            self.redis_client.delete("batching_list")
        self.redis_client.lpush("batching_list", value)
        batch_len = self.redis_client.llen("batching_list")
        # batch size
        if batch_len == self.batch_size:
            batch = self.redis_client.lrange("batching_list",  0, -1)
            batch = [float(item) for item in batch]
            self.redis_client.delete("batching_list")
            curr_digest_dict = json.loads(self.redis_client.get(digest_name))
            curr_digest = TDigest()
            curr_digest.update_from_dict(curr_digest_dict)
            curr_digest.batch_update(batch)
            self.redis_client.set(digest_name, json.dumps(curr_digest.to_dict()))
            return curr_digest
        return None

    def get_t_digest(self, digest_name="default_digest"):
        return json.loads(self.redis_client.get(digest_name))

    def restart_digest(self, digest_name="default_digest"):
        init_digest = TDigest()
        self.redis_client.set(digest_name, json.dumps(init_digest.to_dict()))
