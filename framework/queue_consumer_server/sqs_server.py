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
    The logic in this class should be deployed to a Lambda subscribed tot the SQS if you want the serverless architecture.
    """

    def __init__(self, config_file='server_configs.json', aggregation_size=500000, alerting_threshold=0.05):
        """
        Initialize the SQS server with configurations from the provided file.
        """
        self.configs = self.read_config(config_file)
        self.sqs = boto3.client('sqs', endpoint_url=self.configs["endpoint_url"])
        self.redis_client = redis.Redis(host='localhost', port=32768, password='redispw', db=0)
        self.aggregation_size = aggregation_size
        self.combined_digest = TDigest()

        # todo read ref data from a file uploaded with the lambda
        self.ref_data = [1, 2, 3]
        self.ref_digest = TDigest()
        self.ref_digest.batch_update(self.ref_data)

        # the default users group with a batch size (d=20000)
        self.redis_handler = RedisHandler(digest_name="default_digest", restart_digest=True, batch_size=20000)
        self.lock = threading.Lock()
        self.alerting_KS_threshold = alerting_threshold

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
            # merge the T-Digests
            self.redis_handler.update_digest(json.loads(message['Body']))

            # add the counter by d.
            new_count = self.redis_client.incr("default_digest_counter", 20000)
            print(f"server current aggregation count: {new_count}")
            if new_count >= self.aggregation_size or True:
                test_digest = self.redis_handler.get_t_digest()

                max_cdf = 0
                for item in self.ref_data:
                    curr = abs(self.ref_digest.cdf(item) - test_digest.cdf(item))
                    if curr > max_cdf:
                        max_cdf = curr

                if max_cdf > self.alerting_KS_threshold:
                    # TODO: add preferred alerting method here...
                    print(f"DATA DRIFT DETECTED... KS value: {max_cdf}")

                self.redis_handler.restart_digest()
                self.redis_client.set("default_digest_counter", 0)

            # extract the dictionary that was sent and use it to update the digest.
            # self.combined_digest.update_from_dict(json.loads(message['Body']))

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
        # update redis credentials here...
        self.redis_client = redis.Redis(host='localhost', port=32768, password='redispw', db=0)
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
            batch = self.redis_client.lrange("batching_list", 0, -1)
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
        return TDigest(json.loads(self.redis_client.get(digest_name)))

    def restart_digest(self, digest_name="default_digest"):
        init_digest = TDigest()
        self.redis_client.set(digest_name, json.dumps(init_digest.to_dict()))
