import threading
import random
import time
from edge_client.client import EdgeClient
from queue_consumer_server.sqs_server import SQSServer, RedisHandler


# configurations  for localstack run:
# 1. localstack start
# 2. aws --endpoint=http://localhost:4566 sqs create-queue --queue-name local-queue
# 3. copy queue url to both client and server configs
# 4. Run Redis and add the credentials to RedisHandler code


def run_client(client: EdgeClient):
    for _ in range(100):
        random_values = [random.randint(1, 100) for _ in range(10)]  # Generate 10 random numbers
        client.update_digest_with_vals(random_values)
        client.send_t_digest()
        print("client finished sampling random 100 and sending to server")
        time.sleep(1)


# Function for running the server
def run_server(server: SQSServer):
    while True:
        server.poll_messages()
        server.print_combined_digest_dict()


# Create server and 3 client instances
server = SQSServer()
clients = [EdgeClient() for _ in range(3)]

# Start server thread
server_thread = threading.Thread(target=run_server, args=(server,))
server_thread.start()

time.sleep(2)

# Start client threads
client_threads = []
for client in clients:
    thread = threading.Thread(target=run_client, args=(client,))
    client_threads.append(thread)
    thread.start()

