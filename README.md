# Edge_ML_KS-Test: Distributed KS Test for Edge ML

This is a prototype for the software system explained in the paper:
*Distributed Monitoring for Data Distribution Shifts in Edge-ML Fraud Detection* 
feel free to use this code for inspiration/Implement the monitoring in your environment. 

## Overview
This code contains an implementation of the Kolmogorov-Smirnov (KS) Test, designed explicitly for distributed edge computing environments (Edge-ML for instance). Our design ensures efficiency for both the distributed clients and the aggregating server.

![Page Twosystem diagram (6) (2)](https://github.com/karayanni/Edge_ML_KS-Test/assets/59873152/624c06ba-faac-41b0-ab74-32c1a0313519)


## Getting Started

### Prerequisites
Before running the framework, ensure you have the following prerequisites:

- Python 3.x
- Access to AWS SQS or a local stack setup for it
- Access to AWS lambda or a local stack setup for it (or just use the sqs_server as I did in the mock e2e simulation)
- Redis Instance

### Installation
1. **Install Dependencies:** Refer to `requirements.txt` for the necessary Python packages and install them using `pip install -r requirements.txt`.

2. **Setup cloud/local environment:** The queue, the lambda, and the Redis instance

### Configuration
1. **Configure AWS SQS/Local Stack:** Update the client configuration `framework/edge_client/client_configs.json`
 and server configuration `framework/queue_consumer_server/server_configs.json` files with the details of your SQS setup.
2. **Configure Redis:** Update the Redis client configs in the `SQSServer` code.

### Execution
1. **Run the mock Simulation:** Execute `application_e2e_simulation.py` to start an end-to-end mock of the system.
2. Edit the code to match your needs and environment...

