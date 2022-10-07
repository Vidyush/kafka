#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "/Users/OneDrive/Desktop/Kafka/restaurant.csv"
columns=['order_number', 'order_date', 'item_name', 'quantity', 'product_price', 'total_products']

API_KEY = 'K4CJ7AYHJ2VJXHZ7'
ENDPOINT_SCHEMA_URL  = 'https://psrc-7qgn2.us-central1.gcp.confluent.cloud'
API_SECRET_KEY = 'L/IEsxRC+acG162kFuLe3A9mtCQpXXkr3iH4akWMghtCDG329V4LFqVU9jrXmzD3'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'LAEPYXP5JAIIWAOO'
SCHEMA_REGISTRY_API_SECRET = 'a14XZ4okFTDWfirD3IaXOSTpnnN2dhLaQCJqzCBqN2RtE1VpqxkG2TqFdzfznuF7'
schema_id = 100011

def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_rest(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def get_car_instance(file_path):
    #df=pd.read_csv(file_path)
    #df=df.iloc[:,1:]
    df=pd.read_csv(file_path,names=columns,header=0)
    orders:List[Order]=[]
    for data in df.values:
        order=Order(dict(zip(columns,data)))
        orders.append(order)
        yield order

def order_to_dict(order:Order, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return order.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str1 = schema_registry_client.get_schema(schema_id).schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str1, schema_registry_client, order_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    count=0
    producer.poll(0.0)
    try:
        for order in get_car_instance(file_path=FILE_PATH):
            count=count+1
            print(order)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), order_to_dict),
                            value=json_serializer(order, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            # break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass
    print(count)
    print("\nFlushing records...")
    producer.flush()

main("restaurant_topic")
