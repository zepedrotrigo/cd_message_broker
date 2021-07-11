"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any

import socket, json, pickle
import xml.etree.ElementTree as ET

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self._type = _type
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(('localhost', 5000))

    def encode(self, data): # redefined below in specific queues
        pass
    
    def decode(self, data): # redefined below in specific queues
        pass

    def format_protocol(self, method, topic, value): # message syntax, it is the same for json and pickle but different for xml
        pass

    def push(self, value): # usado pelo produtor
        """Sends data to broker. """
        msg = self.format_protocol("publish", self.topic, value)
        msg = self.encode(msg)
        self.sock.send(len(msg).to_bytes(2, 'big') + msg)

    def pull(self) -> (str, tuple): # usado pelo consumer
        """Receives (topic, data) from broker.
        recv_msg(self.sock)

        Should BLOCK the consumer!"""
        header = self.sock.recv(2) # get first two bytes to know how many bytes to get after
        header = int.from_bytes(header, "big")
        body = self.sock.recv(header) # return the exact bytes needed

        if len(body) != 0:
            data = self.decode(body)
            return data["topic"], data["value"] # enviou uma mensagem do publisher de um topico ao qual o cliente esta subscrito

    def list_topics(self, callback: Callable): # callback é um arg do tipo funcao, essa funcao vai
        # o callback vai ser usado pelo pool para
        """Lists all topics available in the broker."""
        msg = self.format_protocol("topics")
        msg = self.encode(msg)
        self.sock.send(len(msg).to_bytes(2, 'big') + msg) # ask broker for list

    def cancel(self):
        """Cancel subscription."""
        msg = self.format_protocol("cancel", str(self.topic))
        msg = self.encode(msg)
        self.sock.send(len(msg).to_bytes(2, 'big') + msg)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        msg = '{"method" : "serialization", "value" : "json"}'
        self.sock.send(len(msg).to_bytes(2, 'big') + bytes(msg,"utf-8"))

        if self._type == MiddlewareType.CONSUMER:
            msg = self.format_protocol("subscribe", str(self.topic))
            msg = self.encode(msg)
            self.sock.send(len(msg).to_bytes(2, 'big') + msg)
    
    def encode(self, data):
        return (json.dumps(data)).encode("utf-8")

    def decode(self, data):
        return json.loads(data.decode("utf-8"))

    def format_protocol(self, method, topic=None, value=None):
        if (topic and value) is not None:
            return {"method" : method, "topic" : topic, "value" : value}
        elif topic is not None:
            return {"method" : method, "topic" : topic}
        elif value is not None:
            return {"method" : method, "value" : value}
        else:
            return {"method" : method}
        
class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        msg = '{"method" : "serialization", "value" : "xml"}'
        self.sock.send(len(msg).to_bytes(2, 'big') + bytes(msg,"utf-8"))

        if self._type == MiddlewareType.CONSUMER:
            msg = self.format_protocol("subscribe", str(self.topic))
            msg = self.encode(msg)
            self.sock.send(len(msg).to_bytes(2, 'big') + msg)

    def encode(self, data):
        return data.encode("utf-8")

    def decode(self, data):
        tree = ET.ElementTree(ET.fromstring(data.decode("utf-8")))           
        data = {}
        
        for el in tree.iter():
            data[el.tag] = el.text

        return data

    def format_protocol(self, method, topic=None, value=None):
        if (topic and value) is not None:
            return "<data><method>"+str(method)+"</method><topic>"+str(topic)+"</topic><value>"+str(value)+"</value></data>"
        elif topic is not None:
            return "<data><method>"+str(method)+"</method><topic>"+str(topic)+"</topic></data>"
        elif value is not None:
            return "<data><method>"+str(method)+"</method><value>"+str(value)+"</value></data>"
        else:
            return "<data><method>"+str(method)+"</method></data>"

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        msg = '{"method" : "serialization", "value" : "pickle"}'
        self.sock.send(len(msg).to_bytes(2, 'big') + bytes(msg,"utf-8"))

        if self._type == MiddlewareType.CONSUMER:
            msg = self.format_protocol("subscribe", str(self.topic))
            msg = self.encode(msg)
            self.sock.send(len(msg).to_bytes(2, 'big') + msg)
    
    def encode(self, data):
        return pickle.dumps(data)

    def decode(self, data):
        return pickle.loads(data)

    def format_protocol(self, method, topic=None, value=None):
        if (topic and value) is not None:
            return {"method" : method, "topic" : topic, "value" : value}
        elif topic is not None:
            return {"method" : method, "topic" : topic}
        elif value is not None:
            return {"method" : method, "value" : value}
        else:
            return {"method" : method}
