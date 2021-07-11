"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple

import socket, selectors, sys, json, pickle
import xml.etree.ElementTree as ET

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self.sel = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self._host, self._port))
        self.sock.listen(100)
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        self.sockets = {} # {connection : serialization}
        self.subscriptions = {} # { (connection, serialization) : [topic1,topic2,...] }
        self.topics = {} # {topic : [value1, value2]}


    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        return [k for k in self.topics]

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        for t in self.topics: # o t um topico anterior, o topic é um subtopico do t
            if topic.startswith(t): # o caminho grande (arvore) contiver  o pequeno (topico pai)
                if len(self.topics[topic]) > 0: # se a lista de valores nao estiver vazia retorna o valor
                    return self.topics[topic][-1] # se nao continua a iterar
        return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        if topic not in self.topics:
            self.topics[topic] = [value] # {topic : [value1, value2]}
        else:
            self.topics[topic].append(value)


    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        return [k for k,v in self.subscriptions.items() if topic in v]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""

        if (address, _format) not in self.subscriptions: # { (connection, serialization) : [topic1,topic2,...] }
            self.subscriptions[(address, _format)] = [topic]
        elif topic not in self.subscriptions[(address, _format)]:
            self.subscriptions[(address, _format)].append(topic)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for key in self.subscriptions:
            if address in key and topic in self.subscriptions[key]:
                self.subscriptions[key].remove(topic)

    def format_protocol(self, method, topic, value, serialization):
        if serialization == "xml":
            if topic is None:
                return "<data><method>"+str(method)+"</method><value>"+str(value)+"</value></data>"
            else:
                return "<data><method>"+str(method)+"</method><topic>"+str(topic)+"</topic><value>"+str(value)+"</value></data>"
        else:
            if topic is None:
                return {"method" : method, "value" : value}
            else:
                return {"method" : method, "topic" : topic, "value" : value}

    def encode(self, data, serialization):
        if serialization == "json":
            return (json.dumps(data)).encode("utf-8")
        elif serialization == "xml":
            return data.encode("utf-8")
        elif serialization == "pickle":
            return pickle.dumps(data)

    def decode(self, data, serialization):
        if serialization == "json":
            return json.loads(data.decode("utf-8"))
        elif serialization == "xml":
            tree = ET.ElementTree(ET.fromstring(data.decode("utf-8")))    
            data = {}
            
            for el in tree.iter():
                data[el.tag] = el.text

            return data

        elif serialization == "pickle":
            return pickle.loads(data)
            
    def accept(self, sock, mask):
        conn, addr = sock.accept()
        self.sel.register(conn, selectors.EVENT_READ, self.read)  
    
    def read(self, conn, mask):
        header = conn.recv(2) # get first two bytes to know how many bytes to get after
        header = int.from_bytes(header, "big")
        body = conn.recv(header) # return the exact bytes needed

        if len(body) != 0:
            if conn in self.sockets:
                serialization = self.sockets[conn]
            else:
                serialization = "json"
            
            data = self.decode(body, serialization)
        else:
            data = None
            
        if data != None:
            if (conn not in self.sockets):
                self.sockets[conn] = data["value"]
            if data["method"] == "publish":
                self.put_topic(data["topic"], data["value"])
                for key, topics in self.subscriptions.items():
                    connection = key[0]
                    for t in topics:  # verificar para cada topico na lista de topicos
                        if data["topic"].startswith(t):
                            try:
                                reply = self.format_protocol("reply", data["topic"], data["value"], self.sockets[connection])
                                reply = self.encode(reply, self.sockets[connection])
                                connection.send(len(reply).to_bytes(2, 'big') + reply)
                            except:
                                pass # socket was closed during iteration
            
            elif data["method"] == "topics":
                reply = self.format_protocol("reply", None, str(self.list_topics()), self.sockets[conn])
                reply = self.encode(reply, self.sockets[conn])
                conn.send(len(reply).to_bytes(2, 'big') + reply)
            
            elif data["method"] == "subscribe":
                self.subscribe(data["topic"], conn)
                last_topic = self.get_topic(data["topic"])

                if last_topic != None:
                    reply = self.format_protocol("reply", data["topic"], last_topic, self.sockets[conn])
                    reply = self.encode(reply, self.sockets[conn])
                    conn.send(len(reply).to_bytes(2, 'big') + reply)
            
            elif data["method"] == "cancel":
                self.unsubscribe(data["topic"], conn)
        else:
            #self.sockets.pop(conn)
            self.sel.unregister(conn)
            conn.close()

        # ler mensagem(receber o tamanho certo de bytes), descodificar mensagem  
        # verificar se é xml, json ou pickle e ver o que a mensagem contem  
        # analisar o tipo da mensagem, subscribe, unsubscribe    

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            try:
                for key, mask in self.sel.select():
                    callback = key.data
                    callback(key.fileobj, mask) # usar selectors para ficar à espera de sockets e correr as funcoes
                                        # e fazer send do return das funcoes para a socket
                                        # Antes de chegar aqui, ja passou tudo pelo middleware
                                        # é o middleware que envia a socket para aqui para o broker
            except KeyboardInterrupt:
                sys.exit(0)

            except socket.error:
                sys.exit(0)    
