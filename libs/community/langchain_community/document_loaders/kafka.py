from typing import Any, Callable, Iterator, Mapping, Optional
from langchain_core.documents import Document
from confluent_kafka import Consumer, KafkaException, KafkaError
from langchain_community.document_loaders.base import BaseLoader

def error_cb(err):
    print("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or err.code() == KafkaError._AUTHENTICATION:
        raise KafkaException(err)

class KafkaDocumentLoader(BaseLoader):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = 'latest',
        security_protocol: Optional[str] = None,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password

        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'error_cb': error_cb,
            **kwargs,
        }

        if self.security_protocol:
            self.consumer_config['security.protocol'] = self.security_protocol

        if self.sasl_mechanism:
            self.consumer_config['sasl.mechanism'] = self.sasl_mechanism
            self.consumer_config['sasl.username'] = self.sasl_username
            self.consumer_config['sasl.password'] = self.sasl_password

        self.consumer = None

    def _create_consumer(self):
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([self.topic])

    def _handle_record(self, record: Any, id: Optional[str]) -> Document:
        return Document(page_content=record.value().decode('utf-8'))

    def lazy_load(self) -> Iterator[Document]:
        if not self.consumer:
            self._create_consumer()
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._ALL_BROKERS_DOWN or msg.error().code() == KafkaError._AUTHENTICATION:
                    raise KafkaException(msg.error())
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue
            document = self._handle_record(msg, str(msg.offset()))
            yield document

    def load(self) -> list[Document]:
        return list(self.lazy_load())

    def close(self):
        if self.consumer:
            self.consumer.close()