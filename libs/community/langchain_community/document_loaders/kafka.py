from typing import Any, Callable, Iterator, Mapping, Optional
from langchain_core.documents import Document
from confluent_kafka import Consumer, KafkaException, KafkaError
from langchain_community.document_loaders.base import BaseLoader

RecordHandler = Callable[[Any, Optional[str]], Document]


class KafkaDocumentLoader(BaseLoader):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = 'latest',
        record_handler: Optional[RecordHandler] = None,
        **kwargs: Any,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.record_handler = record_handler
        self.consumer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            **kwargs,
        }
        self.consumer = None

    def _create_consumer(self):
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([self.topic])

    def _handle_record(self, record: Any, id: Optional[str]) -> Document:
        if self.record_handler:
            return self.record_handler(record, id)
        return Document(page_content=record.value().decode('utf-8'), metadata={'offset': record.offset(), 'timestamp': record.timestamp()})

    def lazy_load(self) -> Iterator[Document]:
        if not self.consumer:
            self._create_consumer()

        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise Exception(f"Consumer error: {msg.error()}")
            yield self._handle_record(msg, str(msg.offset()))

    def load(self) -> list[Document]:
        return list(self.lazy_load())

    def close(self):
        if self.consumer:
            self.consumer.close()