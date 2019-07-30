"""
Sends messages to Elasticsearch
"""
import datetime
import logging
import queue
import typing

import elasticsearch

import stats


logger = logging.Logger(__name__)
HTTP_CONFLICT = 409


class ElasticsearchShipper:
    def __init__(
        self,
        elasticsearch_client: elasticsearch.client,
        record_queue: queue.Queue,
        index_pattern: str,
        stats: stats.ShipperStats,
    ) -> None:
        self.es = elasticsearch_client
        self.record_queue = record_queue
        self.index_pattern = index_pattern
        self.stats = stats

    def run(self) -> None:
        while True:
            record = self.record_queue.get()
            self.index_record(*record)

    def index_record(self, id_: str, record: typing.Dict) -> None:
        index = self.figure_index(record)
        try:
            self.es.create(index=index, id=id_, body=record)
        except elasticsearch.ConflictError:
            self.stats.increment_duplicates_skipped()
            logger.info("Skipping duplicate document with id %s", id_)
        except e:
            # if it failed for an unknown reason, log it and put it back on the queue so we can try again
            self.stats.increment_documents_errored()
            logger.error(e)
            self.record_queue.put((id_, record))
        else:
            self.stats.increment_documents_indexed()
            logger.debug("Indexing document with id %s", id_)

    def figure_index(self, record: typing.Dict) -> str:
        ts = datetime.datetime.fromisoformat(record["@timestamp"])
        return ts.strftime(self.index_pattern)

    @property
    def healthy(self) -> bool:
        healthy = False
        try:
            healthy = self.es.ping()
        except:
            healthy = False
        return healthy