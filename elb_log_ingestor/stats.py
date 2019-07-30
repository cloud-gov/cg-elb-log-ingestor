"""
Thread-safe stat tracker for parser
"""
import datetime
import threading
import typing


class ParserStats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self._lines_processed: int = 0
        self._lines_errored: int = 0
        self._files_processed: int = 0
        # None is boooorrring
        self._last_new_file_time: datetime.datetime = datetime.datetime.min

    @property
    def lines_processed(self) -> int:
        """
        The number of log lines the parser has successfully parsed
        """
        with self.lock:
            return self._lines_processed

    def increment_lines_processed(self) -> None:
        with self.lock:
            self._lines_processed += 1

    @property
    def lines_errored(self) -> int:
        """
        The number of log lines the parser has encountered errors on
        """
        with self.lock:
            return self._lines_errored

    def increment_lines_errored(self) -> None:
        with self.lock:
            self._lines_errored += 1

    @property
    def files_processed(self) -> int:
        """
        The number of files the parser has completed processing
        """
        with self.lock:
            return self._files_processed

    def increment_files_processed(self) -> None:
        with self.lock:
            self._files_processed += 1

    @property
    def last_new_file_time(self) -> datetime.datetime:
        """
        The last time a new file was downloaded
        """
        with self.lock:
            return self._last_new_file_time

    def new_file_time(self, new_time: datetime.datetime = None) -> None:
        if new_time is None:
            new_time = datetime.datetime.now()
        with self.lock:
            self._last_new_file_time = new_time

    @property
    def summary(self) -> typing.Dict:
        with self.lock:
            return dict(
                lines_processed=self._lines_processed,
                lines_errored=self._lines_errored,
                last_new_file_time=self._last_new_file_time,
            )


class ShipperStats:
    def __init__(self) -> None:
        self.lock: threading.Lock = threading.Lock()
        self._documents_indexed: int = 0
        self._documents_errored: int = 0
        self._duplicates_skipped: int = 0
        self._last_document_time: datetime.datetime = datetime.datetime.min

    @property
    def documents_indexed(self) -> int:
        """
        The number of documents the ElasticsearchShipper has successfully sent to elasticsearch
        """
        with self.lock():
            return self._documents_indexed

    def increment_documents_indexed(self) -> None:
        with self.lock:
            self._documents_indexed += 1

    @property
    def documents_errored(self) -> int:
        """
        The number of times the ElasticsearchShipper has attempted to index a document and failed,
        not including failures due to duplicate documents
        """
        with self.lock():
            return self._documents_errored

    def increment_documents_errored(self) -> None:
        with self.lock:
            self._documents_errored += 1

    @property
    def duplicates_skipped(self) -> int:
        """
        The number of times the ElasticsearchShipper has tried to index a document that already exists
        """
        with self.lock():
            return self._duplicates_skipped

    def increment_duplicates_skipped(self) -> None:
        with self.lock:
            self._duplicates_skipped += 1

    @property
    def last_document_time(self) -> datetime.datetime:
        """
        The last time the ElasticsearchShipper indexed a document
        """
        with self.lock:
            return self._last_document_time

    def document_time(self, new_time: datetime.datetime = None) -> None:
        if new_time is None:
            new_time = datetime.datetime.now()
        with self.lock:
            self._last_document_time = new_time

    @property
    def summary(self) -> typing.Dict:
        with self.lock:
            return dict(
                documents_indexed=self._documents_indexed,
                documents_errored=self._documents_errored,
                duplicates_skipped=self._duplicates_skipped,
                last_document_indexed_at=self._last_document_time,
            )
