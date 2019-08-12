import http.server
import os
import pathlib
import queue
import sys
import threading

import boto3
import elasticsearch

from . import api_endpoint
from . import elasticsearch_shipper
from . import elb_log_fetcher
from . import elb_log_parse
from . import stats


def start_server():
    parser_stats = stats.ParserStats()
    shipper_stats = stats.ShipperStats()

    elasticsearch_hosts = os.environ["ELB_INGESTOR_ELASTICSEARCH_HOSTS"]
    elasticsearch_hosts = elasticsearch_hosts.split(",")
    es_client = elasticsearch.Elasticsearch(
        elasticsearch_hosts, sniff_on_start=True, sniffer_timeout=60
    )
    s3_client = boto3.resource("s3")
    server_address = get_server_address()

    logs_to_be_processed = queue.Queue()
    logs_processed = queue.Queue()
    records = queue.Queue()
    bucket_name = os.environ["ELB_INGESTOR_BUCKET"]
    bucket = s3_client.Bucket(bucket_name)
    unprocessed_prefix = os.environ.get("ELB_INGESTOR_SEARCH_PREFIX", "logs/")
    processing_prefix = os.environ.get("ELB_INGESTOR_WORKING_PREFIX", "logs-working/")
    processed_prefix = os.environ.get("ELB_INGESTOR_DONE_PREFIX", "logs-done/")
    start_queue_size = int(os.environ.get("ELB_INGESTOR_START_QUEUE_SIZE", 5))
    index_pattern = os.environ.get("ELB_INDEX_PATTERN", "logs-platform-%Y.%m.%d")
    fetcher = elb_log_fetcher.S3LogFetcher(
        bucket,
        unprocessed_prefix=unprocessed_prefix,
        processing_prefix=processing_prefix,
        processed_prefix=processed_prefix,
        to_do=logs_to_be_processed,
        done=logs_processed,
        start_queue_size=start_queue_size,
    )

    parser = elb_log_parse.LogParser(
        logs_to_be_processed, logs_processed, records, parser_stats
    )
    shipper = elasticsearch_shipper.ElasticsearchShipper(
        es_client, records, index_pattern, shipper_stats
    )

    # prepare the ApiEndpoint class for use
    api_endpoint.ApiEndpoint.parser_stats = parser_stats 
    api_endpoint.ApiEndpoint.shipper_stats = shipper_stats
    api_endpoint.ApiEndpoint.fetcher = fetcher
    api_endpoint.ApiEndpoint.shipper = shipper

    server = http.server.HTTPServer(server_address, api_endpoint.ApiEndpoint)

    fetcher_thread = threading.Thread(target=fetcher.run)
    parser_thread = threading.Thread(target=parser.run, daemon=True)
    shipper_thread = threading.Thread(target=shipper.run, daemon=True)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)

    fetcher_thread.start()
    parser_thread.start()
    shipper_thread.start()
    server_thread.start()


def get_server_address() -> (str, int):
    listen_host = os.environ.get("ELB_INGESTOR_LISTEN_HOST", "localhost")
    if listen_host == "0.0.0.0":
        listen_host = ""
    listen_port = int(os.environ.get("ELB_INGESTOR_LISTEN_PORT", 13131))
    return listen_host, listen_port


if __name__ == "__main__":
    start_server()
