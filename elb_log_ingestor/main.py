import http.server
import os
import pathlib
import queue
import sys
import tempdir
import threading

import boto
import elasticsearch

import api_endpoint
import elasticsearch_shipper
import elb_log_fetcher
import elb_log_parse
import stats


def start_server():
    parser_stats = stats.ParserStats()
    shipper_stats = stats.ShipperStats()

    es_client = elasticsearch.Elasticsearch()
    s3_client = boto.client("s3")
    workdir = get_workdir()
    server_address = get_server_address()

    logs_to_be_processed = queue.Queue()
    logs_processed = queue.Queue()
    records = queue.Queue()
    unprocessed_prefix = os.environ.get("ELB_INGESTOR_SEARCH_PREFIX", "logs/")
    processing_prefix = os.environ.get("ELB_INGESTOR_WORKING_PREFIX", "logs-working/")
    processed_prefix = os.environ.get("ELB_INGESTOR_DONE_PREFIX", "logs-done/")
    start_queue_size = int(os.environ.get("ELB_START_QUEUE_SIZE", 5))
    fetcher = elb_log_fetcher.S3LogFetcher(
        bucket,
        s3_client,
        unprocessed_prefix=unprocessed_prefix,
        processing_prefix=processing_prefix,
        processed_prefix=processed_prefix,
        workdir=workdir,
        to_do=logs_to_be_processed,
        done=logs_processed,
        start_queue_size=start_queue_size,
    )

    parser = elb_log_parse.LogParser(
        logs_to_be_processed, logs_processed, parser_stats, workdir
    )
    shipper = elasticsearch_shipper.ElasticsearchShipper(
        es_client, records, index_pattern, shipper_stats
    )
    api_endpoint = api_endpoint.ApiEndpoint(parser_stats, shipper, shipper, fetcher)
    server = http.server.HTTPServer(server_address, api_endpoint)

    fetcher_thread = threading.Thread(target=fetcher.run)
    parser_thread = threading.Thread(target=parser.run, daemon=True)
    shipper_thread = threading.Thread(target=shipper.run, daemon=True)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)

    fetcher_thread.start()
    parser_thread.start()
    shipper_thread.start()
    server_thread.start()


def get_workdir() -> pathlib.Path:
    workdir = os.environ.get("ELB_INGESTOR_WORKDIR")
    if workdir is None:
        workdir = tempfile.mkdtemp()
    return pathlib.Path(workdir)


def get_server_address() -> (str, int):
    listen_host = os.environ.get("ELB_INGESTOR_LISTEN_HOST", "localhost")
    if listen_host == "0.0.0.0":
        listen_host = ""
    listen_port = os.environ.get("ELB_INGESTOR_LISTEN_PORT", 13131)
    return listen_host, listen_port
