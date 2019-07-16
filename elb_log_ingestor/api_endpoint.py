import json

from http.server import BaseHTTPRequestHandler


class ApiEndpoint(BaseHTTPRequestHandler):
    def __init__(self, parser_stats, shipper_stats, shipper, fetcher, **kwargs) -> None:
        self.parser_stats = parser_stats
        self.shipper_stats = shipper_stats
        self.shipper = shipper
        self.fetcher = fetcher
        super().__init__(**kwargs)

    def do_GET(self) -> None:
        if self.path == "/stats":
            self.send_stats()
        elif self.path == "/health":
            self.send_health()
        else:
            self.send_error(404)
            self.end_headers()

    def send_stats(self) -> None:
        parser = self.parser_stats.summary
        shipper = self.shipper_stats.summary
        parser["last_new_file_time"] = str(parser["last_new_file_time"])
        shipper["last_document_indexed_at"] = str(shipper["last_document_indexed_at"])
        stats = dict(parser=parser, shipper=shipper)
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(json.dumps(stats)))

    def send_health(self) -> None:
        response = dict()
        response["elasticsearch_connected"] = self.shipper.healthy
        response["s3_connected"] = self.fetcher.healthy
        if response["elasticsearch_connected"] and response["s3_connected"]:
            response["status"] = "UP"
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps(response)))
        else:
            response["status"] = "DOWN"
            self.send_error(500, explain=bytes(json.dumps(response)))
            self.end_headers()
