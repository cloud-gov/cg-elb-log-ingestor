import json
import pathlib
import queue

import pytest

import elb_log_ingestor.elb_log_parse
import elb_log_ingestor.stats


@pytest.mark.parametrize("test_input,expected",
[
    # deeply-nested should be empty
    ({'foo': {'bar': { 'baz': { 'quuz': {}}}}}, {}),
    # mix empty and non-empty
    ({'foo': 1, 'bar': {'baz': {}}}, {'foo': 1}),
    # test some other falsey values
    ({'foo': False, 'bar': 0, 'baz': ""}, {'foo': False, 'bar': 0, 'baz': ""}),
])
def test_remove_empty_fields(test_input, expected):
    assert elb_log_ingestor.elb_log_parse.remove_empty_fields(test_input) == expected


@pytest.fixture(params=pathlib.Path(__file__).parent.glob("*.log"))
def log_file(request, tmp_path):
    """gather the logs and their expected files"""
    # determine the names
    logfile = request.param
    expected = logfile.with_name(logfile.stem + "-expected.json")
    return logfile, expected


def read_json_file(filename):
    """Reads a file containing one JSON object per line into a list of dicts"""
    with open(filename) as f:
        lines = [json.loads(line) for line in f.readlines()]
    return lines


def test_parse_logs(log_file):
    logfile, expected = log_file
    expected_contents =  read_json_file(expected)
    file_in_queue = ListQueue() 
    file_out_queue = ListQueue
    record_out_queue = ListQueue()
    stats_parser = elb_log_ingestor.stats.ParserStats()
    parser = elb_log_ingestor.elb_log_parse.LogParser(file_in_queue, file_out_queue, record_out_queue, stats_parser)
    with open(logfile) as f:
        strings = f.readlines()
    parser.parse_alb_logs(logfile.name, strings)
    contents = [x[1] for x in record_out_queue.list_]
    assert sorted(contents, key=lambda x: x['@raw']) == sorted(expected_contents, key=lambda x: x['@raw'])


class ListQueue:
    """
    add queue interface to a list
    """

    def __init__(self):
       self.list_ = []

    def empty(self):
        return bool(self.list_)
    
    def full(self):
        return False

    def put(self,item):
        self.list_.append(item)
    
    def get(self):
        return self.list.pop()
    
    def qsize(self):
        return len(self.list_)
