# cg-elb-log-ingestor

## what does it do?
This is an app to pull alb/elb logs from S3, parse them, and insert them into Elasticsearch

## why not use Logstash?
Logstash does not play well in our infra. Managing its config and fighting with upstream 
opinions took more work than writing our own single-purpose app

## How do I run it?
Coming soon

### reprocessing logs


## How does it work?

### Delivery model
We use an at-least-once model, then rely on using predictable ids in Elasticsearch to
deduplicate logs

### log fetcher
The log fetcher pulls logs from S3. When a log is downloaded, it moves it into a processing directory in the bucket.
When the log parser finishes parsing a file, the fetcher moves it from the processing directory to the processed directory.

### log parser
The log parser gets logs from the fetcher, then proceses them line-by-line into dictionaries. The dictionaries are then put
onto a queue for processing by the event uploader

### event uploader
The event uploader reads events from the queue it shares with the log parser. It takes events off the queue, 
adds a suitable, predicatble ID and then indexes them into Elasticsearch

### stats
The stats objects are thread-safe metrics reporters, allowing the other objects to report their metrics, so the stats
entpoint can expose them

### health
The health server is a simple webserver to expose stats and health information. It exposes the data in the stats object
for a metrics and monitoring system to read

## How do I work on it?

### running tests

```
$ python -m venv venv
$ . venv/bin/activate
$ python setup.py test
```
