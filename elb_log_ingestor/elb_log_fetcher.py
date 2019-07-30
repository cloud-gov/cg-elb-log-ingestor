"""
Retrieves ELB log files
"""
import io
import logging
import pathlib
import queue

logger = logging.Logger(__self__)


class S3LogFetcher:
    """
    Fetches logs from S3, moves logs in S3 around to indicate they're processing/processed
    """

    def __init__(
        self,
        bucket: str,
        s3_client: "boto3.resources.factory.s3.ServiceResource",
        unprocessed_prefix: str,
        processing_prefix: str,
        processed_prefix: str,
        to_do: queue.Queue,
        done: queue.Queue,
        start_queue_size: int = 5,
    ) -> None:
        """
        bucket: the name of the bucket
        s3_client: a boto s3 client
        unprocessed_prefix: the prefix in the bucket to look for new logs
        processing_prefix: the prefix in the bucket to put/find processing logs
        processed_prefix: the prefix in the bucket to put processed logs
        to_do: the queue to send work to the log parser
        done: the queue to listen on for finished work
        start_queue_size: how many log files to pull down on startup
        """
        self.bucket = bucket
        self.botoclient = s3_client
        self.unprocessed_prefix = unprocessed_prefix
        self.processing_prefix = processing_prefix
        self.processed_prefix = processed_prefix
        self.to_do = to_do
        self.done = done
        self.start_queue_size = start_queue_size
        self.healthy = True

    def run(self) -> None:
        """
        Do the work:
        - first, prime the queue with some logs
        - then, poll to see when the logs are done
        - if we can get a log off the done queue, mark it as done
        - if we get a log off the done queue, get another one for the to_do queue
        """
        for i in range(self.start_queue_size):
            next_log = self.get_next_log()
            if next_log is not None:
                self.to_do.put(next_log)
        while True:
            finished_log = self.done.get()
            try:
                self.mark_log_processed(finished_log)
            except Exception as e:
                # if it fails:
                #   - log it
                #   - put it back on the queue, so we can retry
                #   - mark ourselves unhealthy
                logger.error(e)
                self.done.put(finished_log)
                self.healthy = False
            else:
                self.healthy = True
            next_log = self.get_next_log()
            if next_log is not None:
                self.to_do.put(next_log)

    def get_next_log(self) -> str:
        """
        Download one log from S3, mark it as processing, and return its name.
        If there are no logs to get, return None.
        """
        try:
            boto_reponse = self.botoclient.list_objects_v2(
                Bucket=self.bucket, MaxKeys=1, Prefix=self.unprocessed_prefix
            )
        except Exception:
            # ignore it and try again later - hopefully someone's checking health
            logger.exception('Failed listing logs in S3')
            self.healthy = False
            return None
        else:
            self.healthy = True
        if len(boto_reponse["Contents"]) == 0:
            return None
        next_object = boto_reponse["Contents"][0]["Key"]
        processing_name = self.mark_log_processing(next_object)
        contents = io.BytesIO()
        self.botoclient.Bucket(self.bucket).download_fileobj(processing_name, contents)
        contents.seek(0)
        strings = [line.decode('utf-8') for line in contents.readlines()]
        return processing_name, strings

    def mark_log_processed(self, logname: str) -> None:
        """
        Move a logfile from the processing to the processed prefix.
        """
        processed_name = self.processed_name_from_processing_name(logname)
        self.move_object(from_=logname, to=processed_name)
        return processed_name

    def mark_log_processing(self, logname: str) -> None:
        """
        Move a logfile from the unprocessed to the processing prefix.
        """
        # Note - this is one of the places where a race condition can cause
        # us to process a file more than once
        processing_name = self.processing_name_from_unprocessed_name(logname)
        self.move_object(from_=logname, to=processing_name)
        return processed_name

    def move_object(self, from_, to) -> None:
        """
        Move/rename an object within this bucket.
        """
        # boto doesn't have a move operation, so we need to copy then delete
        bucket = self.botoclient.Bucket(self.bucket)
        bucket.copy(dict(Bucket=self.bucket, Key=from_), to)
        delete_request = {"Objects": [{"Key": from_}], "Quiet": True}
        bucket.delete_objects(Delete=delete_request)

    def processing_name_from_unprocessed_name(self, unprocessed_name: str) -> str:
        """
        Determine the processing name from an unprocessed name
        """
        return replace_prefix(
            unprocessed_name, self.unprocessed_prefix, self.processed_prefix
        )

    def processed_name_from_processing_name(self, processing_name: str) -> str:
        """
        Determine the processed name from an unprocessed name
        """
        return replace_prefix(
            processing_name, self.processing_prefix, self.processed_prefix
        )


def replace_prefix(logname: str, old_prefix: str, new_prefix: str) -> str:
    if not logname.startswith(old_prefix):
        raise ValueError
    return logname.replace(old_prefix, new_prefix, 1)
