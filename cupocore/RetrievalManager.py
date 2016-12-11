import logging
import os
import threading
from math import ceil
import mongoops
import tempfile


class RetrievalManager():
    def __init__(self, db, client, vault_name):
        self.client = client
        self.db = db
        self.logger = logging.getLogger("cupobackup{0}.RetrievalManager".format(os.getpid()))
        self.vault_name = vault_name

        self.check_for_jobs = threading.Event()
        self.check_for_jobs.set()
        self.retrieval_thread = threading.Thread(target=self.thread_worker)

    def initiate_retrieval(self, archive_id, download_location):
        job_params = {
            "Format": "JSON",
            "Type": "archive-retrieval",
            "ArchiveID": archive_id
        }

        init_job_ret = self.client.initiate_job(vaultName=self.vault_name,
                                                jobParameters=job_params)

        if init_job_ret:
            mongoops.create_retrieval_entry(self.db,
                                            mongoops.get_vault_by_name(self.db, self.vault_name)["arn"],
                                            archive_id,
                                            init_job_ret["jobId"],
                                            init_job_ret["location"],
                                            download_location)

        if not self.check_for_jobs.isSet(): self.check_for_jobs.set()
        self.retrieval_thread.start()
        return True

    def check_job_status(self, job_id):
        response = self.client.describe_job(vaultName=self.vault_name,
                                            jobId=job_id)

        if response["Completed"] is False | response["StatusCode"] == "InProgress":
            # Still waiting for AWS to make the data available for download
            self.logger.info("Archive unavailable for download - AWS job in progress")
            self.logger.debug("AWS job description response:\n{0}".format(response))
            return False

        elif response["Completed"] is True & response["StatusCode"] == "Succeeded":
            # Job complete, data available for download
            self.logger.info("Job complete, archive available for download")
            return True

    def thread_worker(self):
        while self.check_for_jobs.isSet():
            self.logger.info("Getting new job to check")
            entry = mongoops.get_oldest_retrieval_entry(self.db, self.vault_name)

            if not entry:
                self.logger.info("No jobs available! Stopping trying to retrieve")
                self.check_for_jobs.clear()

            else:

                self.logger.info("Checking if job {0} is ready".format(entry["_id"]))

                status = self.check_job_status(entry["_id"])
                if not status:
                    logging.info("Job {0} is not ready. Waiting 1 minute".format(entry["_id"]))
                else:
                    logging.info("Job {0} is ready - commencing download".format(entry["_id"]))
                    self.download_archive(entry)

    def download_archive(self, job_entry):
        archive_entry = mongoops.get_archive_by_id(self.db, job_entry["archive_id"])
        tmp_dir = tempfile.mkdtemp()

        # Break the job up into chunks to make life easier
        chunk_files = []
        chunk_size = 2**16

        last_byte_downloaded = -1

        while last_byte_downloaded < archive_entry["size"]:
            byte_first = last_byte_downloaded + 1
            if (chunk_size + last_byte_downloaded) >= archive_entry["size"] - 1:
                byte_last = archive_entry["size"] - 1
            else:
                byte_last = chunk_size + last_byte_downloaded

            response = self.client.get_job_output(vaultName=self.vault_name,
                                                  jobId=job_entry["_id"],
                                                  range="bytes={0}-{1}".format(byte_first, byte_last))

            if response["status"] == 200 or response["status"] == 206:
                tmp_chunk_fd, tmp_chunk_path = tempfile.mkstemp(dir=tmp_dir)
                f = os.fdopen(tmp_chunk_fd, "wb")
                f.write(response["body"].read())
                try:
                    response["body"].close()
                except:
                    # May not need to be closed
                    pass
                f.close()
                chunk_files.append(tmp_chunk_path)
            else:
                self.logger.error(
                    "Getting job output for job {0} returned non-successful HTTP code: {1}".format(job_entry["_id"],
                                                                                                   response["status"]))

                # We should delete the retrieval job, now that we have the data
                mongoops.delete_retrieval_entry(self.db, job_entry["_id"])
                # Now that we have all of the files, join them together
                # TODO: add file joining and de-archive mechanism

        os.rmdir(tmp_dir)
