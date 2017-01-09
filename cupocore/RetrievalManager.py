import logging
import os
import threading
import tempfile
from idlelib import IOBinding

import mongoops
import botocore.utils, botocore.exceptions
import subprocess
import zipfile
from time import sleep


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
        # Check whether or not we should be checking for jobs
        while self.check_for_jobs.isSet():
            self.logger.info("Getting new job to check")

            # Start with the first entry
            job_entry = mongoops.get_oldest_retrieval_entry(self.db, self.vault_name)

            if not job_entry:
                self.logger.info("No jobs available! Stopping trying to retrieve")
                self.check_for_jobs.clear()
                return

            # TODO: If job was last checked less than an hour ago, wait for the rest of the hour

            self.logger.info("Checking if job {0} is ready".format(job_entry["_id"]))

            # Is the data available to retrieve from AWS?
            status = self.check_job_status(job_entry["_id"])
            if not status:
                self.logger.info("Job {0} is not ready.".format(job_entry["_id"]))
                mongoops.update_job_last_polled_time(self.db, job_entry["_id"])
                sleep(2)
                continue

            self.logger.info("Job {0} is ready - commencing download".format(job_entry["_id"]))

            # Download the archive from AWS
            local_arch_fullpath = self.download_archive(job_entry)
            if local_arch_fullpath:
                # And unzip it
                self.dearchive_file(local_arch_fullpath, job_entry["job_retrieval_destination"])

                # Delete the zip file
                self.logger.info("Removing archive {0}".format(local_arch_fullpath))
                os.remove(local_arch_fullpath)

    def download_archive(self, job_entry):
        """Downloads the archive associated with `job_entry`.

         Retrieves, in 16MB chunks, the AWS archive associated with the
         database entry `job_entry`, before concatenating the chunks into one
         archive and verifying the archive integrity by calcuating the treehash
         of the archive and comparing it against the treehash of the originally
         uploaded archive.

         Args:
             job_entry: The MongoDB entry describing the retrieval job currently
                being operated on.

        Returns:
            If download, concatenation and verification are successful, return
            the absolute path to the downloaded archive. Otherwise, return
            None.

        Raises:
            botocore.exceptions.ChecksumError: The downloaded archives'
                checksum did not match that of the originally uploaded archive;
                verification failed.
        """
        archive_entry = mongoops.get_archive_by_id(self.db, job_entry["archive_id"])
        tmp_dir = tempfile.mkdtemp()

        # Break the job up into chunks to make life easier
        chunk_files = []
        chunk_size = 16777216

        last_byte_downloaded = -1

        while last_byte_downloaded < archive_entry["size"]:
            byte_first = last_byte_downloaded + 1
            if (chunk_size + last_byte_downloaded) >= archive_entry["size"] - 1:
                byte_last = archive_entry["size"] - 1
            else:
                byte_last = chunk_size + last_byte_downloaded

            response = self.client.get_job_output(vaultName=self.vault_name,
                                                  jobId=job_entry["_id"],
                                                  range="bytes {0}-{1}/*".format(byte_first, byte_last))

            if response["status"] == 200 or response["status"] == 206:
                tmp_chunk_fd, tmp_chunk_path = tempfile.mkstemp(dir=tmp_dir)
                with os.fdopen(tmp_chunk_fd, "wb") as f_tmp_chunk:
                    f_tmp_chunk.write(response["body"].read())
                    chunk_files.append(tmp_chunk_path)
                    self.logger.info("Written bytes {0} to {1} to {2}".format(byte_first, byte_last, tmp_chunk_path))
                    last_byte_downloaded = byte_last
            else:
                self.logger.error(
                    "Getting job output for job {0} returned non-successful HTTP code: {1}".format(job_entry["_id"],
                                                                                                   response["status"]))
                # TODO: Cleanup temp files, reschedule get_job_output
                return None

        # We should delete the retrieval job, now that we have the data
        mongoops.delete_retrieval_entry(self.db, job_entry["_id"])

        # Now that we have all of the files, join them together
        download_dir = job_entry["job_retrieval_destination"]
        download_relpath = archive_entry["path"]
        download_fullpath = os.path.join(download_dir, download_relpath)

        try:
            os.makedirs(os.path.splitext(download_fullpath)[0])
        except os.error:
            # Directory structure already exists
            pass

        # Join the chunks together
        # TODO: Split this into separate function, so it can easily be rescheduled?

        self.logger.info("Concatenating chunks to into archive at {0}".format(download_fullpath))
        with open(download_fullpath, "ab") as f_dest:
            for tmp_chunk_path in chunk_files:
                f_dest.write(open(tmp_chunk_path, 'rb').read())
                f_dest.flush()
                os.remove(tmp_chunk_path)

        self.logger.info("Removing temp dir at{0}".format(tmp_dir))
        os.rmdir(tmp_dir)

        # Make sure that local treehash matches original upload treehash
        with open(download_fullpath, "rb"):
            local_hash = botocore.utils.calculate_tree_hash(download_fullpath)
            if archive_entry["treehash"] == local_hash:
                return download_fullpath
            else:
                logging.error("Downloaded archive treehash does not match original treehash!")
                raise botocore.exceptions.ChecksumError(checksum_type="SHA256 treehash",
                                                  expected_checksum=archive_entry["treehash"],
                                                  actual_checksum=local_hash)

        return None
        # TODO: Reschedule job?

    def dearchive_file(self, archive_path, extract_path):
        """Unzips contents of named archive to named directory.

        Args:
            archive_path: Absolute path to .zip archive file to unzip.
            extract_path: Absolute path to directory to extract contents of `archive_path` to.

        Returns:
            Boolean identifying success - True if archive operation was successful, False otherwise.
            BadZipFile is such a generic exception that identifying why a file may have failed to unzip is a somewhat
            tricky task.
        """

        try:
            # subprocess.check_call(["7z", "x", archive_path, "-o{0}".format(os.path.splitext(archive_path[0]))])
            # return True

            zf = zipfile.ZipFile(open(archive_path, "rb"), "r", allowZip64=True)
            self.logger.info("Extracting files from {0} to {1}".format(archive_path, extract_path))
            zf.extractall(extract_path)
            zf.close()
            return True

        except zipfile.BadZipfile, e:
            self.logger.error("Bad Zip File error: {0}", e.message)
            self.logger.debug(e.args)
            return False
