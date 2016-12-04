import logging
import os
import threading
import tempfile
import mongoops
import botocore.utils
import subprocess


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
            job_entry = mongoops.get_oldest_retrieval_entry(self.db, self.vault_name)

            if not job_entry:
                self.logger.info("No jobs available! Stopping trying to retrieve")
                self.check_for_jobs.clear()
                return

            # TODO: If job was last checked less than an hour ago, wait for the rest of the hour

            self.logger.info("Checking if job {0} is ready".format(job_entry["_id"]))

            status = self.check_job_status(job_entry["_id"])
            if not status:
                logging.info("Job {0} is not ready.".format(job_entry["_id"]))
                mongoops.update_job_last_polled_time(self.db, job_entry["_id"])
                continue

            logging.info("Job {0} is ready - commencing download".format(job_entry["_id"]))

            local_arch_fullpath = self.download_archive(job_entry)
            if local_arch_fullpath:
                self.dearchive_file(local_arch_fullpath)
            os.remove(local_arch_fullpath)

    def download_archive(self, job_entry):
        archive_entry = mongoops.get_archive_by_id(self.db, job_entry["archive_id"])
        tmp_dir = tempfile.mkdtemp()

        # Break the job up into 128MB chunks to make life easier
        chunk_files = []

        last_byte_downloaded = -1

        while last_byte_downloaded < archive_entry["size"]:
            byte_first = last_byte_downloaded + 1
            if (128 * 1000000 + last_byte_downloaded) >= archive_entry["size"]:
                byte_last = archive_entry["size"]
            else:
                byte_last = 128 * 1000000 + last_byte_downloaded

            response = self.client.get_job_output(vaultName=self.vault_name,
                                                  jobId=job_entry["_id"],
                                                  range="bytes={0}-{1}".format(byte_first, byte_last))

            if response["status"] == 200 or response["status"] == 206:
                tmp_chunk_fd, tmp_chunk_path = tempfile.mkstemp(dir=tmp_dir)
                with os.fdopen(tmp_chunk_fd, "wb") as f_tmp_chunk:
                    f_tmp_chunk.write(response["body"].read())
                    chunk_files.append(tmp_chunk_path)
            else:
                self.logger.error(
                    "Getting job output for job {0} returned non-successful HTTP code: {1}".format(job_entry["_id"],
                                                                                                   response["status"]))
                # TODO: Cleanup temp files, reschedule get_job_output
                continue

        # We should delete the retrieval job, now that we have the data
        mongoops.delete_retrieval_entry(self.db, job_entry["_id"])

        # Now that we have all of the files, join them together
        download_dir = job_entry["job_retrieval_destination"]
        download_relpath = archive_entry["archive_dir_path"]
        download_fullpath = os.path.join(download_dir, download_relpath)

        try:
            os.makedirs(os.path.splitext(download_fullpath)[0])
        except os.error:
            # Directory structure already exists
            pass

        with open(download_fullpath, "ab") as f_dest:
            for tmp_chunk_path in chunk_files:
                f_dest.write(open(tmp_chunk_path, 'rb').read())
                f_dest.flush()
                os.remove(tmp_chunk_path)

        os.rmdir(tmp_dir)

        # Make sure that local treehash matches original upload treehash
        with open(download_fullpath, "rb"):
            local_hash = botocore.utils.calculate_tree_hash(download_fullpath)
            if archive_entry["treehash"] == local_hash:
                return download_fullpath

        return False
        # TODO: Reschedule job?

    def dearchive_file(self, archive_path):
        """
        Unzips contents of named archive to directory at 'archive_path/archive_name'
        :param archive_path: Absolute path to archive to unzip
        """

        try:
            subprocess.check_call(["7z", "x", archive_path, "-o{0}".format(os.path.splitext(archive_path[0]))])
            return True
        except subprocess.CalledProcessError, e:
            ret_code = e.returncode
            if ret_code == 1:
                # Warning (Non fatal error(s)). For example, one or more files were locked by some
                # other application, so they were not compressed.
                self.logger.info("7-Zip: Non-fatal error (return code 1)")
                return None
            elif ret_code == 2:
                # Fatal error
                self.logger.info("7-Zip: Fatal error (return code 2)")
                return None
            elif ret_code == 7:
                # Command-line error
                self.logger.info("7-Zip: Command-line error (return code 7)\n%s"
                                 % e.cmd)
                return None
            elif ret_code == 8:
                # Not enough memory for operation
                self.logger.info("7-Zip: Not enough memory for operation (return code 8)")
                return None
            elif ret_code == 255:
                # User stopped the process
                self.logger.info("7-Zip: User stopped the process (return code 255)")
                return None
