import logging
import os


class RetrievalManager():
    def __init__(self, client, vault_name):
        self.client = client
        self.logger = logging.getLogger("cupobackup{0}.RetrievalManager".format(os.getpid()))
        self.vault_name = vault_name

        # TODO: make data retrieval mechanism, set threading up

    def initiate_retrieval(self, archive_id):
        job_params = {
            "Format": "JSON",
            "Type": "archive-retrieval",
            "ArchiveID": archive_id
        }

        init_job_ret = self.client.initiate_job(vaultName=self.vault_name,
                                                jobParameters=job_params)

        return init_job_ret or None

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
