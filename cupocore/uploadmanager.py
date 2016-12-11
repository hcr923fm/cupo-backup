import logging
import mongoops
import threading
import os, os.path


class UploadManager():
    def __init__(self, db, client, vault_name):
        self._concurrent_upload_limit = 3
        self.chunk_size = 16777216 # Multipart size in bytes
        self.db = db
        self.client = client
        self.vault_name = vault_name

        self.logger = logging.getLogger("cupobackup{0}.UploadManager".format(os.getpid()))

        self.upload_threads = []

    def initialize_upload(self, tmp_archive_location, subdir_rel_path, archive_checksum, archive_size):
        try:
            response = self.client.initiate_multipart_upload(vaultName=self.vault_name,
                                                             archiveDescription=subdir_rel_path,
                                                             partSize=str(self.chunk_size))

        except Exception, e:
            self.logger.error("Failed to init multipart upload!")
            self.logger.debug("Error msg:\n{0}n\Error args:\n".format(e.message, e.args))
            return False

        for i in xrange(0, archive_size+1, self.chunk_size-1):
            if i + self.chunk_size >= archive_size - 1:
                last_byte = archive_size - 1
            else:
                last_byte = i + self.chunk_size

            mongoops.create_mpart_part_entry(self.db, mongoops.get_vault_by_name(self.db, self.vault_name)["arn"],
                                             response["uploadId"], i, last_byte, tmp_archive_location)

        while len(self.upload_threads) < self._concurrent_upload_limit:
            t = threading.Thread(target=self.thread_worker,
                                 kwargs={"archive_size": archive_size,
                                         "archive_checksum": archive_checksum,
                                         "subdir_rel_path": subdir_rel_path})
            self.upload_threads.append(t)
            t.start()

    def thread_worker(self, *args, **kwargs):
        mpart_entry = mongoops.get_oldest_inactive_mpart_entry(self.db, self.vault_name)
        mongoops.set_mpart_active(self.db, mpart_entry["_id"])

        try:
            self.logger.debug("File at {0} exists: {1}".format(mpart_entry["tmp_archive_location"],
                                                               os.path.exists(mpart_entry["tmp_archive_location"])))
            with open(mpart_entry["tmp_archive_location"], "rb") as mpart_f:
                mpart_f.seek(mpart_entry["first_byte"], 0)
                upload_response = self.client.upload_multipart_part(vaultName=self.vault_name,
                                                                    uploadId=mpart_entry["uploadId"],
                                                                    range="bytes {0}-{1}/*".format(
                                                                        mpart_entry["first_byte"],
                                                                        mpart_entry["last_byte"]-1),
                                                                    body=mpart_f.read(
                                                                        mpart_entry["last_byte"] - mpart_entry[
                                                                            "first_byte"]))
                if upload_response:
                    mongoops.delete_mpart_entry(self.db, mpart_entry["_id"])
                    self.logger.info("Uploaded bytes {0} to {1} of {2}".format(mpart_entry["first_byte"],
                                                                               mpart_entry["last_byte"]-1,
                                                                               mpart_entry["tmp_archive_location"]))

        except Exception, e:
            self.logger.error("Failed to upload mpart!")
            self.logger.debug("Error msg:\n{0}\nError args:\n{1}".format(e.message, e.args))
            return False

        # At end, check if there are any more parts with this uploadId - if not, complete the mpart upload
        is_more = mongoops.is_existing_mparts_remaining(self.db, self.vault_name, mpart_entry["uploadId"])
        if not is_more:
            try:
                final_response = self.client.complete_multipart_upload(vaultName=self.vault_name,
                                                  uploadId=mpart_entry["uploadId"],
                                                  archiveSize=kwargs["archive_size"],
                                                  checksum=kwargs["archive_checksum"])
                mongoops.create_archive_entry(self.db, kwargs["subdir_rel_path"],
                                              mongoops.get_vault_by_name(self.db, self.vault_name)["arn"],
                                              final_response["archiveId"], final_response["checksum"],
                                              kwargs["archive_size"], final_response["location"])
                os.remove(mpart_entry["tmp_archive_location"])
            except Exception, e:
                self.logger.error("Failed to complete mpart upload!")
                self.logger.debug("Error msg:\n{0}\nError args:\n{1}".format(e.message, e.args))
                return False

