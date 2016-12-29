import logging
import mongoops
import threading
import os, os.path
import time


class UploadManager():
    def __init__(self, db, client, vault_name):
        self._concurrent_upload_limit = 5
        self.chunk_size = 16777216  # Multipart size in bytes
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

        for i in xrange(0, archive_size + 1, self.chunk_size):
            if i + self.chunk_size >= archive_size - 1:
                last_byte = archive_size - 1
            else:
                last_byte = i + self.chunk_size - 1

            mongoops.create_mpart_part_entry(self.db, mongoops.get_vault_by_name(self.db, self.vault_name)["arn"],
                                             response["uploadId"], i, last_byte, tmp_archive_location, archive_size,
                                             archive_checksum, subdir_rel_path)

        # Remove dead threads
        for t in self.upload_threads:
            if not t.is_alive(): self.upload_threads.remove(t)

        # And start new ones in their place!
        while len(self.upload_threads) < self._concurrent_upload_limit:
            t = threading.Thread(target=self.thread_worker)

            self.upload_threads.append(t)
            t.start()
            time.sleep(2)

    def thread_worker(self, *args, **kwargs):
        while True:
            mpart_entry = mongoops.get_oldest_inactive_mpart_entry(self.db, self.vault_name)
            if not mpart_entry:
                self.logger.info("Thread exiting, no more mparts available")
                return None
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
                                                                            mpart_entry["last_byte"]),
                                                                        body=mpart_f.read(self.chunk_size))
                    if upload_response:
                        mongoops.delete_mpart_entry(self.db, mpart_entry["_id"])
                        self.logger.info("Uploaded bytes {0} to {1} of {2}".format(mpart_entry["first_byte"],
                                                                                   mpart_entry["last_byte"],
                                                                                   mpart_entry["tmp_archive_location"]))

            except Exception, e:
                self.logger.error("Failed to upload mpart!")
                self.logger.debug("Error msg:\n{0}\nError args:\n{1}".format(e.message, e.args))
                self.logger.debug(e.__repr__)
                mongoops.set_mpart_inactive(self.db, mpart_entry["_id"])
                continue

            # At end, check if there are any more parts with this uploadId - if not, complete the mpart upload
            is_more = mongoops.is_existing_mparts_remaining(self.db, self.vault_name, mpart_entry["uploadId"])
            if not is_more:
                try:
                    final_response = self.client.complete_multipart_upload(vaultName=self.vault_name,
                                                                           uploadId=mpart_entry["uploadId"],
                                                                           archiveSize=str(mpart_entry["full_size"]),
                                                                           checksum=mpart_entry["full_hash"])
                except Exception, e:
                    self.logger.error("Failed to complete mpart upload at AWS!")
                    self.logger.debug("Error msg:\n{0}\nError args:\n{1}".format(e.message, e.args))
                    continue

                try:

                    mongoops.create_archive_entry(self.db, os.path.join(mpart_entry["subdir_rel_path"],
                                                                        os.path.basename(
                                                                            mpart_entry["tmp_archive_location"])),
                                                  mongoops.get_vault_by_name(self.db, self.vault_name)["arn"],
                                                  final_response["archiveId"], final_response["checksum"],
                                                  mpart_entry["full_size"], final_response["location"])

                except Exception, e:
                    self.logger.error("Failed to complete mpart upload - could not create DB archive entry")
                    self.logger.debug("Error msg:\n{0}\nError args:\n{1}".format(e.message, e.args))
                    continue

                try:
                    os.remove(mpart_entry["tmp_archive_location"])
                    self.logger.info("Completed upload of {0}".format(mpart_entry["tmp_archive_location"]))

                except Exception, e:
                    self.logger.error("Failed to complete mpart upload - could not remove temp archive")
                    self.logger.debug("Error msg:\n{0}\nError args:\n{1}".format(e.message, e.args))
                    continue

    def wait_for_finish(self):
        for t in self.upload_threads:
            if t.is_alive:
                t.join()
