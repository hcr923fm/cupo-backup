#from pymongo import MongoClient
import pymongo
import time, datetime


# Use the format:
#
# db.archives.insert_one({
#     "path": "/path/to/archived/subdir",
#     "vault_arn": "aws://vault_arn"
#     "_id": "AWS-ARCHIVE-ID-GOES-HERE-ABCDEFHGIJKLMNOPQRSTUVWXYZ0123456789",
#     "treehash": "SHA256-TREEHASH-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#     "size": 123456789,
#     "uploaded_time": 147258369,
#     "aws_URI": "aws://AWS-ARCHIVE-URI-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#     "to_delete": False
# })
#
#
# db.vaults.insert_one({
#     "arn": "aws://AWS-VAULT-ARN-123456789",
#     "name": "vault name",
# })


def create_backup_database(database_name, db_client, drop_existing=True):
    """
    Creates a MongoDB database `database_name` that is ready to be used as a backup tracking
    database for this software.
    :param database_name The name of the database we'll use
    :param host The hostname of the server running MongoDB
    :param port The port number to connect to `host` on
    :param drop_existing If true, drop any existing database with this name
    """

    if drop_existing and (database_name in db_client.database_names()):
        db_client.drop_database(database_name)

    db = db_client[database_name]
    db.create_collection('archives')
    db.create_collection('vaults')

    return db

def create_vault_entry(db, vault_arn, vault_name):
    # Check first to see if there's already a vault by this name.
    existing_vault_entry = db["vaults"].find_one({"name": vault_name})
    # If so, return that instead.
    if existing_vault_entry: return existing_vault_entry["_id"]

    # If not, let's create one with the specified info...
    doc_vault = {}
    doc_vault["arn"] = vault_arn
    doc_vault["name"] = vault_name

    return db["vaults"].insert_one(doc_vault).inserted_id

def create_archive_entry(db, archived_dir_path, vault_arn, aws_archive_id,
                         archive_treehash, archive_size, aws_uri):

    # Find an entry in the archives list that matches the path and vault arn
    # that we are uplaoding to..
    doc_arch = {}
    doc_arch["path"] = archived_dir_path
    doc_arch["vault_arn"] = vault_arn
    doc_arch["_id"] = aws_archive_id
    doc_arch["treehash"] = archive_treehash
    doc_arch["size"] = archive_size
    doc_arch["uploaded_time"] = time.time()
    doc_arch["aws_URI"] = aws_uri
    doc_arch["to_delete"] = 0

    # Add the entry.
    return db['archives'].insert(doc_arch)

def get_most_recent_version_of_archive(db, path):
    return db["archives"].find_one(
        {"path": path, "to_delete": 0},
        sort=[('uploaded_time', pymongo.DESCENDING)])

def get_old_archives(db, archived_dir_path, vault_name):
    vault_arn = get_vault_by_name(db, vault_name)["arn"]

    deadline_dt = datetime.datetime.utcnow() - datetime.timedelta(weeks=12)
    deadline_ts = time.mktime(deadline_dt.timetuple())
    cursor = db["archives"].find({"to_delete":0,
                                  "path": archived_dir_path,
                                  "uploaded_time":
                                  {"$lt": deadline_ts}
                                  },
                                 sort=[("uploaded_time", pymongo.DESCENDING)],
                                 skip=3)

    old_archives = []
    for arch in cursor:
        old_archives.append(arch)

    return old_archives

def mark_archive_for_deletion(db, archive_id):
    db["archives"].find_one_and_update({"_id": archive_id},
                                       {"$set":
                                        {
                                           "to_delete": 1
                                           }
                                        })

def get_archives_to_delete(db):
    cursor = db["archives"].find({"to_delete": 1})
    redundant_archives = []
    for arch in cursor:
        redundant_archives.append(arch)

    return redundant_archives

def delete_archive_document(db, archive_id):
    db["archives"].find_one_and_delete({"_id": archive_id})

def get_vault_by_name(db, vault_name):
    return db['vaults'].find_one({"name": vault_name})

def get_vault_by_arn(db, vault_arn):
    return db['vaults'].find_one({"arn": vault_arn})

def connect(database_name, host="localhost", port=27017):
    mongodb_uri = "{host}:{port}".format(host=host, port=port)
    client = pymongo.MongoClient(mongodb_uri)

    if database_name in client.database_names():
        db = client[database_name]
    else:
        db = create_backup_database(database_name, client)

    return client, db


def disconnect(client):
    client.close()
