#from pymongo import MongoClient
import pymongo
import time


# Archive records are organised by the path of the origin directory, with sub-entries for each individual version
# of the uploaded archive.
# Use the format:
#
# db.archives.insert_one({
#     "path": "/path/to/archived/subdir",
#     "vault_arn":
#     "versions":
#         [
#             {
#                 "_id": "AWS-ARCHIVE-ID-GOES-HERE-ABCDEFHGIJKLMNOPQRSTUVWXYZ0123456789",
#                 "treehash": "SHA256-TREEHASH-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "size": 123456789,
#                 "uploaded_time": 147258369,
#                 "aws_URI": "aws://AWS-ARCHIVE-URI-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "to_delete": False
#             },
#             {
#                 "_id": "AWS-ARCHIVE-ID-GOES-HERE-ABCDEFHGIJKLMNOPQRSTUVWXYZ0123456789",
#                 "treehash": "SHA256-TREEHASH-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "size": 987654321,
#                 "uploaded_time": 369258147,
#                 "aws_URI": "aws://AWS-ARCHIVE-URI-GOES-HERE-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
#                 "to_delete": False
#             }
#         ]
# })
#
#
# db.vaults.insert_one({
#     "arn": "aws://AWS-VAULT-ARN-123456789",
#     "name": "vault name",
#     "region": "eu-west-1"
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

def create_vault_entry(db, vault_arn, vault_name, vault_region):
    # Check first to see if there's already a vault by this name.
    existing_vault_entry = db["vaults"].find_one({"name": vault_name})
    # If so, return that instead.
    if existing_vault_entry: return existing_vault_entry["_id"]

    # If not, let's create one with the specified info...
    doc_vault = {}
    doc_vault["arn"] = vault_arn
    doc_vault["name"] = vault_name
    doc_vault["region"] = vault_region

    return db["vaults"].insert_one(doc_vault).inserted_id

def create_archive_entry(db, archived_dir_path, vault_arn, aws_archive_id,
                         archive_treehash, archive_size, aws_uri):

    # Find an entry in the archives list that matches the path and vault arn
    # that we are uplaoding to. If it's found, append the information about the
    # newly uploaded archive to the 'versions' field.

    # Create an entry that refers to the vault and path that we are looking
    # for.
    doc_path = {}
    doc_path["path"] = archived_dir_path
    doc_path["vault_arn"] = vault_arn

    # Now create a subdocument for the version of the vault that we have
    # uploaded.
    doc_version = {}
    doc_version["_id"] = aws_archive_id
    doc_version["treehash"] = archive_treehash
    doc_version["size"] = archive_size
    doc_version["uploaded_time"] = time.time()
    doc_version["aws_URI"] = aws_uri
    doc_version["to_delete"] = False

    # Add the sub-entry.
    return db['archives'].find_one_and_update(doc_path,
                                       {'$push': {'versions':doc_version}},
                                       upsert=True,)

def connect(database_name, host="localhost", port=27017):
    mongodb_uri = "mongo://{host}:{port}".format(host=host, port=port)
    client = pymongo.MongoClient(mongodb_uri)

    if database_name in client.database_names():
        db = client[database_name]
    else:
        db = create_backup_database(database_name, client)

    return client, db


def disconnect(client):
    client.close()
