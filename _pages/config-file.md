---
permalink: /config-file/
---

# Config File Reference

To save having to use painfully long command-line strings to pass settings to Cupo, it's often easier to use a config file. This is simply a JSON file:

```
{
  "database": "DATABASE-NAME",
  "vault_name": "VAULT-NAME",
  "account_id": "000000000000",
  "aws_profile": "AWS-PROFILE",
  "debug": false,
  "logging_dir": "/home/USERNAME",
  "backup_directory": "/path/to/dir"
}
```

* `database`: The name of the MongoDB database to track the backup operation in. Using multiple databases allows you to manage multiple backup operations.
* `vault_name`: The name of the Glacier vault to archive the files to. Must be registered in the local database - use `cupo.py new-vault VAULT_NAME` to make sure.
* `account_id`: Your AWS account ID. Should be a number.
* `aws_profile`: If you configured the AWS CLI to use a profile to store your credentials in, supply its' name here. Leave blank if unsure.
* `debug`: Allows more verbose output in the log.
* `logging_dir`: Specifies which directory to write the log `.cupoLog` to. Leave blank for home directory.
* `backup_directory`: The big one - the top folder to back up to Glacier.