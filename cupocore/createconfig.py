import os, os.path
import logging
import json

def create_config_file(file_location):
    config_opts = {"database": "",
                   "vault_name": "",
                   "account_id": "",
                   "aws_profile": "",
                   "debug": false,
                   "logging_dir": "",
                   "backup_directory": ""
                   }

    with open(file_location) as f:
        json.dump(config_opts, f, indent=4, separators=(",", ": "))
