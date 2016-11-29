---
permalink: /quick-start/
---
{% include toc %}

# Quick Start

Getting started with Cupo is fairly straightforward – I think! It's as simple as installing the software and it's dependencies, creating a config file, creating a vault on AWS and storing our files in it.

Note: This guide assumes that you have Python (v2) installed. Cupo is just a set of Python scripts, so Python is kind of important. You'll need ``pip`` as well.
{:.notice--info}

## Installing Cupo

### Installing the Prerequisites

Cupo depends on a few things to make the backend work - namely: 7-Zip, MongoDB, the AWS CLI, `pymongo` (the MongoDB Python bindings), and `boto3` (the AWS Python bindings.) We'll grab these now.

* Firstly, download and install MongoDB as per [the MongoDB documentation](https://docs.mongodb.com/manual/installation/#mongodb-community-edition>) - don't forget to create the MongoDB `data` directory, either `C:\\Data` or `/var/lib/mongodb`
* Then, download 7-Zip.
	* On Debian: `apt install p7zip-full`
	* Others: Go to the [7-Zip website](http://www.7-zip.org/download.html) and download and install the correct version of 7-Zip for you.
* Then, the rest can be installed with `pip`.
	* On Windows: `python -m pip install pymongo boto3 awscli`
	* On Linux/OSX: `pip install pymongo boto3 awscli`

### Configuring AWS

In order for Cupo (or any application that uses AWS, for that matter) to be able to use AWS on your behalf, you'll need to use the AWS CLI to store your credentials. It's quick and simple – just run:

``aws configure``

and follow the instructions in the terminal.

### Getting the Cupo Source
Getting Cupo itself is as easy as downloading the source archive.

* Zip archive: <i class="fa fa-archive"></i> [Download .zip archive]({{ site.github.releases[0].zipball_url }})
* Tar archive: <i class="fa fa-archive"></i> [Download .tar.gz archive]({{ site.github.releases[0].tarball_url }})

Extract the files to a directory, and you're good to go!

## Using Cupo

Now that Cupo is good to go, we'll start by creating a vault in AWS and then upload a directory tree to it!

### Creating A Vault

In Glacier, all your archives are stored in a vault. We'll need to create one. Run:

`cupo.py -i YOUR-AWS-ACCOUNT-ID -d MONGODB-NAME NEW-VAULT-NAME`

Note the `MONGODB-NAME` parameter – Cupo uses a MongoDB database to track which archives you've uploaded, so you can use multiple databases to manage different backup systems. Just pick a name for the database.

This has now created a new vault in Glacier and registered it in the local database.

### Backing Up A Directory

Backing up a directory tree to Glacier is simple - it's just a matter of

`cupo.py [global_options] backup [backup_options]`

#### Creating A Config File

...but not *that* simple.

See those `[global_options]` and `[backup_options]` parts above? Cupo can work with different Glacier vaults, different databases, even different AWS accounts. As such, specifiying all of that on the command line can make for a horribly complicated command-line string. Save your future self some pain and set up a config file – future you will buy you a beer in return. Anyway, Cupo will do it for you!

`cupo.py sample-config /path/to/config/file`

Open up the config file (it's just a JSON file) in your favourite text editor and fill in the variables. Info about all of the fields can be found in [the Config File reference](https://calmcl1.github.com/cupo-backup/config-file), but you'll only need to fill in the `database`, `vault_name`, `account_id` and `backup_directory` fields.

Then, to kick off our backup operation, we can just run:

`cupo.py -c /path/to/config/file backup`

And watch Cupo ticking away, uploading your files to Glacier!

If anything happens, you can check the output on the terminal, or in `~/.cupoLog`.