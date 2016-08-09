---
permalink: /about/
---

# About Cupo

## What does Cupo do?
*Cupo* is an incremental backup system designed to be used with Amazon Glacier. The idea is that, if used with a system where only small parts of the file system change between backups, rather than duplicate the majority of the backup (which hasn't changed since the previous backup), just upload the files that have changed. If no files have changed, no backups are uploaded.

That way, simply downloading the latest version of each backed-up file will give you an up-to-date backup, even if some of the backups are years old (because they haven't changed in years!).

By default, *Cupo* will create an archive for each directory in a given tree, and upload that directory to Amazon Glacier. Each time a file in that directory changes, *Cupo* will re-archive that directory and upload the new version. Once a backup archive has become obsolete, it is removed from Glacier.

A backup archive is considered to be obsolete when:

* It is older than a given threshold (by default, three months, as this is the Glacier minimum archive term)
* AND there are at least **x** more recent versions of that archive in Glacier (by default, 3)

## How does Cupo work?

*Cupo* manages the archives by keeping a local database of all of the archives that have been uploaded and comparing them to any new archives that it creates. When a new backup is uploaded, an corresponding entry is created in the *Cupo* database. In this manner, it keeps track of everything that it has uploaded to Glacier.

## Why was Cupo created?
*Cupo* was originally created to manage the disaster-recovery backups for a radio station in the UK. At the time, the station didn't have the bandwidth or the finances to store terabytes of largely-identical redundant backups. Since most of the system doesn't often change, it only made sense to continually back up the files that did. In order to minimize the cost whilst keeping reliability as high as possible, a project called *HCRBackup* was created to manage the incremental backups to Glacier. This later became *Cupo*.
