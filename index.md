---
layout: single
author_profile: true
---
{% include base_path %}

# Cupo - Incremental Backup for Amazon Glacier

## Welcome to Cupo!

*Cupo* is an incremental backup system, designed to back up a file tree to Amazon Glacier, uploading only the files that have changed between backups and maintaining redundant versions for retrieval.

## Getting Cupo
It's all Python - no compilation required! Just [download and install the prerequisites](https://calmcl1.github.com/cupo-backup/quick-start#installing), grab a copy of the source, and you're good to go!

### Latest Version: [{{ site.github.releases[0].name }}]({{ site.github.releases[0].url }}){: .btn}

[Download .tar]({{ site.github.releases[0].tarball_url }}){: .btn .btn--info .btn--large}
[Download .zip]({{ site.github.releases[0].zipball_url }}){: .btn .btn--info .btn--large}

### Bleeding Edge
To get the latest copy of the source - which may or may not be entirely stable or work as according to the documentation - just clone the GitHub repo: ` {{ site.github.clone_url }} ` 

Find Cupo on <i class="fa fa-fw fa-github"></i>[GitHub]({{ site.github.repository_url }}).

#### Changes in this version:
{{ site.github.releases[0].body }}
