---
layout: single
author_profile: true
---
{% include base_path %}

# Cupo

## Welcome to Cupo!

*Cupo* is an incremental backup system, designed to back up a file tree to Amazon Glacier, uploading only the files that have changed between backups and maintaining redundant versions for retrieval.

## Grabbing Cupo
To grab the latest release, download the source. It's all Python - no compilation required! Just [download and install the prerequisites](https://calmcl1.github.com/cupo-backup/quick-start#installing), clone the repo, and you're good to go!
Find Cupo on [GitHub]({{ site.github.repository_url }})

### Latest Version: [{{ site.github.releases[0].name }}]({{ site.github.releases[0].url }}){: .btn}

[Download .tar]({{ site.github.releases[0].tarball_url }}){: .btn .btn--info .btn--large}
[Download .zip]({{ site.github.releases[0].zipball_url }}){: .btn .btn--info .btn--large}

#### Changes in this version:
{{ site.github.releases[0].body }}
