---
permalink: /get-cupo/
author_profile: true

feature_row:
  - image-path: /path/to/tarball/image
    excerpt: Best for Linux and OSX users
  - image-path: /path/to/zipball/image
    excerpt: Best for Windows users

---

# Downloading Cupo

## Current version ({{ site.github.releases[0].name }})

The core of Cupo is all Python, so just grab a copy of the source. You can either clone the GitHub repository or download an archive.

{% include feature_row type="center" %}

Or clone the repository:
```bash
git clone https://github.com/calmcl1/cupo-backup.git
```

## Older versions

To use an older release of Cupo, download a source archive from the list below.

{% for release in site.github.releases %}

### {{ release.name }}

**Download tar.gz archive**: [{{ release.name }}.tar.gz]({{ release.tarball_url }})

**Download zip archive**: [{{ release.name }}.zip]({{ release.zipball_url }})

**Release info on GitHub** : [Cupo {{ release.name }}]({{ release.url }})

{% endfor %}
