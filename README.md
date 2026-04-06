# connector

FIXME

## Prerequisites

You will need [Leiningen][] 2.0.0 or above installed.

[leiningen]: https://github.com/technomancy/leiningen

## Running

To start a web server for the application, run:

    lein ring server

## API request body

### Common request body

```json
{
    "type": "gdrive | dropbox | s3 | gcs | local",
    "format": "csv",
    "link_type": "public | private",
    "source": {
        "path": "",
        "url": "",
        "bucket": "",
        "key": "",
        "region": "",
        "revision_id": "",
        "version_id": "",
        "generation": ""
    },
    "auth": {
        "token": "",
        "access_key": "",
        "secret_key": "",
        "project_id": "",
        "client_email": "",
        "private_key": ""
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Rules:
- `format` is always `csv`.
- `link_type = private` requires `auth`.
- `link_type = public` does not require `auth`.
- `revision_id`, `version_id`, `generation` are optional.
- Dropbox: public uses `source.url`, private uses `source.path`.

### GDrive

Public:

```json
{
    "type": "gdrive",
    "format": "csv",
    "link_type": "public",
    "source": {
        "url": "https://drive.google.com/file/d/FILE_ID/view?usp=sharing"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Private (optional `revision_id`):

```json
{
    "type": "gdrive",
    "format": "csv",
    "link_type": "private",
    "source": {
        "url": "https://drive.google.com/file/d/FILE_ID/view?usp=sharing",
        "revision_id": "REV_ID"
    },
    "auth": {
        "token": "GDRIVE_TOKEN"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

### Dropbox

Public:

```json
{
    "type": "dropbox",
    "format": "csv",
    "link_type": "public",
    "source": {
        "url": "https://www.dropbox.com/s/FILE_ID/customers.csv?dl=0"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Private:

```json
{
    "type": "dropbox",
    "format": "csv",
    "link_type": "private",
    "source": {
        "path": "/customers-100000.csv"
    },
    "auth": {
        "token": "DROPBOX_TOKEN"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

### S3

Public (signed or public bucket):

```json
{
    "type": "s3",
    "format": "csv",
    "link_type": "public",
    "source": {
        "bucket": "my-bucket",
        "key": "transaction.csv",
        "region": "ap-south-1"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Private (optional `version_id`):

```json
{
    "type": "s3",
    "format": "csv",
    "link_type": "private",
    "source": {
        "bucket": "my-bucket",
        "key": "transaction.csv",
        "region": "ap-south-1",
        "version_id": "VERSION_ID"
    },
    "auth": {
        "access_key": "S3_ACCESS_KEY",
        "secret_key": "S3_SECRET_KEY"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

### GCS

Public:

```json
{
    "type": "gcs",
    "format": "csv",
    "link_type": "public",
    "source": {
        "bucket": "my-gcs-bucket",
        "key": "data/file.csv"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Private (optional `generation`):

```json
{
    "type": "gcs",
    "format": "csv",
    "link_type": "private",
    "source": {
        "bucket": "my-gcs-bucket",
        "key": "data/file.csv",
        "generation": "GENERATION_ID"
    },
    "auth": {
        "project_id": "GCP_PROJECT_ID",
        "client_email": "svc@project.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----..."
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

### Local

```json
{
    "type": "local",
    "format": "csv",
    "link_type": "public",
    "source": {
        "path": "/data/input.csv"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

## License

Copyright © 2026 FIXME
