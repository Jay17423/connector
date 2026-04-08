# connector

FIXME

## Prerequisites

You will need [Leiningen][] 2.0.0 or above installed.

[leiningen]: https://github.com/technomancy/leiningen

## Running

To start a web server for the application, run:

    lein uberjar
    lein run

## API request body

### Common request body

```json
{
    "type": "gdrive | dropbox | s3 | gcs | local",
    "format": "csv",
    "link-type": "public | private",
    "source": {
        "path": "",
        "url": "",
        "bucket": "",
        "key": "",
        "region": "",
        "revision-id": "",
        "version-id": "",
        "generation": ""
    },
    "auth": {
        "access-key": "",
        "secret-key": "",
        "project-id": "",
        "client-email": "",
        "private-key": "",
        "client-id": "",
        "client-secret": "",
        "refresh-token": ""
  },    
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Rules:
- `format` is always `csv`.
- `link-type = private` requires `auth`.
- `link-type = public` does not require `auth`.
- `revision-id`, `version-id`, `generation` are optional.
- Dropbox: public uses `source.url`, private uses `source.path`.

### GDrive

Public:

```json
{
    "type": "gdrive",
    "format": "csv",
    "link-type": "public",
    "source": {
        "url": "https://drive.google.com/file/d/FILE_ID/view?usp=sharing"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

Private (optional `revision-id`):

```json
{
    "type": "gdrive",
    "format": "csv",
    "link-type": "private",
    "source": {
        "url": "https://drive.google.com/file/d/FILE_ID/view?usp=sharing",
        "revision-id": "REV-ID"
    },
    "auth": {
    "client-id": "xxxx",
    "client-secret": "xxxx",
    "refresh-token": "xxxx"
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
    "link-type": "public",
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
    "link-type": "private",
    "source": {
        "path": "/customers-100000.csv"
    },
    "auth": {
        "token": "DROPBOX-TOKEN"
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
    "link-type": "public",
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

Private (optional `version-id`):

```json
{
    "type": "s3",
    "format": "csv",
    "link-type": "private",
    "source": {
        "bucket": "my-bucket",
        "key": "transaction.csv",
        "region": "ap-south-1",
        "version-id": "VERSION-ID"
    },
    "auth": {
        "access-key": "S3-ACCESS-KEY",
        "secret-key": "S3-SECRET-KEY"
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
    "link-type": "public",
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
    "link-type": "private",
    "source": {
        "bucket": "my-gcs-bucket",
        "key": "data/file.csv",
        "generation": "GENERATION-ID"
    },
    "auth": {
        "project-id": "GCP-PROJECT-ID",
        "client-email": "svc@project.iam.gserviceaccount.com",
        "private-key": "-----BEGIN PRIVATE KEY-----..."
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
    "link-type": "public",
    "source": {
        "path": "/data/input.csv"
    },
    "options": {
        "header": true,
        "delimiter": ","
    }
}
```

