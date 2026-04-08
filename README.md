# Connector

Connector is a Clojure service that loads CSV data from multiple sources and
returns a preview response.

Supported source types:
- local filesystem
- Google Drive
- Dropbox
- Amazon S3
- Google Cloud Storage

## Prerequisites

- JDK 17
- Leiningen 2.0.0+

Verify Java version:

```bash
java -version
```

Expected major version is 17.

Install Leiningen:
- https://github.com/technomancy/leiningen

## Configuration

Application config is loaded from `config/config.edn`.

Default values:
- HTTP port: `3000`
- Spark app name: `connector-app`
- Spark master: `spark://fc-bergin:7077`

If needed, edit `config/config.edn` before starting the service.

## Build And Run

From project root:

```bash
lein uberjar
lein run
```

When startup is complete, the API is available at:

```text
http://localhost:3000
```

## API Overview

- Method: `POST`
- Endpoint: `/data-load`
- Content-Type: `application/json`
- Response: JSON

Example curl call:

```bash
curl -X POST http://localhost:3000/data-load \
    -H "Content-Type: application/json" \
    -d '{
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
    }'
```

## Request Body

### Common structure

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
        "token": "",
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

### Rules

- `format` must be `csv`.
- For `link-type = public`, `auth` is usually not required.
- For `link-type = private`, include provider-specific `auth` fields.
- `revision-id`, `version-id`, and `generation` are optional.
- Dropbox uses:
    - `source.url` for public links
    - `source.path` for private files

## Source-specific Examples

### Google Drive

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
        "path": "/customers-100000.csv",
        "revision-id":"xxxxx"
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

### Amazon S3

Public (public bucket/object):

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

### Google Cloud Storage

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

### Local Filesystem

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

## Response Format

Success (`200`):

```json
{
    "status": "success",
    "source": "local",
    "duration-ms": 123,
    "data": [
        {
            "columnA": "value1",
            "columnB": "value2"
        }
    ]
}
```

Validation error (`400`):

```json
{
    "status": "error",
    "msg": "Invalid request body",
    "error": "...details...",
    "duration-ms": 15
}
```

Internal error (`500`):

```json
{
    "status": "error",
    "msg": "Internal server error",
    "duration-ms": 20
}
```

