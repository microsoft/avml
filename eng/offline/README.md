# AVML with Azurite Blob Storage

Use Azurite as a local Azure Blob Storage endpoint for streaming AVML memory images in
unreliable networks. This demonstrates this approach by running Azurite, creating an `avml`
container and SAS URL, streaming a memory image directly from AVML, and downloading the
completed blob.

## 0. Prerequisites

Linux, Node.js 24, and Azure CLI

## 1. Build Azurite

The following will build a linux executable version of Azurite.

```bash
git clone --branch v3.36.0 https://github.com/Azure/Azurite.git
cd Azurite
npm install
npm run build:linux
```

The executable is created at:

```text
release/azuritelinux
```

## 2. Start Azurite

Azurite can start its Storage Blob server with a specific destination directory.

```bash
mkdir -p .azurite

./release/azuritelinux \
  --blobHost <BIND_ADDR> \
  --blobPort 10000 \
  --location .azurite \
  --disableTelemetry \
  --skipApiVersionCheck
```

## 3. Create the container and SAS URL

Run the following to initialize Azurite and create an AVML command to use.

```bash
./eng/offline/setup-azurite-avml.sh <BIND_ADDR> [PORT]
```

The port defaults to `10000`.

The script will:

1. Create the `avml` blob container
1. Generate the destination blob name
1. Generate a SAS token
1. Print the AVML command

## 4. Stream to Azurite

The setup script prints the avml command to use:

```bash
sudo avml stream blob --compress 'http://<AZURITE_IP>:10000/devstoreaccount1/avml/snapshot-<TIMESTAMP>.lime?<SAS>'
```

AVML streams the snapshot directly into a block blob; it does not first write the image to local
disk. The blob becomes visible as a completed blob only after the stream finishes and AVML commits
its block list.

## 5. List and download the blobs

```bash
./eng/offline/download-azurite-avml.sh <AZURITE_IP> [PORT] [OUTPUT_DIRECTORY]
```

The script lists and downloads every blob in the `avml` container. The port defaults to `10000`
and the destination defaults to `./avml-blobs`.
