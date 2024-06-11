# Builds

Our CI/CD pipelines utilize a Docker build image configured with support for GoLang, Rust, MUSL, OSXCross and Protobuf. The `circle.yml` file references this Docker container to handle building, testing, and creating release packages.

## Custom Builder

The necessity for a custom builder arises from compatibility issues between the `protobuf` library and Chronograf's Python UDFs. The `cross-builder` was updated to `protobuf` version `26.1` in [PR #669](https://github.com/influxdata/edge/pull/669), introducing breaking changes in the Python protobuf library. Specifically, [protobuf 5.26.1 on PyPI](https://pypi.org/project/protobuf/5.26.1/) does not support Python 2. Consequently, using the newest `cross-builder` would result in the loss of Python v2 support in UDFs.

:warning: **Note:** The custom builder depends on the `MUSL` compiler this. In the current state (`2024-06-11`) the `MUSL` compiler requires Intel hardware to build. This means that the custom builder is not able to __build__ on Apple Silicon hardware.

## Updating Component Versions

To update component versions like GoLang, Rust, and Protobuf, modifications must be made in `Dockerfile_build`. After updates, a new Docker image needs to be built, published, and then utilized in CI.

### Rust

The Rust version is defined in the `Dockerfile_build` file. The Rust version should be same as the compile version for `flux` library.

### Step 1: Authenticate with Quay.io

```sh
export QUAY_CD_USER=<quay.io username>
export QUAY_CD_PASSWORD=<quay.io token>
```

### Step 2: Build and Push the New Docker Image to Quay

Navigate to the builder directory and execute the build script:

```sh
cd $KAPACITOR_REPOSITORY_ROOT/builder
./Dockerfile_build_push.sh
```

### Step 3: Update Scripts and CircleCI Configuration

1. Update the `cross-builder` tag in `.circleci/config.yml` to the new version.
2. Update the `quay.io/influxdb/builder` tag in `Dockerfile_build_ubuntu64` to reflect the new version.
