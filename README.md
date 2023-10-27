# 1. Introduction

This library is the implementation of `data-services data source` for [Lightweight Data Provider](https://github.com/th2-net/th2-lw-data-provider) (LwDP).

See more about `data-services data source` [here](https://not_implemented_yet_relates_to_TH2-4185).

# 2. Getting started

## 2.1. Installation

- From PyPI (pip)   
  This package can be found on [PyPI](https://pypi.org/project/th2-data-services-lwdp/ "th2-data-services-lwdp").
    ```
    pip install th2-data-services-lwdp
    ```

## 2.2. Releases

Each release has separate branch indicated by `DataSourceMajorVersion` of branch name.

Available versions:
- [ds-lwdp1](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_1.0.1.0) - dev version. 
- [ds-lwdp2.0](https://github.com/th2-net/th2-ds-source-lwdp/tree/release-2.0) - release 2.0
- [ds-lwdp2.1](https://github.com/th2-net/th2-ds-source-lwdp/tree/release-2.1) - release 2.1
- [ds-lwdp3](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_3.0.1.0) - release 3.0

## 2.3. Release versioning

Implementations versions have the following structure: `DataSourceMajorVersion`.`ImplVerison`

`DataSourceMajorVersion` - the major version of LwDP the release uses

`ImplVerison` - the version of data source implementation in `Major`.`Minor`.`Patch` versioning semantic style

For example `v1.0.1.0` is the version for LwDP `v1.x.y`. The implementation version `0.1.0`.
