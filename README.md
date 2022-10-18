# 1. Introduction

This repository is a library for implementing LwDP data source.

The library uses ["Lightweight Data Provider"](https://github.com/th2-net/th2-lw-data-provider) as provider.

# 2. Getting started

## 2.1. Installation

- From PyPI (pip)   
  This package can be found on [PyPI](https://pypi.org/project/th2-data-services-lwdp/ "th2-data-services-lwdp").
    ```
    pip install th2-data-services-lwdp
    ```

- From Source
    ```
    git clone https://github.com/th2-net/th2-ds-source-lwdp
    pip install th2-data-services-lwdp
    ```

## 2.2. Releases

Currently there is only [ds-lwdp v1](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_1.0.1.0) under developement.

Newer releases will have separate branches indicated by SourceVersion of branch name.

# 2.3 Versioning

Versioning of the library looks like this:

DataSourceMajorVersion.LibVerison

DataSourceMajorVersion - the major version of LwDP the release uses
LibVersion - the version of data source implementation in Major.Minor.Patch versioning semantic style

## 2.4. Examples

[Example of using data source v1 implementation](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_1.0.1.0/examples/demo.py).

-The rest, for example mapping and filtering data or using event trees, is the same for now as in ["th2-data-services"](https://github.com/th2-net/th2-data-services).
