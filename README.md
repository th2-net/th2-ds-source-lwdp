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

|Data Source Status|Data Source Name|Req. provider version for DS impl|DS Impl Status|DS Impl version|DS Impl grpc version|Features|
|--|--|--|--|--|--|--|
|Released|LwDP|[1.1.0](https://github.com/th2-net/th2-lw-data-provider/tree/v1.1.0)|Canceled|[1.0.1.0](https://github.com/th2-net/th2-ds-source-lwdp/tree/dev_1.0.1.0)|[1.1.0](https://github.com/th2-net/th2-grpc-data-provider/blob/756e6841a4f3923789486fd17a39a25176f50a20/src/main/proto/th2_grpc_data_provider/data_provider.proto) <br> *LwDP 1.1.0 do not use all RPCs and all fields from grpc 1.1.0 because this PROTO file is shared with RDP6. This was solved since 1.1.1*||
|Dev|LwDP|[2.0.0](https://github.com/th2-net/th2-lw-data-provider/tree/dev-version-2)|Released|[2.x.y.z](https://github.com/th2-net/th2-ds-source-lwdp/tree/release-2.0)|Canceled <br> *We decided to not implement GRPC version because it works more slowly than http*|groups + books & pages|
|Dev|LwDP|_3.0.0_ <br> (actually 2.6.0+ with TP mode*)|Released|[3.0.1.0+](https://github.com/th2-net/th2-ds-source-lwdp/tree/release-3.0)|Not supported by Impl|Transp proto|
|Dev|LwDP|_3.0.0_ <br> (actually 2.6.0+ with TP mode*)|Released|[3.1.0.0+](https://github.com/th2-net/th2-ds-source-lwdp/tree/v3.1.0.1)|Not supported by Impl|ds-impl 3.1.x.y is appeared because of few not backward compatible changes [https://github.com/th2-net/th2-ds-source-lwdp/releases/tag/v3.1.0.0](https://github.com/th2-net/th2-ds-source-lwdp/releases/tag/v3.1.0.0)|
|Dev|LwDP|_3.0.0_ <br> (actually 2.12.0+ with TP mode*)|Released|[3.1.1.0+](https://github.com/th2-net/th2-ds-source-lwdp/tree/v3.1.0.0)|Not supported by Impl|Added in LwDP <br> - `/download/events` endpoint to download events as file in JSONL format <br> - `EVENTS` resource option for `/download` task endpoint|

\* TP mode – trasport protocol mode in LwDP.

## 2.3. Release versioning

Implementations versions have the following structure: `DataSourceMajorVersion`.`ImplVerison`

`DataSourceMajorVersion` - the major version of LwDP the release uses

`ImplVerison` - the version of data source implementation in `Major`.`Minor`.`Patch` versioning semantic style

For example `v1.0.1.0` is the version for LwDP `v1.x.y`. The implementation version `0.1.0`.
