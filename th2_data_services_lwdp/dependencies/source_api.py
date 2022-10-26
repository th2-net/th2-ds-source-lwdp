from th2_data_services.interfaces import ISourceAPI

class IProviderSourceAPI(ISourceAPI):
    """Interface for Source API of rpt-data-provider."""

class IGRPCProviderSourceAPI(IProviderSourceAPI):
    """Interface for Source API of rpt-data-provider which works via GRPC."""