"""Configure Qdrant transport and warnings for local HTTP connections."""

import warnings

from qdrant_client import QdrantClient

from vector.domain.config import QdrantConfig


def create_qdrant_client(config: QdrantConfig, timeout: int, *, check_compatibility: bool = True) -> QdrantClient:
    """Create a gRPC-preferred client with the configured TLS and timeout."""
    with warnings.catch_warnings():
        if not config.tls and config.host.lower() in ("127.0.0.1", "localhost", "::1"):
            warnings.filterwarnings(
                "ignore", message=r"^Api key is used with an insecure connection\.$", category=UserWarning
            )
        return QdrantClient(
            host=config.host,
            port=config.http_port,
            grpc_port=config.grpc_port,
            https=config.tls,
            api_key=config.api_key,
            prefer_grpc=True,
            timeout=timeout,
            check_compatibility=check_compatibility,
            # Numeric vectors are already encoded; skip the SDK's local inference scans.
            cloud_inference=True,
        )
