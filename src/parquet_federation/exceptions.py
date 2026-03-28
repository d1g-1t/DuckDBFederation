from __future__ import annotations


class FederationError(Exception):
    pass


class QueryTimeoutError(FederationError):
    pass


class UnsupportedSourceTypeError(FederationError):
    def __init__(self, source_type: str) -> None:
        super().__init__(f"Unsupported source type: {source_type}")


class QueryValidationError(FederationError):
    pass


class SourceConnectionError(FederationError):
    pass
