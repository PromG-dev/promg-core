class BatchQueryExecutionError(Exception):
    """Raised when batch query retries are exhausted without success."""
    pass