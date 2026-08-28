"""
Source file immutability verification via SHA-256 checksums.

Architecture v0.3 §2, Principle 1:
  "Source immutability. Bronze records are never modified after ingestion.
   Bronze is append-only."

This module provides checksum computation and verification to guarantee
that source files remain unmodified throughout the pipeline lifecycle.
"""

import hashlib
from pathlib import Path


def compute_sha256(filepath: Path, chunk_size: int = 65536) -> str:
    """
    Compute SHA-256 hash of a file.

    Args:
        filepath: Path to the file to hash.
        chunk_size: Read buffer size in bytes.

    Returns:
        Lowercase hex string of the SHA-256 digest.
    """
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def verify_immutability(filepath: Path, expected_checksum: str) -> bool:
    """
    Verify that a source file has not been modified since ingestion.

    Args:
        filepath: Path to the source file.
        expected_checksum: SHA-256 hex digest recorded at ingestion time.

    Returns:
        True if the file's current checksum matches the expected value.
    """
    actual = compute_sha256(filepath)
    return actual == expected_checksum
