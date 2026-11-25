from __future__ import annotations

import hashlib


def stable_hash64(*parts: str) -> int:
    """Signed 64-bit key from text parts (ADR-0001).

    A fixed algorithm (BLAKE2b, 8-byte digest) rather than Polars `.hash()`,
    whose output is only guaranteed stable within one Polars version. Parts
    are joined with the ASCII unit separator so ("ab", "c") != ("a", "bc").
    """
    digest = hashlib.blake2b("\x1f".join(parts).encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)
