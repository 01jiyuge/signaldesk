"""One Bearer token check shared by business APIs and protocol transports."""

import secrets

from fastapi import HTTPException


def authenticate_bearer(expected_token: str, scheme: str, supplied_token: str) -> None:
    if not expected_token:
        return
    matched = False
    if scheme.lower() == "bearer":
        try:
            supplied = supplied_token.encode("ascii")
            expected = expected_token.encode("ascii")
        except UnicodeError:
            # Bearer credentials are ASCII. Malformed wire bytes must remain an
            # authentication failure, rather than compare_digest(str)'s 500.
            pass
        else:
            matched = secrets.compare_digest(supplied, expected)
    if not matched:
        raise HTTPException(
            401, "需要有效的 Bearer API token。", headers={"WWW-Authenticate": "Bearer"}
        )
