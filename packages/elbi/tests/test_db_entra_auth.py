"""The store can sign in to Azure Postgres with a managed identity, not a password.

An Azure Database for PostgreSQL server with password sign-in turned off accepts only an
Entra access token as the password. Tokens last about an hour, so a token baked into
``DB_URI`` at start would lock the app out of every new connection after that. With
``DB_AUTH=azure-managed-identity`` the store fetches a fresh token each time it opens a
connection, and the URI carries no password at all.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import create_engine

from elbi.db import apply_store_auth, install_entra_auth, open_store

PG_URL = "postgresql+psycopg://ids-id-example@pg.example.postgres.database.azure.com:5432/elbi"


def _tokens() -> Any:
    issued: list[str] = []

    def get_token() -> str:
        issued.append(f"token-{len(issued) + 1}")
        return issued[-1]

    get_token.issued = issued  # type: ignore[attr-defined]
    return get_token


def test_each_new_connection_gets_a_fresh_token_as_its_password() -> None:
    get_token = _tokens()
    hook = install_entra_auth(create_engine(PG_URL), get_token)

    first: dict[str, Any] = {}
    second: dict[str, Any] = {}
    hook(None, None, [], first)
    hook(None, None, [], second)

    assert first["password"] == "token-1"
    assert second["password"] == "token-2"
    assert get_token.issued == ["token-1", "token-2"]


def test_the_uri_never_carries_the_token() -> None:
    engine = create_engine(PG_URL)
    install_entra_auth(engine, _tokens())

    assert engine.url.password is None


def test_without_db_auth_the_store_connects_as_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DB_AUTH", raising=False)

    assert apply_store_auth(create_engine(PG_URL)) is None


def test_managed_identity_sign_in_on_sqlite_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("DB_AUTH", "azure-managed-identity")

    with pytest.raises(ValueError, match="Postgres"):
        open_store(f"sqlite:{tmp_path / 'app.db'}")


def test_an_unknown_db_auth_value_is_refused_by_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_AUTH", "azure-password")

    with pytest.raises(ValueError, match="azure-managed-identity"):
        apply_store_auth(create_engine(PG_URL))


def test_db_auth_on_postgres_installs_the_token_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_AUTH", "azure-managed-identity")
    get_token = _tokens()
    monkeypatch.setattr("elbi.db._managed_identity_token", get_token)

    hook = apply_store_auth(create_engine(PG_URL))
    assert hook is not None
    params: dict[str, Any] = {}
    hook(None, None, [], params)

    assert params["password"] == "token-1"
