from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

import obstore
import pydantic_obstore
import restate
import structlog
import workstate
import workstate.obstore
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .restate_pydub import Executor, create_service

if TYPE_CHECKING:
    from obstore.store import ClientConfig


class ObstoreSettings(pydantic_obstore.Config):
    url: str | None = None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")  # pyright: ignore[reportUnannotatedClassAttribute]

    obstore: ObstoreSettings = Field(default_factory=ObstoreSettings)

    service_name: str = "Pydub"

    identity_keys: list[str] = Field(alias="restate_identity_keys", default=[])


settings = Settings()  # pyright: ignore[reportCallIssue]

# logging.basicConfig(level=logging.INFO)
structlog.stdlib.recreate_defaults(log_level=logging.INFO)

store: obstore.store.ObjectStore | None = None
client_options: ClientConfig | None = None

if settings.obstore.client_options:
    client_options = cast(
        "ClientConfig",
        settings.obstore.client_options.model_dump(exclude_none=True),
    )

if settings.obstore.url:
    store = obstore.store.from_url(settings.obstore.url, client_options=client_options)

loader = workstate.obstore.FileLoader(
    store,
    client_options=client_options,
    logger=structlog.get_logger("workstate"),
)

persister = workstate.obstore.FilePersister(
    store,
    client_options=client_options,
    logger=structlog.get_logger("workstate"),
)

executor = Executor(
    loader,
    persister,
    logger=structlog.get_logger("pydub"),
)

service = create_service(executor, service_name=settings.service_name)

app = restate.app(services=[service], identity_keys=settings.identity_keys)
