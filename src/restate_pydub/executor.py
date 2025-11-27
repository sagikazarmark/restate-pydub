import logging
import tempfile
import typing
from pathlib import Path, PurePosixPath
from typing import Protocol, Sequence

from pydantic import AnyUrl, BaseModel, ConfigDict, Field
from pydub import AudioSegment

_logger = logging.getLogger(__name__)


class Input(BaseModel):
    source: AnyUrl | PurePosixPath = Field(
        description="The audio file to export",
        union_mode="left_to_right",  # This is important to keep best match order (TODO: consider using a custom discriminator)
    )

    format: str | None = Field(
        default=None,
        description="Format of the audio file (falls back extension of the file)",
    )


class ExportOptions(BaseModel):
    format: str | None = Field(
        default=None,
        description="Format for destination audio file (falls back to the input format)",
    )

    codec: str | None = Field(
        default=None,
        description="Codec used to encode the destination file",
    )

    bitrate: str | None = Field(
        default=None,
        description="Bitrate used when encoding destination file",
    )

    parameters: Sequence[str] | None = Field(
        default=None,
        description="Aditional ffmpeg/avconv parameters",
    )

    tags: dict[str, str] | None = Field(
        default=None,
        description="Set metadata information to destination files",
    )

    id3v2_version: str | None = Field(
        default=None,
        description="Set ID3v2 version for tags",
    )


class Output(BaseModel):
    destination: AnyUrl | PurePosixPath = Field(
        description="The destination of the exported audio file",
        union_mode="left_to_right",  # This is important to keep best match order (TODO: consider using a custom discriminator)
    )

    export: ExportOptions | None = Field(
        default=None,
        description="Export options",
    )


class ExportRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "input": {
                        "source": "s3://bucket/audio.wav",
                    },
                    "output": {
                        "destination": "s3://bucket/audio.mp3",
                    },
                },
            ]
        }
    )

    input: Input
    output: Output


class Segment(BaseModel):
    start: float = Field(description="The start time of the segment in seconds")
    end: float = Field(description="The end time of the segment in seconds")
    name: str = Field(description="The final file name")


class SegmentRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "input": {
                        "source": "s3://bucket/audio.wav",
                    },
                    "output": {
                        "destination": "s3://bucket/segments/",
                    },
                    "segments": [
                        {"start": 0, "end": 10, "name": "segment1.mp3"},
                        {"start": 10, "end": 20, "name": "segment2.mp3"},
                    ],
                },
            ]
        }
    )

    input: Input
    segments: Sequence[Segment]
    output: Output


class Loader(typing.Protocol):
    def load(self, ref: AnyUrl | PurePosixPath, dst: Path): ...


class Persister(Protocol):
    def persist(
        self,
        ref: AnyUrl | PurePosixPath,
        src: Path,
    ): ...


class Executor:
    def __init__(
        self,
        loader: Loader,
        persister: Persister,
        logger: logging.Logger = _logger,
    ):
        self.loader = loader
        self.persister = persister
        self.logger = logger

    def export(self, request: ExportRequest):
        self.logger.info(
            "Exporting audio",
            extra={"source": request.input.source, "format": request.input.format},
        )

        format = request.input.format
        if not format:
            format = _get_extension(request.input.source)

            self.logger.info(
                "Detecting input format",
                extra={"source": request.input.source, "format": format},
            )

        with tempfile.NamedTemporaryFile(delete=True) as sourceFile:
            self.loader.load(request.input.source, Path(sourceFile.name))

            audio: AudioSegment = AudioSegment.from_file(sourceFile.name, format)

            self._export(audio, request.output)

    def segment(self, request: SegmentRequest):
        self.logger.info(
            "Segmenting audio",
            extra={"source": request.input.source, "format": request.input.format},
        )

        format = request.input.format
        if not format:
            format = _get_extension(request.input.source)

            self.logger.info(
                "Detecting input format",
                extra={"source": request.input.source, "format": format},
            )

        with tempfile.NamedTemporaryFile(delete=True) as sourceFile:
            self.loader.load(request.input.source, Path(sourceFile.name))

            audio: AudioSegment = AudioSegment.from_file(sourceFile.name, format)

            for segment in request.segments:
                start_ms = int(segment.start * 1000)
                end_ms = int(segment.end * 1000)

                audioSegment = audio[start_ms:end_ms]

                self.logger.info(
                    "Exporting audio segment",
                    extra=segment.model_dump(),
                )

                self._export(audioSegment, request.output, segment.name)  # pyright: ignore[reportArgumentType]

    def _export(
        self,
        audio: AudioSegment,
        output: Output,
        name: str | None = None,
    ):
        exportOptions = output.export
        exportArgs = {}

        destination = output.destination

        if name:
            destination = _append_path(destination, name)

        if exportOptions:
            if not exportOptions.format:
                exportOptions.format = _get_extension(destination)

                self.logger.info(
                    "Detecting output format",
                    extra={
                        "destination": output.destination,
                        "format": exportOptions.format,
                    },
                )

            exportArgs = exportOptions.model_dump(exclude_none=True)

        with tempfile.NamedTemporaryFile(delete=True) as exportedFile:
            self.logger.debug(
                "Exporting audio starts",
                extra={
                    "file": exportedFile.name,
                    "options": exportArgs,
                },
            )

            audio.export(exportedFile.name, **exportArgs)

            self.logger.info(
                "Exporting audio completed",
                extra={"file": exportedFile.name},
            )

            self.persister.persist(destination, Path(exportedFile.name))


def _get_extension(ref: AnyUrl | PurePosixPath) -> str:
    if isinstance(ref, PurePosixPath):
        return ref.suffix.lstrip(".")

    if not ref.path:
        raise ValueError("Cannot determine format from empty path")

    return PurePosixPath(ref.path).suffix.lstrip(".")


def _append_path(ref: AnyUrl | PurePosixPath, name: str) -> AnyUrl | PurePosixPath:
    if isinstance(ref, PurePosixPath):
        return ref / name

    return ref.build(
        scheme=ref.scheme,
        username=ref.username,
        password=ref.password,
        host=str(ref.host),
        port=ref.port,
        path=str(PurePosixPath(str(ref.path)) / name).lstrip("/"),
        query=ref.query,
        fragment=ref.fragment,
    )
