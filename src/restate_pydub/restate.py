import restate

from .executor import (
    Executor,
    ExportRequest,
    SegmentRequest,
)


def create_service(
    downloader: Executor,
    service_name: str = "Pydub",
) -> restate.Service:
    service = restate.Service(service_name)

    register_service(downloader, service)

    return service


def register_service(
    executor: Executor,
    service: restate.Service,
):
    @service.handler()
    async def export(
        ctx: restate.Context,
        request: ExportRequest,
    ):
        """Export an audio file."""

        return await ctx.run_typed(
            "export",
            executor.export,
            request=request,
        )

    @service.handler()
    async def segment(
        ctx: restate.Context,
        request: SegmentRequest,
    ):
        """Segment an audio file."""

        return await ctx.run_typed(
            "segment",
            executor.segment,
            request=request,
        )
