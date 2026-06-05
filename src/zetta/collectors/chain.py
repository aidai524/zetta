from __future__ import annotations

from dataclasses import dataclass

from zetta.chain.rpc import PolygonRpcClient
from zetta.storage.raw import RawJsonlWriter


@dataclass(frozen=True)
class ChainLogsCollectionResult:
    from_block: int
    to_block: int
    logs: int
    output_path: str


class ChainCollector:
    def __init__(self, *, client: PolygonRpcClient, raw_writer: RawJsonlWriter) -> None:
        self.client = client
        self.raw_writer = raw_writer

    def collect_logs(
        self,
        *,
        from_block: int,
        to_block: int,
        addresses: list[str] | None = None,
        topics: list[str] | None = None,
    ) -> ChainLogsCollectionResult:
        logs = self.client.get_logs(
            from_block=from_block,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )
        output_path = self.raw_writer.write(
            source="polygon",
            entity="logs",
            request_url=self.client.settings.polygon_rpc_url,
            payload={
                "from_block": from_block,
                "to_block": to_block,
                "addresses": addresses or [],
                "topics": topics or [],
                "logs": logs,
            },
        )
        return ChainLogsCollectionResult(
            from_block=from_block,
            to_block=to_block,
            logs=len(logs),
            output_path=str(output_path),
        )
