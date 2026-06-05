from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any, Protocol


class CommandRunner(Protocol):
    def __call__(
        self,
        args: list[str],
        *,
        input: str | None = None,
        text: bool = True,
        capture_output: bool = True,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        ...


@dataclass(frozen=True)
class PublishResult:
    topic: str
    messages: int


class RpkPublisher:
    def __init__(
        self,
        *,
        service: str = "redpanda",
        compose_file: str | None = None,
        runner: CommandRunner = subprocess.run,
    ) -> None:
        self.service = service
        self.compose_file = compose_file
        self.runner = runner

    def create_topic(self, topic: str) -> bool:
        result = self.runner(
            [
                *self.base_command(),
                "rpk",
                "topic",
                "create",
                topic,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        output = f"{result.stdout}\n{result.stderr}".lower()
        if result.returncode == 0:
            return True
        if "already exists" in output or "already been created" in output:
            return False
        raise RuntimeError(f"Failed to create Redpanda topic {topic}: {result.stderr}")

    def publish_json(self, *, topic: str, messages: list[dict[str, Any]]) -> PublishResult:
        if not messages:
            return PublishResult(topic=topic, messages=0)
        payload = "\n".join(
            json.dumps(message, ensure_ascii=False, separators=(",", ":")) for message in messages
        )
        payload = f"{payload}\n"
        self.runner(
            [
                *self.base_command(),
                "rpk",
                "topic",
                "produce",
                topic,
            ],
            input=payload,
            text=True,
            capture_output=True,
        )
        return PublishResult(topic=topic, messages=len(messages))

    def base_command(self) -> list[str]:
        command = ["docker", "compose"]
        if self.compose_file:
            command.extend(["-f", self.compose_file])
        command.extend(["exec", "-T", self.service])
        return command
