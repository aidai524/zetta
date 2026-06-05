import json
import subprocess

from zetta.storage.redpanda import RpkPublisher


class FakeRunner:
    def __init__(self, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls = []

    def __call__(self, args, **kwargs):
        self.calls.append((args, kwargs))
        return subprocess.CompletedProcess(
            args=args,
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


def test_rpk_publisher_builds_base_command() -> None:
    publisher = RpkPublisher(service="rp", compose_file="compose.test.yml", runner=FakeRunner())

    assert publisher.base_command() == [
        "docker",
        "compose",
        "-f",
        "compose.test.yml",
        "exec",
        "-T",
        "rp",
    ]


def test_rpk_publisher_create_topic_is_idempotent() -> None:
    runner = FakeRunner(returncode=1, stdout="TOPIC_ALREADY_EXISTS: already been created")
    publisher = RpkPublisher(runner=runner)

    assert not publisher.create_topic("topic-1")


def test_rpk_publisher_publishes_json_lines() -> None:
    runner = FakeRunner()
    publisher = RpkPublisher(runner=runner)

    result = publisher.publish_json(topic="topic-1", messages=[{"a": 1}, {"b": "two"}])

    assert result.messages == 2
    args, kwargs = runner.calls[0]
    assert args[-3:] == ["topic", "produce", "topic-1"]
    lines = kwargs["input"].splitlines()
    assert [json.loads(line) for line in lines] == [{"a": 1}, {"b": "two"}]
