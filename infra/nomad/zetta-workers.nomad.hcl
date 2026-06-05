job "zetta-workers" {
  datacenters = ["dc1"]
  type        = "service"

  group "collectors" {
    count = 3

    network {
      port "api" {
        static = 8088
      }
    }

    task "worker" {
      driver = "raw_exec"

      config {
        command = "/opt/zetta/.venv/bin/python"
        args = [
          "-m",
          "zetta.cli",
          "--task-store",
          "postgres",
          "--node-id",
          "${node.unique.name}-${NOMAD_ALLOC_INDEX}",
          "--postgres-dsn",
          "${ZETTA_POSTGRES_DSN}",
          "--raw-data-dir",
          "/var/lib/zetta/raw",
          "--state-dir",
          "/var/lib/zetta/state",
          "tasks",
          "run-loop",
          "--idle-sleep-seconds",
          "5"
        ]
      }

      env {
        PYTHONPATH         = "/opt/zetta/src"
        ZETTA_POSTGRES_DSN = "postgresql://zetta:zetta@postgres.service.consul:5432/zetta"
      }

      resources {
        cpu    = 500
        memory = 512
      }

      restart {
        attempts = 10
        interval = "10m"
        delay    = "15s"
        mode     = "delay"
      }
    }
  }

  group "api" {
    count = 1

    network {
      port "http" {
        static = 8088
      }
    }

    task "product-api" {
      driver = "raw_exec"

      config {
        command = "/opt/zetta/.venv/bin/python"
        args = [
          "-m",
          "zetta.cli",
          "--clickhouse-host",
          "clickhouse.service.consul",
          "--postgres-dsn",
          "${ZETTA_POSTGRES_DSN}",
          "api",
          "serve",
          "--host",
          "0.0.0.0",
          "--port",
          "${NOMAD_PORT_http}"
        ]
      }

      env {
        PYTHONPATH         = "/opt/zetta/src"
        ZETTA_POSTGRES_DSN = "postgresql://zetta:zetta@postgres.service.consul:5432/zetta"
      }

      resources {
        cpu    = 300
        memory = 256
      }
    }
  }
}
