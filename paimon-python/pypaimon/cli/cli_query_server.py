#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""CLI subcommands for managing the Paimon query server."""

import logging
import os
import signal
import sys

_PID_FILE = os.path.expanduser("~/.paimon/query-server.pid")


def _ensure_pid_dir():
    os.makedirs(os.path.dirname(_PID_FILE), exist_ok=True)


def _read_pid() -> int | None:
    """Read the stored PID, returning *None* if the file is missing or stale."""
    try:
        with open(_PID_FILE) as f:
            pid = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None
    # Check whether the process is still alive.
    try:
        os.kill(pid, 0)
    except OSError:
        os.remove(_PID_FILE)
        return None
    return pid


def cmd_start(args):
    """Start the query server."""
    try:
        import uvicorn  # noqa: F401
    except ImportError:
        print(
            "Error: query-server extras not installed.\n"
            "Run: pip install 'pypaimon[query-server]'",
            file=sys.stderr,
        )
        sys.exit(1)

    existing = _read_pid()
    if existing is not None:
        print(f"Query server is already running (PID {existing}).")
        sys.exit(0)

    host = args.host
    port = args.port

    if args.daemon:
        # Fork into background.
        pid = os.fork()
        if pid > 0:
            # Parent – wait briefly then verify the child is alive.
            import time
            time.sleep(0.5)
            if _read_pid() is not None:
                print(f"Query server started on {host}:{port} (PID {pid}).")
            else:
                print(f"Query server started (PID {pid}).")
            sys.exit(0)

        # Child – detach from terminal.
        os.setsid()
        # Redirect stdio to /dev/null so the process doesn't hang.
        devnull = os.open(os.devnull, os.O_RDWR)
        os.dup2(devnull, 0)
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        os.close(devnull)

    # Configure logging for pypaimon modules.
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress noisy third-party loggers at DEBUG level.
    if level == logging.DEBUG:
        logging.getLogger("urllib3").setLevel(logging.INFO)
        logging.getLogger("httpcore").setLevel(logging.INFO)

    _ensure_pid_dir()
    with open(_PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    try:
        import uvicorn
        uvicorn.run(
            "pypaimon.query_server.app:app",
            host=host,
            port=port,
            log_level="debug" if args.verbose else "info",
        )
    finally:
        try:
            os.remove(_PID_FILE)
        except FileNotFoundError:
            pass


def cmd_stop(args):
    """Stop a running query server."""
    pid = _read_pid()
    if pid is None:
        print("Query server is not running.")
        sys.exit(0)

    os.kill(pid, signal.SIGTERM)
    # Wait for the process to exit.
    import time
    for _ in range(20):
        time.sleep(0.25)
        if _read_pid() is None:
            print(f"Query server (PID {pid}) stopped.")
            return
    print(f"Query server (PID {pid}) did not stop in time; sending SIGKILL.")
    os.kill(pid, signal.SIGKILL)
    try:
        os.remove(_PID_FILE)
    except FileNotFoundError:
        pass


def cmd_status(args):
    """Show query server status."""
    pid = _read_pid()
    if pid is None:
        print("Query server is not running.")
    else:
        print(f"Query server is running (PID {pid}).")


def add_query_server_subcommands(parser):
    """Register query-server subcommands on *parser*."""
    sub = parser.add_subparsers(dest="qs_command", help="Query server commands")

    # start
    start_p = sub.add_parser("start", help="Start the query server")
    start_p.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    start_p.add_argument("--port", "-p", type=int, default=8187, help="Bind port (default: 8187)")
    start_p.add_argument("--daemon", "-d", action="store_true", help="Run in the background")
    start_p.add_argument("--verbose", "-v", action="store_true", help="Enable DEBUG logging")
    start_p.set_defaults(func=cmd_start)

    # stop
    stop_p = sub.add_parser("stop", help="Stop a running query server")
    stop_p.set_defaults(func=cmd_stop)

    # status
    status_p = sub.add_parser("status", help="Show query server status")
    status_p.set_defaults(func=cmd_status)
