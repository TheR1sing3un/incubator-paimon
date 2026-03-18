# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Manages the lifecycle of a real Java REST Catalog Server process for E2E testing."""

import logging
import os
import re
import signal
import subprocess
import threading
import time

import requests

logger = logging.getLogger(__name__)

# Project root: paimon-python/pypaimon/tests/e2e_rest/server_manager.py -> paimon/
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "..", ".."))

_DEFAULT_JAR_GLOB = os.path.join(
    _PROJECT_ROOT,
    "paimon-rest-server", "target", "paimon-rest-server-*-server.jar"
)


def _find_default_jar():
    """Find the REST server shaded JAR in the Maven target directory."""
    import glob as _glob
    jars = sorted(_glob.glob(_DEFAULT_JAR_GLOB))
    if jars:
        return jars[-1]
    return None


class RESTServerManager:
    """Manages the lifecycle of a real Java REST Catalog Server subprocess."""

    def __init__(self, warehouse_path, prefix="e2e-test", jar_path=None):
        self.warehouse_path = os.path.abspath(warehouse_path)
        self.prefix = prefix
        self.jar_path = jar_path or os.environ.get("PAIMON_REST_SERVER_JAR") or _find_default_jar()
        self.process = None
        self.port = None
        self.uri = None
        self._output_lines = []
        self._reader_thread = None
        self._port_event = threading.Event()

    def start(self, timeout=30):
        """Start the Java REST server and wait until it is ready.

        Args:
            timeout: Maximum seconds to wait for server startup.

        Raises:
            FileNotFoundError: If the server JAR cannot be found.
            RuntimeError: If the server fails to start within timeout.
        """
        if self.jar_path is None or not os.path.isfile(self.jar_path):
            raise FileNotFoundError(
                f"REST server JAR not found at: {self.jar_path}\n"
                f"Build it first: mvn package -pl paimon-rest-server -am -DskipTests\n"
                f"Or set PAIMON_REST_SERVER_JAR environment variable."
            )

        os.makedirs(self.warehouse_path, exist_ok=True)

        cmd = [
            "java", "-jar", self.jar_path,
            "--warehouse", self.warehouse_path,
            "--metastore", "filesystem",
            "--rest-server.port", "0",
            "--rest-server.prefix", self.prefix,
        ]
        logger.info("Starting REST server: %s", " ".join(cmd))

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
        )

        # Read output in a daemon thread to find the port
        self._reader_thread = threading.Thread(
            target=self._read_output, daemon=True
        )
        self._reader_thread.start()

        # Wait for port to be discovered
        if not self._port_event.wait(timeout=timeout):
            self.stop()
            output = "\n".join(self._output_lines[-50:])
            raise RuntimeError(
                f"REST server did not start within {timeout}s.\n"
                f"Last output:\n{output}"
            )

        self.uri = f"http://localhost:{self.port}"
        logger.info("REST server started at %s", self.uri)

        # Health check
        self._wait_for_health(retries=5, interval=1.0)

    def stop(self):
        """Stop the Java REST server process."""
        if self.process is None:
            return

        logger.info("Stopping REST server (pid=%d)", self.process.pid)
        try:
            self.process.send_signal(signal.SIGTERM)
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("Server did not stop gracefully, sending SIGKILL")
            self.process.kill()
            self.process.wait(timeout=5)
        except Exception as e:
            logger.warning("Error stopping server: %s", e)
        finally:
            self.process = None
            self.port = None
            self.uri = None

    @property
    def catalog_options(self):
        """Return options dict suitable for CatalogFactory.create()."""
        if self.uri is None:
            raise RuntimeError("Server is not started")
        return {
            "metastore": "rest",
            "uri": self.uri,
            "warehouse": self.warehouse_path,
            "token.provider": "bear",
            "token": "e2e-test-token",
        }

    def _read_output(self):
        """Background thread: read process output and detect port."""
        port_pattern = re.compile(r"REST Catalog Server started on .*:(\d+)")
        try:
            for line in self.process.stdout:
                line = line.rstrip("\n")
                self._output_lines.append(line)
                logger.debug("[REST-SERVER] %s", line)

                if not self._port_event.is_set():
                    m = port_pattern.search(line)
                    if m:
                        self.port = int(m.group(1))
                        self._port_event.set()
        except Exception:
            pass

    def _wait_for_health(self, retries=5, interval=1.0):
        """Wait for the server health check endpoint to respond."""
        url = f"{self.uri}/v1/config"
        for i in range(retries):
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    logger.info("Health check passed: %s", url)
                    return
            except requests.ConnectionError:
                pass
            if i < retries - 1:
                time.sleep(interval)

        self.stop()
        output = "\n".join(self._output_lines[-30:])
        raise RuntimeError(
            f"Health check failed after {retries} attempts at {url}\n"
            f"Last output:\n{output}"
        )
