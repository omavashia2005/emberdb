#!/usr/bin/env python3
"""Serve benchmark results and one selected Go 1.25 trace viewer."""

import errno
import functools
import json
import pathlib
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler


class BenchmarkServer(HTTPServer):
    viewer = None
    viewer_url = None
    viewer_image = "emberdb-trace-viewer:go1.25"
    image_ready = False

    def stop_viewer(self):
        if self.viewer:
            subprocess.run(["docker", "stop", self.viewer], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.viewer = None
            self.viewer_url = None

    def open_trace(self, filename):
        self.stop_viewer()
        if not self.image_ready:
            subprocess.run(
                ["docker", "build", "-q", "-t", self.viewer_image, "-"],
                input="FROM golang:1.25-alpine\nRUN apk add --no-cache graphviz\n",
                capture_output=True, text=True, check=True,
            )
            self.image_ready = True
        result = subprocess.run(
            ["docker", "run", "-d", "--rm", "-p", "127.0.0.1::7070",
             "-v", f"{self.directory}:/results:ro", self.viewer_image,
             "go", "tool", "trace", "-http=0.0.0.0:7070", f"/results/{filename}"],
            capture_output=True, text=True, check=True,
        )
        self.viewer = result.stdout.strip()
        try:
            mapped = subprocess.run(
                ["docker", "port", self.viewer, "7070/tcp"],
                capture_output=True, text=True, check=True,
            ).stdout.strip().splitlines()[0]
            self.viewer_url = f"http://{mapped}/"
        except (subprocess.CalledProcessError, IndexError):
            self.stop_viewer()
            raise
        for _ in range(80):
            try:
                with urllib.request.urlopen(self.viewer_url, timeout=1):
                    return
            except OSError:
                time.sleep(0.25)
        self.stop_viewer()
        raise RuntimeError("trace viewer did not start")


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlsplit(self.path)
        if url.path != "/trace":
            return super().do_GET()
        filenames = urllib.parse.parse_qs(url.query).get("file", [])
        if len(filenames) != 1:
            return self.send_error(400, "expected one trace file")
        filename = filenames[0]
        try:
            index_path = self.server.directory / "traces/index.json"
            if not index_path.is_file():
                return self.send_error(404, "no traces available")
            index = json.loads(index_path.read_text())
            allowed = {entry["file"] for entry in index}
            path = (self.server.directory / filename).resolve()
            if filename not in allowed or not path.is_file() or path.suffix != ".trace":
                return self.send_error(404, "trace not found")
            try:
                path.relative_to(self.server.directory)
            except ValueError:
                return self.send_error(404, "trace not found")
            self.server.open_trace(filename)
        except subprocess.CalledProcessError as exc:
            return self.send_error(500, f"trace viewer failed: {(exc.stderr or str(exc)).strip()}")
        except (OSError, ValueError, KeyError, TypeError, IndexError, RuntimeError) as exc:
            return self.send_error(500, f"trace viewer failed: {exc}")
        self.send_response(302)
        self.send_header("Location", self.server.viewer_url)
        self.end_headers()


def bind_server(directory, port):
    handler = functools.partial(Handler, directory=str(directory))
    try:
        server = BenchmarkServer(("127.0.0.1", port), handler)
    except OSError as exc:
        if exc.errno != errno.EADDRINUSE:
            raise
        server = BenchmarkServer(("127.0.0.1", 0), handler)
    server.directory = directory
    return server


def main():
    directory = pathlib.Path(sys.argv[1]).resolve()
    server = bind_server(directory, int(sys.argv[2]))
    url = f"http://127.0.0.1:{server.server_address[1]}/"
    print(f"Dashboard: {url} (Ctrl+C to stop)", flush=True)
    opener = threading.Timer(0.5, webbrowser.open, args=(url,))
    opener.daemon = True
    opener.start()

    def stop(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop_viewer()
        server.server_close()


if __name__ == "__main__":
    main()
