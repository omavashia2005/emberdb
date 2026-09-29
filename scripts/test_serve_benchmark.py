#!/usr/bin/env python3
"""Run with python3 scripts/test_serve_benchmark.py."""

import importlib.util
import errno
import json
import pathlib
import sys
import tempfile
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch


sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("serve_benchmark", pathlib.Path(__file__).with_name("serve-benchmark.py"))
server_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server_module)


def main():
    with tempfile.TemporaryDirectory() as temp:
        root = pathlib.Path(temp, "results").resolve()
        (root / "traces").mkdir(parents=True)
        (root / "traces/node1.trace").write_bytes(b"trace")
        (root / "traces/unlisted.trace").write_bytes(b"trace")
        (root.parent / "outside.trace").write_bytes(b"trace")
        (root / "traces/index.json").write_text(json.dumps([
            {"file": "traces/node1.trace"}, {"file": "../outside.trace"},
        ]))
        server = server_module.BenchmarkServer.__new__(server_module.BenchmarkServer)
        server.directory = root
        server.viewer = None
        server.viewer_url = None
        server.image_ready = False

        def request(filename):
            handler = server_module.Handler.__new__(server_module.Handler)
            handler.path = f"/trace?file={filename}"
            handler.server = server
            response = SimpleNamespace(status=None, headers={})
            handler.send_error = lambda code, *args: setattr(response, "status", code)
            handler.send_response = lambda code: setattr(response, "status", code)
            handler.send_header = lambda key, value: response.headers.__setitem__(key, value)
            handler.end_headers = lambda: None
            handler.do_GET()
            return response

        def docker(command, **_):
            if command[1] == "run":
                return SimpleNamespace(stdout="container-id\n")
            if command[1] == "port":
                return SimpleNamespace(stdout="127.0.0.1:49123\n")
            return SimpleNamespace(stdout="")

        with patch.object(server_module.subprocess, "run", side_effect=docker) as run, \
             patch.object(server_module.urllib.request, "urlopen", return_value=nullcontext()):
            response = request("traces/node1.trace")
            assert response.status == 302
            assert response.headers["Location"] == "http://127.0.0.1:49123/"
            build = run.call_args_list[0]
            assert build.args[0] == ["docker", "build", "-q", "-t", "emberdb-trace-viewer:go1.25", "-"]
            assert "apk add --no-cache graphviz" in build.kwargs["input"]
            command = run.call_args_list[1].args[0]
            assert command == [
                "docker", "run", "-d", "--rm", "-p", "127.0.0.1::7070",
                "-v", f"{root}:/results:ro", "emberdb-trace-viewer:go1.25", "go", "tool", "trace",
                "-http=0.0.0.0:7070", "/results/traces/node1.trace",
            ]
            assert run.call_args_list[2].args[0] == ["docker", "port", "container-id", "7070/tcp"]
            assert request("traces/unlisted.trace").status == 404
            assert request("../outside.trace").status == 404
            assert run.call_count == 3
            server.stop_viewer()
            assert run.call_args.args[0] == ["docker", "stop", "container-id"]

        fallback = SimpleNamespace(directory=None)
        with patch.object(server_module, "BenchmarkServer", side_effect=[OSError(errno.EADDRINUSE, "busy"), fallback]) as factory:
            assert server_module.bind_server(root, 8080) is fallback
            assert factory.call_args_list[0].args[0] == ("127.0.0.1", 8080)
            assert factory.call_args_list[1].args[0] == ("127.0.0.1", 0)
            assert fallback.directory == root


if __name__ == "__main__":
    main()
