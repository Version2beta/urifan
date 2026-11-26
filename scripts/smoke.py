#!/usr/bin/env python3
"""Exercise a compiled CLI with files, UDP fan-out, and actual SIGTERM."""
import pathlib
import re
import selectors
import signal
import socket
import subprocess
import sys
import tempfile
import time

binary = str(pathlib.Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory(prefix="urifan-smoke-") as directory:
    directory = pathlib.Path(directory)
    payload = b"binary CLI smoke\x00\xff"
    source, output = directory / "input", directory / "output"
    source.write_bytes(payload)
    result = subprocess.run(
        [binary, "--source", source.as_uri(), "--destination", output.as_uri()],
        capture_output=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == b"" and output.read_bytes() == payload

receivers = []
process = None
try:
    arguments = [binary, "--source", "udp://127.0.0.1:0", "--packet-size", "1"]
    for _ in range(2):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receivers.append(receiver)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(5)
        arguments += ["--destination", "udp://127.0.0.1:" + str(receiver.getsockname()[1])]
    process = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    address = None
    with selectors.DefaultSelector() as selector:
        selector.register(process.stderr, selectors.EVENT_READ)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if not selector.select(0.1):
                continue
            line = process.stderr.readline().decode()
            if not line:
                raise RuntimeError("CLI exited before readiness")
            if line.startswith("source_connected"):
                address = re.search(r'address="([^"]+)"', line).group(1)
                break
    assert address, "CLI did not report its bound source address"
    host, port = address.rsplit(":", 1)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
        for payload in (b"abc", b"", b"larger than stream packet-size"):
            sender.sendto(payload, (host, int(port)))
            for receiver in receivers:
                assert receiver.recvfrom(65535)[0] == payload
    process.send_signal(signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0, stderr
    assert stdout == b""
finally:
    if process is not None and process.poll() is None:
        process.kill()
        process.wait()
    for receiver in receivers:
        receiver.close()
print("Compiled CLI: file bytes, UDP fan-out/empty packets, and SIGTERM passed")
