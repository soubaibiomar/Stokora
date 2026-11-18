"""Allow offline Spark children to call ephemeral loopback endpoints only (Linux/amd64)."""
import ctypes
import os
import platform
import struct
import sys
from pathlib import Path


def restrict():
    libc = ctypes.CDLL(None, use_errno=True)
    def checked(result):
        if result < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        return result

    if platform.machine() != "x86_64" or checked(libc.syscall(444, 0, 0, 1)) < 4:
        raise RuntimeError("Offline jobs require Linux/amd64 Landlock network ABI 4 or later")
    # Spark uses loopback. Spark, Py4J and Python workers discover
    # each other on kernel-assigned ports; fixed service ports remain denied.
    first, last = map(int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split())
    if any(first <= port <= last for port in (5432, 8083, 9092, 9093)):
        raise RuntimeError("Ephemeral range overlaps a service listener")
    connect_tcp = 2
    attr = struct.pack("=QQ", 0, connect_tcp)
    fd = checked(libc.syscall(444, attr, len(attr), 0))
    try:
        for port in range(first, last + 1):
            checked(libc.syscall(445, fd, 2, struct.pack("=QQ", connect_tcp, port), 0))
        checked(libc.prctl(38, 1, 0, 0, 0))
        checked(libc.syscall(446, fd, 0))
    finally:
        os.close(fd)


if __name__ == "__main__":
    restrict()
    os.execvp(sys.argv[1], sys.argv[1:])
