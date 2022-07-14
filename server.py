#! /Users/ealmina/.pyenv/shims/python
import asyncio
import logging
import socket
import sqlite3
import subprocess
from asyncio import StreamReader, StreamWriter
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

HOST = "127.0.0.1"
PORT = 7000

BLOCKED = ("stackoverflow.com", "stackexchange.com")
FILE = "/Users/ealmina/Documents/proxy/stat.sqlite"

SQL_CREATE_WEB_STAT = "CREATE TABLE IF NOT EXISTS web_stat (id integer PRIMARY KEY, site TEXT, visited DATETIME)"
SQL_CREATE_WEB_STAT_BLOCKED = "CREATE TABLE IF NOT EXISTS web_stat_blocked (id integer PRIMARY KEY, site TEXT, visited DATETIME)"
SQL_INSERT_WEB_STAT = "INSERT INTO web_stat (site, visited) VALUES ('{url}', '{time}')"
SQL_INSERT_WEB_STAT_BLOCKED = (
    "INSERT INTO web_stat_blocked (site, visited) VALUES ('{url}', '{time}')"
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@dataclass(frozen=True, slots=True)
class Config:
    host: str
    port: int


def parse_request(data: bytes) -> Optional[Config]:
    request_data = data.decode().split()
    method, url = request_data[0:2]
    host, port = url.split(":")
    minutes = datetime.now().minute
    if host in BLOCKED and minutes < 45:
        add_visit_stat(
            SQL_INSERT_WEB_STAT_BLOCKED.format(url=host, time=datetime.now())
        )
        return
    if method == "CONNECT":
        return Config(host, int(port))


def init_db() -> None:
    conn = sqlite3.connect(FILE)
    conn.execute(SQL_CREATE_WEB_STAT)
    conn.execute(SQL_CREATE_WEB_STAT_BLOCKED)
    conn.commit()
    conn.close()


def add_visit_stat(sql: str) -> None:
    conn = sqlite3.connect(FILE)
    conn.execute(sql)
    conn.commit()
    conn.close()


async def forward_data(writer: StreamWriter, reader: StreamReader) -> None:
    with closing(writer):
        while True:
            data = await reader.read(1024)

            if data == b"":
                break

            writer.write(data)
            await writer.drain()


async def start_forwarding(
    reader: StreamReader,
    writer: StreamWriter,
    remote_reader: StreamReader,
    remote_writer: StreamWriter,
) -> None:
    with closing(remote_writer):
        writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        await writer.drain()
        await asyncio.gather(
            forward_data(remote_writer, reader),
            forward_data(writer, remote_reader),
        )


async def handle_request(reader: StreamReader, writer: StreamWriter) -> None:
    data = await reader.readuntil(b"\r\n\r\n")
    config = parse_request(data)
    if config:
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                config.host, config.port
            )
            await start_forwarding(reader, writer, remote_reader, remote_writer)
            await asyncio.to_thread(
                add_visit_stat,
                SQL_INSERT_WEB_STAT.format(url=config.host, time=datetime.now()),
            )
        except (
            ConnectionRefusedError,
            ConnectionResetError,
            socket.gaierror,
            TimeoutError,
        ) as e:
            logger.warning(e.strerror)
    else:
        writer.write(b"Blocked \r\n\r\n")
        await writer.drain()


async def main():
    try:
        subprocess.run(["networksetup -setsecurewebproxystate wi-fi on"], shell=True)
        init_db()
        server = await asyncio.start_server(handle_request, host=HOST, port=PORT)
        async with server:
            await server.serve_forever()
    except KeyboardInterrupt:
        subprocess.run(["networksetup -setsecurewebproxystate wi-fi off"], shell=True)
    finally:
        subprocess.run(["networksetup -setsecurewebproxystate wi-fi off"], shell=True)


asyncio.run(main())
