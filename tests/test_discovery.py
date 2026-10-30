"""Тесты обнаружения хаба (WebSub §4): Link-заголовки и разметка страницы/ленты."""
from __future__ import annotations

import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from hubd.discovery import discover, from_body, from_headers


class FakeHeaders:
    """Объект с get_all — как у urllib-ответа."""

    def __init__(self, links):
        self._links = links

    def get_all(self, name):
        return self._links if name == "Link" else None


class DiscoveryTestCase(unittest.TestCase):
    def test_link_header(self):
        hubs, selfs = from_headers(FakeHeaders([
            '<http://hub.example/>; rel="hub"',
            '<http://src.example/page>; rel=self',
        ]))
        self.assertEqual(hubs, ["http://hub.example/"])
        self.assertEqual(selfs, ["http://src.example/page"])

    def test_link_header_unquoted_and_multiple_rels(self):
        hubs, _ = from_headers({"Link": '<http://h.example/>; rel=hub, '
                                         '<http://s.example/>; rel="self hub"'})
        self.assertEqual(hubs, ["http://h.example/", "http://s.example/"])

    def test_body_html_and_atom_link(self):
        body = (b'<html><head><link rel="hub" href="http://hub.example/">'
                b'<atom:link rel="self" href="http://src.example/" /></head></html>')
        hubs, selfs = from_body(body)
        self.assertEqual(hubs, ["http://hub.example/"])
        self.assertEqual(selfs, ["http://src.example/"])

    def test_no_hub_anywhere(self):
        self.assertEqual(from_headers({}), ([], []))
        self.assertEqual(from_body(b"<html></html>"), ([], []))


class DiscoveryHTTPTestCase(unittest.TestCase):
    def test_discover_against_local_page(self):
        class Page(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                body = (b'<feed><link rel="hub" href="http://127.0.0.1:1/hub"/>'
                        b'<link rel="self" href="http://127.0.0.1:1/feed"/></feed>')
                self.send_response(200)
                self.send_header("Content-Type", "application/atom+xml")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Page)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            page = discover("http://127.0.0.1:%d/feed" % server.server_address[1])
        finally:
            server.shutdown()
        self.assertEqual(page["hubs"], ["http://127.0.0.1:1/hub"])
        self.assertEqual(page["self"], "http://127.0.0.1:1/feed")
        self.assertIn("atom", page["content_type"])
