"""Тесты TLS-режима хаба: https с самоподписанным сертификатом (нужен openssl)."""
from __future__ import annotations

import os
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
import urllib.request

from hubd.hub import Hub, build_server


@unittest.skipIf(shutil.which("openssl") is None, "нет openssl в окружении")
class TlsTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        cert = os.path.join(self.tmp.name, "cert.pem")
        key = os.path.join(self.tmp.name, "key.pem")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", key, "-out", cert, "-days", "1", "-subj",
                        "/CN=127.0.0.1"], check=True, capture_output=True)
        self.hub = Hub(os.path.join(self.tmp.name, "hub.json"))
        self.server = build_server(self.hub, "127.0.0.1", 0, tls_cert=cert, tls_key=key)
        self.url = "https://127.0.0.1:%d" % self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.tmp.cleanup()

    def test_publish_over_https(self):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE          # самоподписанный сертификат теста
        request = urllib.request.Request(self.url + "/publish?topic=t", data=b"hi",
                                         method="POST")
        with urllib.request.urlopen(request, context=context, timeout=5) as resp:
            self.assertEqual(resp.status, 200)

    def test_half_config_is_error(self):
        with self.assertRaises(ValueError):
            build_server(self.hub, "127.0.0.1", 0, tls_cert="only.pem")
