"""Поведение сетевого слоя сканера: что повторяем, а что нет.

Зачем отдельным файлом: на этой машине соединения к крупным архивам рвутся на
середине тела (rfc-index.xml, 13,8 МБ — `IncompleteRead` на 13 590 773 байтах).
Без повтора источник навсегда остаётся без отпечатка и молча не приносит
изменений: он в списке, но мёртвый. Повтор — единственный рычаг, потому что
сервер rfc-editor.org Range не поддерживает (на `Range:` отвечает 200 с полным
телом, а не 206), а gz-варианта у индекса нет (404).
"""
from __future__ import annotations

import email.message
import http.client
import unittest
import urllib.error
from unittest import mock

from hubd.scan import default_fetcher


class _Response:
    def __init__(self, body: bytes, status: int = 200, headers: dict | None = None):
        self._body = body
        self.status = status
        self.headers = headers or {"Content-Type": "application/xml"}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class DefaultFetcherTest(unittest.TestCase):
    def test_incomplete_read_is_retried(self):
        """Обрыв на середине тела — не приговор источнику."""
        incomplete = http.client.IncompleteRead(b"x" * 100, 50)
        with mock.patch("hubd.scan.urllib.request.urlopen",
                        side_effect=[incomplete, _Response(b"whole body")]) as call:
            with mock.patch("hubd.scan.time.sleep"):
                status, headers, body = default_fetcher("https://example.test/big.xml", {})
        self.assertEqual(status, 200)
        self.assertEqual(body, b"whole body")
        self.assertEqual(call.call_count, 2, "первый обрыв обязан стоить одного повтора")

    def test_connection_error_is_retried_up_to_limit(self):
        """Если сеть лежит совсем — три попытки и исключение наружу, а не тишина."""
        err = ConnectionResetError("connection reset by peer")
        with mock.patch("hubd.scan.urllib.request.urlopen", side_effect=err) as call:
            with mock.patch("hubd.scan.time.sleep"):
                with self.assertRaises(ConnectionResetError):
                    default_fetcher("https://example.test/big.xml", {})
        self.assertEqual(call.call_count, 3)

    def test_http_error_is_not_retried(self):
        """404 — это ответ сервера, а не сбой сети: повторять бессмысленно."""
        err = urllib.error.HTTPError("https://example.test/gone", 404, "Not Found",
                                     email.message.Message(), None)
        with mock.patch("hubd.scan.urllib.request.urlopen", side_effect=err) as call:
            status, _, _ = default_fetcher("https://example.test/gone", {})
        self.assertEqual(status, 404)
        self.assertEqual(call.call_count, 1)


if __name__ == "__main__":
    unittest.main()
