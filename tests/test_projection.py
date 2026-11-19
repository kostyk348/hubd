"""Проекция статусов RFC-индекса вместо сырого диффа.

Архивесса разобрала store_body вживую и нашла изъян: сырой unified diff по 13,8 МБ
XML не говорит, какой документ поменял судьбу, и в 80 строк вмещает ~8 смен. Проекция
`doc-id -> current-status` даёт одну-две строки на смену, и каждая строка сама себя
объясняет — ровно то, что нужно триажу музея.
"""
from __future__ import annotations

import unittest

from hubd.scan import parse_rfc_index, project_rfc_index_status

NS = 'xmlns="https://www.rfc-editor.org/rfc-index"'


def _index(entries: list[tuple[str, str, str]]) -> bytes:
    body = "".join(
        "<rfc-entry><doc-id>%s</doc-id><current-status>%s</current-status>"
        "<stream>%s</stream></rfc-entry>" % (doc, status, stream)
        for doc, status, stream in entries
    )
    return ("<?xml version='1.0'?><rfc-index %s>%s</rfc-index>" % (NS, body)).encode()


class ParseTest(unittest.TestCase):
    def test_parses_doc_id_status_and_stream(self):
        table = parse_rfc_index(_index([("RFC1347", "PROPOSED STANDARD", "Legacy")]))
        self.assertEqual(table["RFC1347"]["current-status"], "PROPOSED STANDARD")
        self.assertEqual(table["RFC1347"]["stream"], "Legacy")

    def test_broken_xml_gives_empty_table(self):
        self.assertEqual(parse_rfc_index(b"<rfc-index><not-closed>"), {})


class ProjectionTest(unittest.TestCase):
    def test_status_change_becomes_two_readable_lines(self):
        before = _index([("RFC1", "UNKNOWN", "Legacy"),
                         ("RFC1347", "PROPOSED STANDARD", "Legacy")])
        after = _index([("RFC1", "UNKNOWN", "Legacy"),
                        ("RFC1347", "HISTORIC", "Legacy")])
        text = project_rfc_index_status(before, after)
        self.assertIn("изменено записей: 1 (показано 1)", text)
        self.assertIn("-RFC1347  current-status: PROPOSED STANDARD  (stream: Legacy)", text)
        self.assertIn("+RFC1347  current-status: HISTORIC  (stream: Legacy)", text)
        self.assertNotIn("RFC1  current-status", text, "неизменившиеся записи в дифф не идут")

    def test_new_and_removed_entries_are_reported(self):
        before = _index([("RFC1", "UNKNOWN", "Legacy")])
        after = _index([("RFC1", "UNKNOWN", "Legacy"), ("RFC9999", "INFORMATIONAL", "IETF")])
        text = project_rfc_index_status(before, after)
        self.assertIn("изменено записей: 1 (показано 1)", text)
        self.assertIn("новая запись", text)

    def test_identical_bodies_give_no_text(self):
        body = _index([("RFC1", "UNKNOWN", "Legacy")])
        self.assertEqual(project_rfc_index_status(body, body), "")

    def test_limit_keeps_output_small(self):
        before = _index([("RFC%d" % i, "PROPOSED STANDARD", "Legacy") for i in range(100)])
        after = _index([("RFC%d" % i, "HISTORIC", "Legacy") for i in range(100)])
        text = project_rfc_index_status(before, after, limit_lines=11, limit_chars=10_000)
        self.assertLessEqual(len(text.splitlines()), 11)


if __name__ == "__main__":
    unittest.main()
