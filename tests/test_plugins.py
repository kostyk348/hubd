"""Тесты слота плагинов: загрузка, extract-замена адаптера, transform-цепочка, изоляция сбоев."""
from __future__ import annotations

import os
import tempfile
import unittest

from hubd.adapters import AdapterError, run_adapter
from hubd.plugins import load_plugins

GOOD = '''
def extract(item, result, state):
    return [{"topic": item["topic"] + "/via-plugin", "payload": "from " + result["url"]}]

def transform(event, item):
    return dict(event, payload=event["payload"] + b"!")
'''

BROKEN = "raise RuntimeError('сломан при загрузке')\n"

BAD_HOOK = '''
def extract(item, result, state):
    raise ValueError("упал на источнике")
'''


class PluginsTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = os.path.join(self.tmp.name, "plugins")
        os.makedirs(self.dir)
        for name, body in (("good", GOOD), ("broken", BROKEN), ("badhook", BAD_HOOK)):
            with open(os.path.join(self.dir, name + ".py"), "w", encoding="utf-8") as fh:
                fh.write(body)
        self.plugins, skipped = load_plugins([self.dir])
        self.assertEqual(set(self.plugins), {"good", "badhook"})
        self.assertEqual(len(skipped), 1)
        self.assertIn("сломан при загрузке", skipped[0][1])

    def tearDown(self):
        self.tmp.cleanup()

    def test_extract_replaces_adapter(self):
        item = {"plugin": "good", "topic": "t"}
        result = {"url": "http://src.example/x", "body": b"", "hash": "h"}
        events = run_adapter(item, result, {}, self.plugins)
        self.assertEqual(events[0]["topic"], "t/via-plugin")
        self.assertEqual(events[0]["payload"], b"from http://src.example/x")

    def test_transform_applies_after_extract(self):
        item = {"plugin": "good", "transform": "good", "topic": "t"}
        events = run_adapter(item, {"url": "u", "body": b"", "hash": "h"}, {}, self.plugins)
        self.assertTrue(events[0]["payload"].endswith(b"!"))

    def test_plugin_crash_is_adapter_error(self):
        item = {"plugin": "badhook", "topic": "t"}
        with self.assertRaises(AdapterError) as ctx:
            run_adapter(item, {"url": "u", "body": b"", "hash": "h"}, {}, self.plugins)
        self.assertIn("badhook", str(ctx.exception))

    def test_unknown_plugin_is_error(self):
        with self.assertRaises(AdapterError):
            run_adapter({"plugin": "ghost"}, {"url": "u", "body": b"", "hash": "h"},
                        {}, self.plugins)

    def test_missing_dir_is_skipped_with_reason(self):
        plugins, skipped = load_plugins([os.path.join(self.tmp.name, "нет-такого")])
        self.assertEqual(plugins, {})
        self.assertEqual(len(skipped), 1)
        self.assertIn("каталог не найден", skipped[0][1])
