"""Тесты политики тем из конфига: deny > allow > всё разрешено; JSON-файл."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from hubd.hub import load_policy_file, topic_policy_from_config


class PolicyTestCase(unittest.TestCase):
    def test_deny_wins_over_allow(self):
        policy = topic_policy_from_config({"allow": ["museum/*"],
                                           "deny": ["museum/private/*"]})
        self.assertEqual(policy("museum/site"), (True, ""))
        ok, reason = policy("museum/private/x")
        self.assertFalse(ok)
        self.assertIn("deny", reason)

    def test_allow_list_restricts(self):
        policy = topic_policy_from_config({"allow": ["rfc/*"]})
        ok, reason = policy("other/1")
        self.assertFalse(ok)
        self.assertIn("rfc/*", reason)

    def test_empty_config_allows_everything(self):
        self.assertEqual(topic_policy_from_config({})("anything/at/all"), (True, ""))

    def test_load_policy_file(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump({"deny": ["bad/*"]}, fh)
        try:
            policy = load_policy_file(path)
            self.assertFalse(policy("bad/1")[0])
            self.assertTrue(policy("good/1")[0])
        finally:
            os.unlink(path)

    def test_load_policy_file_rejects_non_object(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("[]")
        try:
            with self.assertRaises(ValueError):
                load_policy_file(path)
        finally:
            os.unlink(path)
