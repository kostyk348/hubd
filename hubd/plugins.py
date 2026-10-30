"""Слот плагинов: свои адаптеры и фильтры событий без правки ядра.

Плагин — обычный .py-файл в каталоге, указанном флагом --plugins (флаг можно
повторять). Загружаются все *.py по алфавиту; имя плагина — имя файла без .py.

Контракт плагина (оба хука необязательны):

    def extract(item, result, state) -> list[dict] | dict | None
        Полная замена адаптера для источников, у которых в списке наблюдения
        указано "plugin": "имя файла". item — строка списка наблюдения,
        result — результат условного GET сканера (url, body, hash, validators…),
        state — состояние источника (переживает перезапуск). None — событий нет.
        Событие: {"topic", "url", "payload" (bytes/str/dict), "hash", "content_type"}.

    def transform(event, item) -> dict | None
        Переписать событие после адаптера: payload, topic, url, hash. None —
        событие отброшено: это осознанный фильтр, а не молчаливая потеря.
        Подключается полем "transform": "имя" (или списком имён).

Плагины — доверенный код: они исполняются с правами hubd, как и сам hubd.
Ошибка загрузки или неверный контракт не роняют обход: сбойный плагин
пропускается с видимой строкой, а ошибка на конкретном источнике возвращается
как ошибка этого источника (scan.py печатает её в отчёте обхода).
"""
from __future__ import annotations

import importlib.util
import os


def load_plugins(dirs) -> tuple[dict, list[tuple[str, str]]]:
    """Загрузить плагины из каталогов.

    Возвращает (плагины, пропущенные): {имя: модуль} и список
    (путь, причина) того, что загрузить не удалось и почему.
    """
    loaded: dict = {}
    skipped: list[tuple[str, str]] = []
    for directory in dirs or []:
        directory = os.path.abspath(directory)
        if not os.path.isdir(directory):
            skipped.append((directory, "каталог не найден"))
            continue
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".py") or name.startswith("_"):
                continue
            path = os.path.join(directory, name)
            module_name = "hubd_plugin_%s" % name[:-3]
            try:
                spec = importlib.util.spec_from_file_location(module_name, path)
                if spec is None or spec.loader is None:
                    raise ImportError("не удалось прочитать модуль")
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
            except Exception as exc:                   # сбойный плагин не роняет обход
                skipped.append((path, "%s: %s" % (type(exc).__name__, exc)))
                continue
            loaded[name[:-3]] = module
    return loaded, skipped
