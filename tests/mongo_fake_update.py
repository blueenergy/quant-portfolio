"""Minimal MongoDB update operators for in-memory test collections."""

from __future__ import annotations

from typing import Any, Dict


def _set_nested(doc: Dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        child = cur.get(part)
        if not isinstance(child, dict):
            child = {}
            cur[part] = child
        cur = child
    cur[parts[-1]] = value


def _unset_path(doc: Dict[str, Any], path: str) -> None:
    if "." not in path:
        doc.pop(path, None)
        return
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        if not isinstance(cur.get(part), dict):
            return
        cur = cur[part]
    cur.pop(parts[-1], None)


def apply_mongo_update(doc: Dict[str, Any], update: Dict[str, Any], *, is_insert: bool) -> None:
    if is_insert:
        for key, value in (update.get("$setOnInsert") or {}).items():
            if key not in doc:
                doc[key] = value

    for key, value in (update.get("$set") or {}).items():
        if "." in key:
            _set_nested(doc, key, value)
        else:
            doc[key] = value

    for key in (update.get("$unset") or {}):
        _unset_path(doc, key)

    for field, spec in (update.get("$addToSet") or {}).items():
        if isinstance(spec, dict) and "$each" in spec:
            items = list(spec["$each"])
        else:
            items = [spec]
        existing = doc.get(field)
        if existing is None:
            doc[field] = list(items)
        elif isinstance(existing, list):
            for item in items:
                if item not in existing:
                    existing.append(item)
        else:
            doc[field] = list({existing, *items})


def update_one_collection(collection, filter_doc, update, upsert=False):
    """Apply update_one semantics to a list-backed fake collection."""
    import types

    filter_doc = filter_doc or {}
    for doc in collection._docs:
        if all(doc.get(k) == v for k, v in filter_doc.items()):
            apply_mongo_update(doc, update, is_insert=False)
            collection._last_update = update
            return types.SimpleNamespace(upserted_id=None, modified_count=1)

    if upsert:
        new_doc = dict(filter_doc)
        apply_mongo_update(new_doc, update, is_insert=True)
        collection._docs.append(new_doc)
        collection._last_update = update
        return types.SimpleNamespace(upserted_id="newid", modified_count=0)

    return types.SimpleNamespace(upserted_id=None, modified_count=0)
