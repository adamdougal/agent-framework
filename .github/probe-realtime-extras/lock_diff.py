"""Summarise how a uv.lock changed: requires-python, forks, and every added, removed or changed package."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

FOCUS = ["openai", "websockets", "azure-ai-voicelive", "azure-ai-projects", "aiohttp", "azure-core"]


def load(path: str) -> tuple[dict, dict[str, list[str]]]:
    lock = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    versions: dict[str, list[str]] = {}
    for package in lock["package"]:
        versions.setdefault(package["name"], []).append(package["version"])
    return lock, versions


def main() -> int:
    base_lock, base = load(sys.argv[1])
    new_lock, new = load(sys.argv[2])
    title = sys.argv[3] if len(sys.argv) > 3 else "Lock changes"
    lines = [f"### {title}", ""]
    lines.append(f"- requires-python: `{base_lock.get('requires-python')}` -> `{new_lock.get('requires-python')}`")
    lines.append(
        f"- resolution forks: {len(base_lock.get('resolution-markers', []))} -> "
        f"{len(new_lock.get('resolution-markers', []))}"
    )
    multi_base = sorted(name for name, found in base.items() if len(found) > 1)
    multi_new = sorted(name for name, found in new.items() if len(found) > 1)
    lines.append(f"- packages locked at more than one version: {multi_base} -> {multi_new}")
    lines.append("")
    lines.append("| Package | Before | After |")
    lines.append("| --- | --- | --- |")
    for name in FOCUS:
        lines.append(f"| {name} | {', '.join(base.get(name, ['-']))} | {', '.join(new.get(name, ['-']))} |")
    lines.append("")
    changed = [
        (name, ", ".join(base.get(name, ["-"])), ", ".join(new.get(name, ["-"])))
        for name in sorted(set(base) | set(new))
        if sorted(base.get(name, [])) != sorted(new.get(name, []))
    ]
    lines.append(f"All changed packages ({len(changed)}):")
    lines.append("")
    lines.append("| Package | Before | After |")
    lines.append("| --- | --- | --- |")
    lines.extend(f"| {name} | {before} | {after} |" for name, before, after in changed)
    print("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
