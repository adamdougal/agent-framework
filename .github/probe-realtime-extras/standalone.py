"""Install each realtime extra on its own, with pip and with uv, in a fresh venv, and probe it.

It builds the core, openai and foundry wheels from this checkout, so the installs resolve the extras exactly as a
user's ``pip install agent-framework-foundry[realtime]`` would, outside the workspace lock.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
PROBE = Path(__file__).resolve().parent / "probe.py"
PACKAGES = {
    "core": "agent-framework-core",
    "openai": "agent-framework-openai",
    "foundry": "agent-framework-foundry",
}


@dataclass
class Wheels:
    core: str
    openai: str
    foundry: str


@dataclass
class Scenario:
    name: str
    installer: str
    extras: list[str]
    requirements: Callable[[Wheels], list[str]]


def foundry_without_extra(projects_version: str) -> Callable[[Wheels], list[str]]:
    """The extra's transports with an azure-ai-projects the extra's ``<2.8`` cap excludes."""
    return lambda wheels: [
        wheels.core,
        wheels.openai,
        wheels.foundry,
        "azure-ai-voicelive[aiohttp]>=1.3.1,<2",
        f"azure-ai-projects=={projects_version}",
    ]


SCENARIOS = [
    Scenario("openai-pip", "pip", ["openai"], lambda wheels: [wheels.core, f"{wheels.openai}[realtime]"]),
    Scenario("openai-uv", "uv", ["openai"], lambda wheels: [wheels.core, f"{wheels.openai}[realtime]"]),
    Scenario(
        "foundry-pip", "pip", ["foundry"], lambda wheels: [wheels.core, wheels.openai, f"{wheels.foundry}[realtime]"]
    ),
    Scenario(
        "foundry-uv", "uv", ["foundry"], lambda wheels: [wheels.core, wheels.openai, f"{wheels.foundry}[realtime]"]
    ),
    Scenario(
        "both-uv",
        "uv",
        ["openai", "foundry"],
        lambda wheels: [wheels.core, f"{wheels.openai}[realtime]", f"{wheels.foundry}[realtime]"],
    ),
    Scenario(
        "openai-floor-uv",
        "uv",
        ["openai"],
        lambda wheels: [wheels.core, f"{wheels.openai}[realtime]", "openai==2.25.0", "websockets==13.0"],
    ),
    Scenario("foundry-projects-2.8-uv", "uv", ["foundry"], foundry_without_extra("2.8.0")),
    Scenario("foundry-projects-2.9-uv", "uv", ["foundry"], foundry_without_extra("2.9.0")),
]


def run(command: list[str], log_path: Path) -> tuple[int, str, float]:
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    seconds = round(time.perf_counter() - started, 1)
    entry = f"$ {' '.join(command)}\n{completed.stdout}\n[exit {completed.returncode} after {seconds}s]\n\n"
    with log_path.open("a", encoding="utf-8") as log:
        log.write(entry)
    print(entry, flush=True)
    return completed.returncode, completed.stdout, seconds


def build_wheels(output: Path, log_path: Path) -> Wheels:
    wheelhouse = output / "wheelhouse"
    for distribution in PACKAGES.values():
        code, _, _ = run(
            [
                "uv",
                "build",
                "--directory",
                str(REPOSITORY / "python"),
                "--package",
                distribution,
                "--wheel",
                "--no-build-logs",
                "--no-create-gitignore",
                "--out-dir",
                str(wheelhouse),
            ],
            log_path,
        )
        if code:
            raise SystemExit(f"building {distribution} failed")
    found = {}
    for key, distribution in PACKAGES.items():
        prefix = distribution.replace("-", "_") + "-"
        matches = sorted(wheelhouse.glob(f"{prefix}*.whl"))
        if len(matches) != 1:
            raise SystemExit(f"expected one wheel for {distribution}, found {matches}")
        found[key] = str(matches[0])
    return Wheels(**found)


def venv_python(venv: Path) -> str:
    return str(venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))


def run_scenario(scenario: Scenario, wheels: Wheels, output: Path) -> dict:
    log_path = output / f"{scenario.name}.log"
    venv = output / "venvs" / scenario.name
    python = venv_python(venv)
    result: dict = {"scenario": scenario.name, "installer": scenario.installer}

    code, _, _ = run(["uv", "venv", "--python", sys.executable, "--seed", str(venv)], log_path)
    if code:
        result["stage"] = "venv"
        return result

    requirements = scenario.requirements(wheels)
    if scenario.installer == "pip":
        install = [python, "-m", "pip", "install", "--disable-pip-version-check", *requirements]
        check = [python, "-m", "pip", "check"]
    else:
        install = ["uv", "pip", "install", "--python", python, *requirements]
        check = ["uv", "pip", "check", "--python", python]

    code, install_output, seconds = run(install, log_path)
    result["install_seconds"] = seconds
    if code:
        result["stage"] = "install"
        result["install_tail"] = install_output[-3000:]
        return result

    code, check_output, _ = run(check, log_path)
    result["dependency_check"] = "ok" if code == 0 else check_output[-2000:]

    report_path = output / f"{scenario.name}.json"
    probe = [python, str(PROBE), "--label", scenario.name, "--json", str(report_path)]
    for extra in scenario.extras:
        probe += ["--extra", extra]
    run(probe, log_path)
    if report_path.exists():
        result["report"] = json.loads(report_path.read_text())
        result["stage"] = "probed"
    else:
        result["stage"] = "probe crashed"
    return result


def passed(result: dict) -> bool:
    report = result.get("report")
    return (
        result.get("stage") == "probed"
        and result.get("dependency_check") == "ok"
        and all(check["ok"] for check in report["checks"])
    )


def markdown(results: list[dict], python_version: str) -> str:
    lines = [
        f"### Standalone installs on Python {python_version} ({sys.platform})",
        "",
        "| Scenario | Result | websockets (speedups) | openai | azure-ai-voicelive | azure-ai-projects | aiohttp | Failures |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        versions = result.get("report", {}).get("versions", {})
        failures = [
            check["check"] + ": " + check["detail"]
            for check in result.get("report", {}).get("checks", [])
            if not check["ok"]
        ]
        if result.get("stage") != "probed":
            failures.append(f"stopped at {result.get('stage')}")
        if result.get("dependency_check") not in (None, "ok"):
            failures.append("dependency check failed")
        cells = [
            result["scenario"],
            "pass" if passed(result) else "FAIL",
            f"{versions.get('websockets')} ({versions.get('websockets.speedups')})",
            *(
                str(versions.get(name))
                for name in ("openai", "azure-ai-voicelive", "azure-ai-projects", "aiohttp")
            ),
            "<br>".join(failure.replace("|", "\\|")[:300] for failure in failures),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    wheels = build_wheels(output, output / "build.log")
    results = [run_scenario(scenario, wheels, output) for scenario in SCENARIOS]
    python_version = ".".join(map(str, sys.version_info[:3]))
    (output / "summary.json").write_text(
        json.dumps({"python": python_version, "platform": sys.platform, "results": results}, indent=2)
    )
    table = markdown(results, python_version)
    print(table)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(table)
    return 0 if all(passed(result) for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
