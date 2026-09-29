#!/usr/bin/env python3
"""
Ship `dev` to production: one command, no prompts.

halibutbank.ca serves the `main` checkout (~/envcan_wave) and cron runs the
pipeline from it, so moving `main` *is* the deploy. All work happens on `dev`
(~/envcan_wave-dev, previewed live at dev.halibutbank.ca); this script is the
only thing that moves `main` forward, and only after everything below passes.

    1. sync      rebase `dev` onto `main` (picks up the nightly crontab
                 commits), refresh `?v=` hashes, sync the venv if the lock
                 changed
    2. test      ruff, ESLint, pytest, JS unit tests, Playwright (every spec
                 but screenshots, on a private port)
    3. publish?  the publication check on everything GitHub and the site
                 would gain: every commit's message, author and diff; the
                 tracked tree; the served tree (scripts/hooks/check_secrets.py)
    4. ship      fast-forward `main` — production is live from here
    5. apply     install config/crontab.txt and restart sr3 services if their
                 configs changed
    6. smoke     fetch every page from the live site and check it serves the
                 new asset hashes, and that every versioned asset loads
    7. record    tag `deploy-YYYY-MM-DD[.N]`, push `main` + tag to GitHub,
                 back up to Forgejo

A failure in 1-3 leaves production untouched. A failure in 5-6 rolls `main`
back to where it was and re-applies the old config; nothing has been pushed
yet, so no public history is rewritten. Every run writes a report to
logs/deploy/.

Usage:
    scripts/deploy.py            # ship
    scripts/deploy.py --check    # steps 1-3 only: would it ship?

See docs/DEPLOY.md.
"""

from __future__ import annotations

import fcntl
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SITE_URL = os.environ.get("DEPLOY_SITE_URL", "https://halibutbank.ca")
PLAYWRIGHT_PORT = "4180"  # not 4173, the default everyone else uses
SR3_DEPLOYED = Path.home() / ".config" / "sr3" / "subscribe"
SMOKE_ATTEMPTS = 3


class DeployError(Exception):
    pass


class Deploy:
    def __init__(self, check_only: bool):
        self.check_only = check_only
        self.prod, self.dev = find_worktrees()
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_dir = self.prod / "logs" / "deploy"
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / f"{stamp}{'-check' if check_only else ''}.log"
        self.log = self.log_path.open("w")
        self.summary: list[str] = []
        self.previous = ""
        self.config_changes: list[str] = []

    # ── plumbing ────────────────────────────────────────────

    def say(self, message: str = "") -> None:
        print(message, flush=True)
        self.log.write(message + "\n")
        self.log.flush()

    def step(self, title: str) -> None:
        self.say(f"\n── {title} " + "─" * max(4, 60 - len(title)))

    def run(self, cmd: list[str], cwd: Path, env: dict | None = None) -> None:
        """Run a command, streaming its output to the terminal and the log."""
        self.say(f"$ {' '.join(cmd)}")
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            env={**os.environ, **(env or {})},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        )
        assert proc.stdout
        for line in proc.stdout:
            print(line, end="", flush=True)
            self.log.write(line)
        if proc.wait() != 0:
            raise DeployError(f"`{' '.join(cmd)}` failed (exit {proc.returncode})")

    def git(self, *args: str, cwd: Path | None = None) -> str:
        out = subprocess.run(
            ["git", *args], cwd=cwd or self.dev, capture_output=True, text=True
        )
        if out.returncode != 0:
            raise DeployError(f"git {' '.join(args)}: {out.stderr.strip()}")
        return out.stdout.strip()

    def changed(self, *paths: str) -> list[str]:
        """Files under `paths` that differ between production and dev."""
        return self.git("diff", "--name-only", "main", "dev", "--", *paths).split()

    # ── steps ───────────────────────────────────────────────

    def preflight(self) -> None:
        self.step("preflight")
        for tree, name in ((self.dev, "dev"), (self.prod, "production")):
            dirty = self.git("status", "--porcelain", "--untracked-files=no", cwd=tree)
            if dirty:
                raise DeployError(f"{name} checkout {tree} has uncommitted changes:\n{dirty}")
        if (Path(self.git("rev-parse", "--absolute-git-dir")) / "rebase-merge").exists():
            raise DeployError("a rebase is in progress on dev")
        # Best effort: a stale origin/main only widens the scanned range.
        subprocess.run(["git", "fetch", "-q", "origin"], cwd=self.prod, check=False)
        self.say(f"production: {self.prod} (main)\ndev:        {self.dev} (dev)")

    def sync(self) -> None:
        self.step("1. sync dev onto main")
        try:
            self.run(["git", "rebase", "main"], cwd=self.dev)
        except DeployError:
            subprocess.run(["git", "rebase", "--abort"], cwd=self.dev, check=False)
            raise DeployError("dev does not rebase cleanly onto main; resolve by hand")
        # Rebases skip the pre-commit hook, so replayed commits can carry
        # stale ?v= hashes.
        out = subprocess.run(
            [".venv/bin/python", "scripts/update_asset_versions.py"],
            cwd=self.dev, capture_output=True, text=True, check=True,
        ).stdout
        updated = re.findall(r"^updated (\S+)", out, re.M)
        if updated:
            self.run(["git", "add", *updated], cwd=self.dev)
            self.run(
                ["git", "commit", "-q", "-m", "Refresh asset versions after rebase onto main"],
                cwd=self.dev,
            )
        if self.changed("requirements-lock.txt"):
            # The venv is shared by both checkouts; new pins must be installed
            # before dev's tests can import them.
            self.run(
                [".venv/bin/pip", "install", "-q", "--disable-pip-version-check",
                 "-r", "requirements-lock.txt"],
                cwd=self.dev,
            )
        if self.changed("package-lock.json"):
            self.summary.append("package-lock.json changed: run `npm ci` by hand if needed")

    def pending(self) -> list[str]:
        return self.git("log", "--reverse", "--format=%h %s", "main..dev").splitlines()

    def test(self) -> None:
        self.step("2. tests")
        py = ".venv/bin/"
        self.run([py + "ruff", "check", "."], cwd=self.dev)
        self.run(
            ["npx", "eslint", "--max-warnings", "0", "site/assets/js/**/*.js"], cwd=self.dev
        )
        self.run(
            [py + "pytest", "tests/", "-q", "--tb=short",
             "--deselect=tests/test_integration.py::TestSystemHealth"],
            cwd=self.dev,
        )
        self.run(["npm", "run", "-s", "test:js"], cwd=self.dev)
        specs = sorted(
            str(p.relative_to(self.dev))
            for p in (self.dev / "tests" / "playwright").glob("*.spec.js")
            if p.name != "screenshots.spec.js"
        )
        self.run(
            ["npx", "playwright", "test", "--reporter=line", *specs],
            cwd=self.dev, env={"PW_PORT": PLAYWRIGHT_PORT},
        )

    def publication_check(self) -> None:
        self.step("3. publication check")
        scan = [".venv/bin/python", "scripts/hooks/check_secrets.py", "--strict"]
        self.run([*scan, "--range", "origin/main..dev"], cwd=self.dev)
        self.run([*scan, "--all"], cwd=self.dev)
        self.run([*scan, "--served"], cwd=self.dev)
        self.say("clean: commits, tracked tree, served tree")

    def ship(self) -> None:
        self.step("4. ship")
        self.previous = self.git("rev-parse", "main")
        self.config_changes = self.changed("config/crontab.txt", "config/sr3/")
        self.run(["git", "merge", "--ff-only", "-q", "dev"], cwd=self.prod)
        self.say(f"main {self.previous[:10]} → {self.git('rev-parse', '--short=10', 'main')}")

    def apply_config(self) -> None:
        if not self.config_changes:
            return
        self.step("5. apply config")
        if "config/crontab.txt" in self.config_changes:
            self.run(["scripts/install_crontab.sh"], cwd=self.prod)
        for rel in self.config_changes:
            path = self.prod / rel
            if not (rel.startswith("config/sr3/") and rel.endswith(".conf")):
                continue
            service = "sr3-" + path.stem.replace("_", "-")
            if not path.exists():
                self.summary.append(f"{rel} was deleted: stop/disable {service} by hand")
                continue
            known = subprocess.run(
                ["systemctl", "cat", service], capture_output=True, check=False
            ).returncode == 0
            if not known:
                self.summary.append(
                    f"{rel} has no {service} service yet: set it up per docs/SR3_MANAGEMENT.md"
                )
                continue
            shutil.copy2(path, SR3_DEPLOYED / path.name)
            self.run(["sudo", "-n", "systemctl", "restart", service], cwd=self.prod)
            time.sleep(3)
            self.run(["systemctl", "is-active", "--quiet", service], cwd=self.prod)
            self.say(f"{service} restarted and active")

    def smoke(self) -> None:
        self.step("6. smoke check the live site")
        last_error = ""
        for attempt in range(1, SMOKE_ATTEMPTS + 1):
            try:
                self._smoke_once()
                return
            except DeployError as exc:
                last_error = str(exc)
                self.say(f"attempt {attempt}: {last_error}")
                time.sleep(5 * attempt)
        raise DeployError(f"smoke check failed: {last_error}")

    def _smoke_once(self) -> None:
        pages = sorted((self.prod / "site").glob("*.html"))
        assets: set[str] = set()
        for page in pages:
            local = page.read_text()
            wanted = set(re.findall(r'(?:href|src)="(/[^"]+\?v=[0-9a-f]+)"', local))
            route = "/" if page.name == "index.html" else f"/{page.name}"
            body = fetch(route).decode("utf-8", errors="replace")
            stale = [u for u in wanted if u not in body]
            if stale:
                raise DeployError(f"{route} is not serving the new version (missing {stale[0]})")
            assets |= wanted
        for url in sorted(assets):
            fetch(url)
        self.say(f"{len(pages)} pages serve the new version; {len(assets)} versioned assets load")

    def rollback(self) -> None:
        self.step("rollback")
        self.run(["git", "reset", "--hard", "-q", self.previous], cwd=self.prod)
        self.say(f"main is back at {self.previous[:10]}")
        if "config/crontab.txt" in self.config_changes:
            self.run(["scripts/install_crontab.sh"], cwd=self.prod)
        for rel in self.config_changes:
            path = self.prod / rel
            if rel.startswith("config/sr3/") and path.exists():
                shutil.copy2(path, SR3_DEPLOYED / path.name)
                service = "sr3-" + path.stem.replace("_", "-")
                self.run(["sudo", "-n", "systemctl", "restart", service], cwd=self.prod)
        try:
            self._smoke_once()
            self.say("previous version verified live")
        except DeployError as exc:
            self.say(f"WARNING: the previous version fails the smoke check too: {exc}")

    def record(self, shipped: list[str]) -> str:
        self.step("7. tag and push")
        tag = next_tag(self.git("tag", "--list", "deploy-*").split())
        subjects = "\n".join(f"- {line.split(' ', 1)[1]}" for line in shipped)
        message = f"Deploy: {len(shipped)} commit(s)\n\n{subjects}"
        self.git("tag", "-a", tag, "-m", message, "main", cwd=self.prod)
        # Production is already live; a failed push is reported, not fatal.
        # The nightly backup pushes main again after its own check.
        for cmd in (
            ["git", "push", "-q", "origin", "main", tag],
            ["git", "push", "-q", "forgejo", "main", "--tags"],
            ["git", "push", "-q", "--force-with-lease", "forgejo", "dev"],
        ):
            try:
                self.run(cmd, cwd=self.prod)
            except DeployError as exc:
                self.summary.append(f"push failed, retry by hand: {exc}")
        return tag

    # ── driver ──────────────────────────────────────────────

    def main(self) -> int:
        started = time.monotonic()
        shipped_live = False
        try:
            self.preflight()
            self.sync()
            shipped = self.pending()
            if not shipped:
                self.say("\nNothing to deploy: dev and main are the same commit.")
                return 0
            self.say(f"\n{len(shipped)} commit(s) to ship:")
            for line in shipped:
                self.say(f"  {line}")
            new_files = self.git("diff", "--name-only", "--diff-filter=A", "main", "dev")
            self.test()
            self.publication_check()
            if self.check_only:
                self.say("\n✅ check passed: this would ship. Nothing was changed on main.")
                return 0
            self.ship()
            shipped_live = True
            self.apply_config()
            self.smoke()
            tag = self.record(shipped)
        except DeployError as exc:
            self.say(f"\n❌ {exc}")
            if shipped_live:
                try:
                    self.rollback()
                except DeployError as rb:
                    self.say(f"\n🚨 ROLLBACK FAILED: {rb}\nProduction needs a human.")
                    return 2
                self.say("\nProduction rolled back; dev still has the commits.")
            else:
                self.say("\nProduction untouched.")
            self.say(f"Log: {self.log_path}")
            return 1
        finally:
            self.say(f"\n({time.monotonic() - started:.0f}s)")

        self.say(f"\n✅ deployed {tag}: {len(shipped)} commit(s)")
        if new_files:
            self.say("New public files:")
            for path in new_files.splitlines():
                self.say(f"  {path}")
        for note in self.summary:
            self.say(f"⚠️  {note}")
        self.say(f"Log: {self.log_path}")
        return 0


def find_worktrees() -> tuple[Path, Path]:
    """(production, dev): the checkouts holding `main` and `dev`."""
    out = subprocess.run(
        ["git", "worktree", "list", "--porcelain"],
        cwd=Path(__file__).resolve().parent, capture_output=True, text=True, check=True,
    ).stdout
    trees: dict[str, Path] = {}
    path = None
    for line in out.splitlines():
        if line.startswith("worktree "):
            path = Path(line.split(" ", 1)[1])
        elif line.startswith("branch refs/heads/") and path:
            trees[line.removeprefix("branch refs/heads/")] = path
    if "main" not in trees or "dev" not in trees:
        raise SystemExit(f"need worktrees for main and dev, found {sorted(trees)}")
    return trees["main"], trees["dev"]


def next_tag(existing: list[str]) -> str:
    """deploy-YYYY-MM-DD, then .2, .3, … for later deploys the same day."""
    base = f"deploy-{datetime.now(timezone.utc):%Y-%m-%d}"
    if base not in existing:
        return base
    n = 2
    while f"{base}.{n}" in existing:
        n += 1
    return f"{base}.{n}"


def fetch(path: str) -> bytes:
    request = urllib.request.Request(
        SITE_URL + path, headers={"User-Agent": "halibutbank-deploy-smoke/1"}
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.read()
    except Exception as exc:  # noqa: BLE001 — any failure is a smoke failure
        raise DeployError(f"GET {path}: {exc}") from exc


def main(argv: list[str]) -> int:
    lock_path = Path("/tmp/envcan_deploy.lock")
    with lock_path.open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("another deploy is running", file=sys.stderr)
            return 1
        return Deploy(check_only="--check" in argv).main()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
