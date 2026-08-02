# Venv prune — pending cutover

**Status:** staged as a written procedure only. Nothing has been done to the
live `.venv`. Nothing is broken; this is hygiene, not a fix.

**Prerequisite work: DONE** (commit `8d3cd8a`, 2026-08-02) — dependencies are
declared in `pyproject.toml`, `requirements-lock.txt` is the derived pinned
artifact, `requirements.txt` is gone, and the nightly cron installs from the
lock.

## What is actually wrong

The live `.venv` holds ~84 packages; `requirements-lock.txt` describes 32. It is
a strict **superset** — nothing the project needs is missing, so the site, the
pipelines and the sr3 services all run correctly today. The problem is only that
what runs is not what the lock describes, so the lock is not yet a truthful
record of production.

Leftovers, and why they are there:

| Tree | Size | Why it can go |
|---|---|---|
| `codebase-digest` → tiktoken, PyGithub, cryptography, keyring, PyNaCl, regex, … | ~19 | Moved to pipx 2026-08-02; `cdigest` now lives in `~/.local/bin` |
| `streamlink` → pycryptodome, trio, wsproto, … | ~9 | Unreferenced by any code or doc in the repo |
| `anthropic` → pydantic, httpx, anyio, jiter, … | ~12 | Unreferenced; nothing requires it |
| `pdf2image` | 1 | Unreferenced — this is what was accidentally supplying Pillow |

## Two footguns (read before touching anything)

1. **A venv cannot be renamed.** Every console script in `bin/` starts with an
   absolute shebang — `#!/home/keelando/envcan_wave/.venv/bin/python3`. Building
   at `.venv.new` and then `mv .venv.new .venv` leaves `sr3`, `yt-dlp`, `pytest`
   and `ruff` pointing at a path that no longer exists. Since systemd invokes
   `/home/keelando/envcan_wave/.venv/bin/sr3` directly, **all four subscriptions
   would fail to start.** The new venv must be created at its final path.
2. **`pip uninstall` is not enough.** Removing `codebase-digest` leaves ~19
   orphaned transitive dependencies behind; pip does not garbage-collect them.
   Rebuild, don't uninstall.

(`.gitignore` was widened from `.venv/` to `.venv*/` in `8d3cd8a`, so a stray
rebuild directory can no longer be swept into the 07:17 auto-backup.)

## Cost

A full rebuild from a warm pip cache measured **16 s**. Budget ~40 s of total
downtime including stopping and starting services.

During that window the every-3-minutes cron jobs may fire and fail. They are
`flock`-guarded and simply log an error; the next run recovers. No data is lost,
because the sr3 feeds queue on the broker side.

## Procedure

Do this while sitting at the machine, not remotely.

```bash
cd /home/keelando/envcan_wave

# 1. Stop the four subscriptions
sudo systemctl stop sr3-bc-buoys sr3-bc-wind-stations \
                    sr3-marine-forecast sr3-bc-lightstation-obs

# 2. Move the old venv aside (do NOT delete it — this is the rollback)
mv .venv .venv.old

# 3. Build the new one AT THE FINAL PATH
/usr/bin/python3 -m venv .venv
.venv/bin/pip install -q --disable-pip-version-check -r requirements-lock.txt
.venv/bin/pip install -q -e .

# 4. Restart the subscriptions
sudo systemctl start sr3-bc-buoys sr3-bc-wind-stations \
                     sr3-marine-forecast sr3-bc-lightstation-obs
```

## Verification

```bash
# Package set matches the lock exactly (only the editable install should differ)
diff <(sort -f requirements-lock.txt | grep -v '^#') \
     <(.venv/bin/pip list --format=freeze | grep -v '^-e ' | sort -f)

# sr3 can still speak AMQP — this is the one that silently breaks
.venv/bin/sr3 features | grep amqp     # expect: Installed

# Services healthy
sudo systemctl status sr3-bc-buoys sr3-bc-wind-stations \
                      sr3-marine-forecast sr3-bc-lightstation-obs

# Project still works
.venv/bin/pytest -q                    # expect 267 passed
.venv/bin/ruff check .
cdigest --help                         # should still resolve, from pipx

# Data is flowing again (watch a fast pipeline tick over)
tail -f logs/buoy_pipeline.log
```

## Rollback

```bash
sudo systemctl stop sr3-bc-buoys sr3-bc-wind-stations \
                    sr3-marine-forecast sr3-bc-lightstation-obs
rm -rf .venv && mv .venv.old .venv
sudo systemctl start sr3-bc-buoys sr3-bc-wind-stations \
                     sr3-marine-forecast sr3-bc-lightstation-obs
```

Once you have watched a full cycle of every pipeline succeed — give it a day —
delete `.venv.old`.

## Is it worth doing at all?

Reasonable to decline. The gain is that the lock becomes a truthful record of
production; the cost is a service interruption on a box with no staging. The
dependency-drift danger was the *reverse* case — the lock missing things a
rebuild needed — and that is already fixed and committed. If you skip this,
nothing degrades; the divergence just persists until the next time the venv is
rebuilt for some other reason, at which point it resolves itself.
