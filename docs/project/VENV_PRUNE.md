# Venv prune — DONE 2026-08-02

**Status: complete.** The rebuild-and-swap below was executed on 2026-08-02.
The live `.venv` now matches `requirements-lock.txt` exactly: 84 packages → 34,
246 MB → 139 MB. The lock is now a truthful record of production.

Outcome, against the verification block below:

- Package diff vs the lock: clean apart from `pip==24.0`, which `python3 -m venv`
  bootstraps into every venv and the lock does not pin. **Expect this one line** —
  it is not drift.
- `sr3 features` → `amqp Installed`; all four subscriptions came back `active`
  with no errors in `journalctl`.
- `pytest -q` → 267 passed (same as pre-swap). `ruff check .` clean.
- `cdigest` still resolves from pipx at `~/.local/bin/cdigest`.
- Buoy pipeline ticked over post-swap with 0 stage failures, 10 buoys.
- Rebuild took **3.9 s** (warm cache), well under the 16 s budgeted.

`.venv.old` was kept as the rollback. **Delete it once a full day of pipelines
has run clean** — see Rollback below.

The procedure is left intact below; it is the reference for the next time the
venv needs rebuilding.

**Prerequisite work: DONE** (commit `8d3cd8a`, 2026-08-02) — dependencies are
declared in `pyproject.toml`, `requirements-lock.txt` is the derived pinned
artifact, `requirements.txt` is gone, and the nightly cron installs from the
lock.

## What was wrong (resolved 2026-08-02)

The live `.venv` held ~84 packages; `requirements-lock.txt` describes 32. It was
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

## Was it worth doing?

It was cheap in the end — under 4 s of rebuild, ~90 s of total wall clock, and
no pipeline lost a cycle. The gain is that the lock is now a truthful record of
production, so a future rebuild on a fresh box reproduces what actually runs.

Worth recording for next time: the risk that justified the caution here was the
*reverse* drift case — the lock missing something a rebuild needed — and that
was already fixed in `8d3cd8a`. Had this been declined, nothing would have
degraded; the divergence would simply have persisted until the next rebuild.
