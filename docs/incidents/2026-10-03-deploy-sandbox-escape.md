# 2026-10-03: a deploy test harness wrote to the production host

**Severity:** low (no outage, no data loss, nothing published).
**Duration:** ~1 minute of writes (23:44:46–23:45:35 UTC); cleaned up the
same night.
**Outcome:** the harness, the deploy script it was testing, and the
deploy-gate workflow they served were all dropped on 2026-10-04.

## Background

On 2026-09-29 we built `scripts/deploy.py`: all work would happen on a `dev`
branch, and `main` (which the site serves and cron runs from) would move only
through a scripted deploy: tests, publication scan, fast-forward, apply
crontab and sr3 changes, smoke-check the live site, roll back on failure.

To test its failure paths before trusting it, we wrote
`tests/deploy_sandbox.py`: throwaway clones of the repo with fake local
remotes, and `crontab`, `sudo` and `systemctl` replaced on `PATH` by shims
that only log. Every scenario ran **on the production host, as the
production user**.

## What happened

1. The harness first gave each sandbox a fake `HOME`. The test suite reads
   real files under `HOME` (databases, env files), so pytest failed.
2. Replacing the fake `HOME` by linking real entries into it was blocked by
   the coding agent's permission check as potentially destructive. Instead,
   the two places the pipeline writes under `HOME` (deployed sr3 configs,
   crontab backups) were made overridable by environment variables, and the
   sandbox kept the real `HOME`.
3. That change was **not committed**. The sandbox clones *committed* code, so
   every scenario ran the old `deploy.py` and `install_crontab.sh`, which
   ignore the overrides.
4. The rollback scenario installed a test crontab and a test sr3 config.
   Through the old code these went to the real locations:
   - `~/.config/sr3/subscribe/bc_buoys.conf` gained two comment lines.
   - Three snapshots of the *sandbox's* crontab landed in
     `~/crontab_backups/`. Those snapshots had a deliberately paused nightly
     job re-enabled, plus test edits.
5. A per-scenario fingerprint of production state (refs, checkouts, live
   crontab, sr3 configs, crontab backups, remotes) detected the change and
   stopped the run before the next scenario.

## Impact

- The sr3 service was not restarted (the shim intercepted `systemctl`), so
  the running buoy feed never read the altered config. The config differed
  from canonical only by comments.
- The live crontab, the repository, GitHub and Forgejo were untouched.
- The latent risk was the backups. Restoring "the newest backup", the
  documented recovery step, would have silently re-enabled a paused job.

Cleanup: the sr3 config was restored from `config/sr3/`, and the three
snapshots were moved out of `~/crontab_backups/`.

## Root causes

1. **Isolation by convention.** PATH shims and environment variables only
   isolate code that cooperates. Anything that calls a real path, or
   predates the override, writes straight through. It *fails open*.
2. **Testing something other than what was reviewed.** The edit existed in
   the working tree. The code under test was the committed version.
3. **Testing ops tooling on the production host.** Every mistake in the
   harness was a mistake against production.
4. **Scope.** The deploy machinery solved a problem the project did not
   have. Committing straight to `main` had worked fine; `dev` only needed to
   hold the UI overhaul until it was ready. Each layer (deploy script, then
   a harness to test it, then safeguards for the harness) needed the next.

**Precedent, same class:** on 2026-09-29 a test in `tests/test_secrets.py`
built a scratch git repo that inherited the pre-commit hook's `GIT_DIR`,
re-initialised the *real* repository as bare and wrote a fake identity
into its config. Two incidents in one week where test code reached
production state.

## What worked

- The production fingerprint, checked after every scenario, limited the
  damage to one scenario and named exactly what changed.
- The permission check's refusals were right both times. They were warning
  signs, and should have been treated as a stop, not a detour.

## Lessons, for as the project grows

1. **Isolation must fail closed.** If something must not touch production,
   make that impossible at the OS level: a separate user, a container, a
   read-only mount. Don't rely on the code under test to honour a variable.
   An escape should be an error, not a write.
2. **Don't test ops tooling on the production host.** Use a container or
   another machine, or don't build tooling that needs that much testing.
3. **Test what you reviewed.** A harness that clones commits must refuse to
   run with uncommitted changes to the code under test.
4. **Prefer the conventional tool, and name it before building.** CI
   services, gitleaks, release-folder deploys and containers exist because
   these problems are common. Custom machinery is more to audit and trust,
   and it compounds.
5. **Detection is a backstop, not a guard.** Fingerprinting caught this
   after the fact. Keep it for anything risky, but don't mistake it for
   prevention.
6. **Backup folders must hold only real backups.** Anything else in them
   becomes a candidate for restore.
7. **Tests that run git must strip `GIT_*` and never write config.** Hooks
   export the real repository's paths to everything they run.

## What remains

Kept from the 2026-09-29 work because it closes real holes independent of
any deploy step: the nightly backup commits only the crontab and pushes to
GitHub only after the publication scan passes; the scan covers commit
messages, history ranges, private addresses and a private denylist. See
`docs/SECRETS.md`. `scripts/deploy.py` is in history (commit `e692287`) if a
deploy step is ever wanted again.

**What does the deploy step's job now** (2026-10-04). The deploy step was
meant to keep the production checkout, which the site serves live, safe from
work in progress. That job now falls to a rule and two built-in guards,
chosen per lesson 4 over anything custom:

- **The rule** (CLAUDE.md, "Where a change goes"): work that could leave the
  live site visibly broken while it is being checked goes to the `dev`
  worktree and is reviewed at the preview site; nobody switches branches in
  the production checkout, and any other branch gets its own worktree.
- **Git's own worktree rule:** `dev` cannot be checked out in production
  while the dev worktree holds it.
- **`scripts/hooks/post-checkout`** prints a warning whenever the production
  checkout lands on anything but `main`.
- **Claude Code sessions started in the production checkout** ask before any
  checkout, switch, stash, rebase, bisect or `reset --hard` (local settings).

These warn or ask; they do not fail closed in the sense of lesson 1. That is
deliberate. Lesson 1 is about test code that must never reach production.
These guards are against a slip by a person or the agent, where making the
mistake loud is proportionate.
