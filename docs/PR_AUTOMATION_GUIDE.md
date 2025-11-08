# PR Automation Guide

How to automatically generate quality pull request titles and descriptions.

## Quick Reference

```bash
# Method 1: GitHub CLI (easiest)
gh pr create --fill

# Method 2: Use our script
./scripts/generate_pr_description.sh > PR.md
gh pr create --body-file PR.md

# Method 3: Manual but with template
# Just open PR on GitHub - template auto-fills
```

---

## Method 1: GitHub CLI Auto-Generate (Recommended)

The `gh` CLI has built-in PR generation from your commits.

### Basic Usage

```bash
# Auto-generate title and body from commits
gh pr create --fill

# Review and edit in your browser before submitting
gh pr create --fill --web

# With additional options
gh pr create --fill --assignee @me --label enhancement --reviewer username
```

### What It Generates

**Title:** First line of your most recent commit message

**Body:**
- Lists all commits on the branch
- Shows files changed with diff stats
- Includes any commit message bodies

**Example output:**
```markdown
## Commits
- Add peak water level tracking and GDSPS limitations documentation
- Implement combined total water level predictions (astronomical + surge)
- Add comprehensive test data infrastructure for offline development

## Files Changed
export_combined_water_level.py | 338 ++++++++++++++++++
tests/README.md                 | 297 +++++++++++++++
...
```

### Advanced Options

```bash
# Use custom title
gh pr create --fill --title "Combined Water Level Predictions + Test Infrastructure"

# Fill in base branch
gh pr create --fill --base main

# Draft PR
gh pr create --fill --draft

# Skip editor, create immediately
gh pr create --fill --yes
```

---

## Method 2: Custom Script Generator

For more control over formatting, use our custom script.

### Usage

```bash
# Generate PR description from commits
./scripts/generate_pr_description.sh > PR_DESCRIPTION.md

# Edit the file
nano PR_DESCRIPTION.md

# Create PR with your description
gh pr create --body-file PR_DESCRIPTION.md --title "Your Custom Title"
```

### What It Generates

```markdown
# PR: your-branch-name

## Summary

_[Add 1-2 sentence summary here]_

## Changes

- Commit message 1
- Commit message 2
- ...

## Files Changed

[Diff stat showing all changes]

## Commits

### Full commit message 1
Body text...

### Full commit message 2
Body text...

## Testing

- [ ] Tested locally
- [ ] All tests pass
```

### Customizing the Script

Edit `scripts/generate_pr_description.sh` to change:
- Format of commit listings
- Sections included
- Testing checklist items
- Base branch comparison

**Example customization:**
```bash
# Show only last 5 commits
git log --reverse --pretty=format:"- %s" HEAD~5..HEAD

# Group by type
git log --pretty=format:"%s" | grep "^feat:" | sed 's/^/- /'
```

---

## Method 3: GitHub PR Template

We've created `.github/pull_request_template.md` which **auto-fills** when you create a PR on GitHub.

### How It Works

1. Push your branch: `git push`
2. Go to GitHub and click "New Pull Request"
3. The template automatically fills in the description box
4. Fill in the placeholders

### Template Structure

```markdown
## Summary
<!-- Brief 1-2 sentence description -->

## Changes
-
-

## Testing
- [ ] Tested locally
- [ ] Test mode working
- [ ] Production deployment tested

## Deployment Notes
- [ ] Requires crontab update
- [ ] No breaking changes

## Screenshots / Output

## Related Issues
Closes #

## Checklist
- [ ] Code follows project style
- [ ] Documentation updated
- [ ] Tests pass
```

### Editing the Template

Customize `.github/pull_request_template.md` for your workflow:
- Add/remove sections
- Change checklist items
- Add project-specific requirements

---

## Method 4: Commit Message Conventions

Write better commit messages → Get better auto-generated PRs.

### Conventional Commits Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Testing
- `refactor:` Code refactoring
- `chore:` Maintenance

**Examples:**
```bash
git commit -m "feat(tide): implement combined water level predictions

Combines astronomical tide with storm surge forecasts to produce
total predicted water level for next 2 calendar days.

Closes #42"
```

### Benefits

- GitHub CLI groups commits by type
- Easier to generate changelogs
- Clear commit history
- Auto-generates release notes

---

## Method 5: One-Liner PR Creation

Quick PR creation with minimal typing.

### Template

```bash
# Save this as an alias in ~/.bashrc or ~/.zshrc
alias pr='gh pr create --fill --web'
alias prdraft='gh pr create --fill --draft --web'
```

### Usage

```bash
# After committing your work
git push
pr  # Opens browser with auto-filled PR
```

---

## Best Practices

### 1. Write Good Commit Messages

Bad:
```
git commit -m "fixes"
git commit -m "update code"
```

Good:
```
git commit -m "Add peak water level tracking to combined predictions"
git commit -m "Update cron.txt with new export schedule"
```

### 2. Squash Before PR (Optional)

If you have messy commits:
```bash
# Interactive rebase to clean up
git rebase -i HEAD~5

# Squash into logical commits
# Then force push
git push --force-with-lease
```

### 3. Use PR Templates Consistently

Always fill in:
- Summary (what and why)
- Testing details (how you verified)
- Deployment notes (special steps needed)

### 4. Link Related Issues

```markdown
Closes #42
Fixes #43
Related to #44
```

GitHub will auto-close issues when PR merges.

### 5. Add Context for Reviewers

Don't just list files changed - explain:
- Why the change was needed
- What alternatives you considered
- Any tradeoffs made
- Testing approach

---

## Comparison

| Method | Speed | Control | Quality | Best For |
|--------|-------|---------|---------|----------|
| `gh pr create --fill` | ⚡⚡⚡ | ⭐⭐ | ⭐⭐⭐ | Quick PRs with good commits |
| Custom script | ⚡⚡ | ⭐⭐⭐ | ⭐⭐⭐⭐ | Complex PRs needing custom format |
| GitHub template | ⚡ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Consistent team workflow |
| Conventional commits | ⚡⚡ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Professional projects |

---

## Example Workflow

Here's a complete workflow for creating a quality PR:

```bash
# 1. Work on feature branch
git checkout -b feature/combined-water-level

# 2. Make changes with good commit messages
git add export_combined_water_level.py
git commit -m "feat(tide): implement combined water level predictions

Combines DFO IWLS astronomical tides with EC GDSPS storm surge
forecasts. Produces 2-day forecast with peak tracking.

Output: ~/site/data/combined-water-level.json"

git add tests/
git commit -m "test: add offline testing infrastructure for tide data

Creates test fixtures and databases for development without
requiring live API access."

git add docs/GDSPS_AND_WAVE_EFFECTS.md
git commit -m "docs: explain GDSPS limitations and wave effects

Documents what GDSPS predicts vs what's missing (wave setup,
wave runup). Critical for users to understand prediction accuracy."

# 3. Push branch
git push -u origin feature/combined-water-level

# 4. Generate PR (choose one method)

# Option A: Quick and dirty
gh pr create --fill --web

# Option B: With custom description
./scripts/generate_pr_description.sh > PR.md
# Edit PR.md to add summary
gh pr create --body-file PR.md --title "Combined Water Level Predictions + Test Infrastructure"

# Option C: Use web UI
# Go to GitHub, click "New PR", template auto-fills
```

---

## Troubleshooting

### "gh: command not found"

Install GitHub CLI:
```bash
# macOS
brew install gh

# Linux (Debian/Ubuntu)
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null
sudo apt update
sudo apt install gh

# Then authenticate
gh auth login
```

### "No base branch found"

Specify base branch explicitly:
```bash
gh pr create --fill --base main
# or
./scripts/generate_pr_description.sh main
```

### Template not showing

Check file location:
```bash
# Must be in one of these locations:
.github/pull_request_template.md
.github/PULL_REQUEST_TEMPLATE.md
docs/pull_request_template.md
```

### Auto-fill shows wrong commits

You're comparing to wrong base:
```bash
# See what commits will be included
git log origin/main..HEAD

# If wrong, rebase onto correct base
git rebase origin/main
```

---

## Tips for Great PRs

1. **One PR = One Feature**
   - Don't mix unrelated changes
   - Makes review easier
   - Easier to revert if needed

2. **Keep PRs Small**
   - Aim for <500 lines changed
   - Split large features into multiple PRs
   - Faster review turnaround

3. **Self-Review First**
   - Look at diff before creating PR
   - Catch obvious issues
   - Add comments explaining tricky parts

4. **Add Screenshots**
   - For UI changes
   - For CLI output
   - Example:
     ```
     ## Output
     ```
     🌊 Combined Water Level Forecast Export
     📍 point_atkinson + Point_Atkinson
       ✅ 155 combined predictions
       🔝 Peak: 3.168m at 2025-11-09 03:29 PM PST
     ```
     ```

5. **Link Documentation**
   - Point to new docs added
   - Reference related guides
   - Help reviewers understand context

---

## Resources

- [GitHub CLI Manual](https://cli.github.com/manual/)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [How to Write a Git Commit Message](https://chris.beams.io/posts/git-commit/)
- [GitHub PR Best Practices](https://github.com/google/eng-practices/blob/master/review/developer/cl-descriptions.md)
