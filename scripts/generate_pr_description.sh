#!/bin/bash
# generate_pr_description.sh
# Auto-generate PR description from commits on current branch

# Get branch name
BRANCH=$(git branch --show-current)

# Get commits not in main (adjust base branch as needed)
BASE_BRANCH="${1:-main}"

echo "# PR: $BRANCH"
echo ""
echo "## Summary"
echo ""
echo "_[Add 1-2 sentence summary here]_"
echo ""
echo "## Changes"
echo ""
git log --reverse --pretty=format:"- %s" $BASE_BRANCH..HEAD
echo ""
echo ""
echo "## Files Changed"
echo ""
git diff --stat $BASE_BRANCH..HEAD | sed 's/^/ /'
echo ""
echo "## Commits"
echo ""
git log --reverse --pretty=format:"### %s%n%n%b%n" $BASE_BRANCH..HEAD
echo ""
echo "## Testing"
echo ""
echo "- [ ] Tested locally"
echo "- [ ] All tests pass"
echo ""
