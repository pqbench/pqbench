# Project workflow

## Updating branches and resolving conflicts

When a feature branch needs the latest project changes, update the fork's
`main` branch from the canonical repository first, then merge that main branch
into the feature branch:

```sh
git fetch upstream main
git push origin upstream/main:main
git fetch origin main
git switch <feature-branch>
git merge origin/main
```

Resolve each conflict, then verify the merge:

```sh
git diff --check
git diff --name-only --diff-filter=U
```

Run the relevant tests, commit the merge, and push the feature branch:

```sh
git push origin <feature-branch>
```

Keep `.docker-data/` untracked and do not commit downloaded sample data.
