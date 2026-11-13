# Publishing dlt-hardcharger

## One-time PyPI setup

At https://pypi.org/manage/account/publishing/ add a **pending GitHub publisher**:

| Field | Exact value |
| --- | --- |
| PyPI project name | `dlt-hardcharger` |
| GitHub owner | `gizmodata` |
| Repository name | `dlt-hardcharger` |
| Workflow filename | `ci.yml` |
| Environment name | `release` |

Enter only the workflow filename, not `.github/workflows/ci.yml`. No API token secret is needed.
A pending publisher creates the PyPI project on the first successful upload; it does not reserve
the name beforehand. In the GitHub repository, create the `release` environment and limit it to
`v*` tags. After the first publication, manage the publisher at
https://pypi.org/manage/project/dlt-hardcharger/settings/publishing/.

References: https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/ and
https://docs.pypi.org/trusted-publishers/adding-a-publisher/.

## Release

1. Set the version in `dlt_hardcharger/_version.py` (the only place it lives) and move
   `CHANGELOG.md`'s `[Unreleased]` entries into a `## [X.Y.Z] - YYYY-MM-DD` section.
2. Run `make all` locally, commit, push `main`, and wait for CI to pass on that commit.
3. Tag the commit CI passed on and push the tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
4. The tag's CI run tests the tagged commit again (lint, unit and integration tests and the
   README examples on every supported Python, with the newest dlt and with the lowest dependency
   versions), builds and checks the sdist and wheel, checks that the tag matches the package
   version and that `CHANGELOG.md` has its section, and only then publishes to PyPI with an OIDC
   identity and attestations and creates the GitHub Release with the section as its notes.
5. Check PyPI shows the new version and install it into a fresh environment
   (`pip install dlt-hardcharger==X.Y.Z`), then run a pipeline with `destination="hardcharger"`
   there. Update the Hardcharger integrations page on gizmodata.com only after that.

If the tag's run fails before anything was published, fix the code and move the same tag to the
fixed commit (this package is on PyPI only: there is no Go module proxy to have recorded the
tag). A failed upload can be retried by rerunning the failed job. Never move a tag whose version
is on PyPI: changed artifacts need a new version.

## When dlt releases a new minor version

The destination subclasses dlt's SQLAlchemy destination, so a dlt release can change what it
builds on. CI installs the newest dlt the requirement allows on every run, and the
lowest-versions job installs dlt 1.32.0; a weekly scheduled run does the same without a commit:
a red run after a dlt release is the signal. Fix
forward and release a patch; raise the floor only when the old dlt can no longer be supported.
