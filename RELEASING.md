# Releasing

## 1. Publish artifacts

Releasing on Sonatype and Maven Central is automated with [sbt-ci-release](https://github.com/sbt/sbt-ci-release).

To trigger a release for version `X.Y.Z` run:

```
version=X.Y.Z
git tag -a v$version -m "v$version"
git push origin v$version
```

or use the `release.sh` script:

```
bash release.sh 1.2.3
```

## 2. Create a GitHub Release

After the tag is pushed, create a GitHub Release to publish release notes and trigger the documentation site update.

Via GitHub: go to [Releases](https://github.com/AmadeusITGroup/spark-perf-gazer/releases) → "Draft a new release" → select the tag → "Generate release notes" → Publish.

This will automatically deploy a versioned documentation site at `https://amadeusitgroup.github.io/spark-perf-gazer/vX.Y.Z/` and update the `latest` alias.
