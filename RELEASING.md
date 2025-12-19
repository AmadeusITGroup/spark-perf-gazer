# Releasing

Releasing on Sonatype and Maven Central is automated with [sbt-ci-release](https://github.com/sbt/sbt-ci-release).

To trigger a release for version `X.Y.Z` run:

```
version=X.Y.Z
git tag -a v$version -m "v$version"
git push origin v$version
```

or use the `release.sh` script, as done in the example below:

```
bash release.sh 1.2.3
```