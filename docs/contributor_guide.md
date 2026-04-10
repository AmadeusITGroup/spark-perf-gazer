# Contributor Guide

## Technical overview

Once registered, PerfGazer will listen to multiple events coming from `Spark`.

Some event objects at query/job/stage level are stored in memory for later processing.
Those events are wrapped by subtypes of `Event`. They are mostly start events, with some exceptions.
These are preserved in a `CappedConcurrentHashMap` that has a maximum size so that memory usage is limited.
The Spark events wrapped are related to classes like: 

- `org.apache.spark...StageInfo`
- `org.apache.spark...SparkListenerJobEnd`
- ...

When a SQL query, a job, a stage, or a task finishes, it triggers a callback mechanism. 

When the inputs are requested to `PerfGazer`, all collected `Event`s are inspected and transformed into `Report`s at the end
of the query/job/stage execution enriched with some extra information only available then, according
to the type of `Event`.

A `Report` is a type that represents the report unit shared with the end-user.

A `Filter` is a filter that operates on `Report`s, so that the end-user can have some control to focus specific aspects of
their Spark ETL (like *file pruning* for instance).

## Build

The project uses `sbt`. 

```sh
sbt test                           # run tests
sbt coverageOn test coverageReport # run tests with coverage checks on
```

## Dev environment

We use IntelliJ IDEA, you can update the ScalaTest Configuration Template to avoid manual settings.

```
Go to Run -> Edit Configurations -> Edit configuration templates -> ScalaTest 
```

For code formatting setup: 

```
Settings -> Editor -> Code Style -> Scala -> Formatter: ScalaFMT
```

## Run

You can run a local `spark-shell` with the listener as follows:

```bash
# publish a local snapshot version
export VERSION=0.0.0-$RANDOM-$RANDOM
sbt "set ThisBuild / version := \"$VERSION\"" publishLocal
# run spark shell with the listener (change the version accordingly) using the snippet provided above
spark-shell \
  --packages io.github.amadeusitgroup:perfgazer_spark_3-5-2_2.12:$VERSION \
  --conf spark.extraListeners=com.amadeus.perfgazer.PerfGazer \
  --conf spark.perfgazer.sink.class=com.amadeus.perfgazer.JsonSink \
  --conf spark.perfgazer.sink.json.destination=/tmp/perfgazer/applicationId={{spark.app.id}}/ \
  --conf "spark.driver.bindAddress=127.0.0.1" --conf "spark.driver.host=127.0.0.1"
```

Then you can run something like this in the shell to see logs from the listener:

```scala
sc.setLogLevel("INFO") // to change the log level
spark.sql("select 1").show()
:quit
```

## Documentation

The project uses [MkDocs](https://www.mkdocs.org/) with the [Material theme](https://squidfunk.github.io/mkdocs-material/).

### Local preview

```bash
pip install mkdocs mkdocs-material
mkdocs serve
```

Then open http://127.0.0.1:8000 in your browser.

### Deployment

Documentation is versioned using [mike](https://github.com/jimporter/mike) and deployed to GitHub Pages automatically:

- Push to `main` (changes in `docs/` or `mkdocs.yml`) → deploys the `dev` version
- Publishing a GitHub Release → deploys a versioned copy (e.g. `v0.1.0`) and updates the `latest` alias

The doc site is available at [amadeusitgroup.github.io/spark-perf-gazer](https://amadeusitgroup.github.io/spark-perf-gazer/).

## Contributing

To contribute to this project, see [CONTRIBUTING.md](https://github.com/AmadeusITGroup/spark-perf-gazer/blob/main/CONTRIBUTING.md).

## Releasing

To release a new version of this project, see [RELEASING.md](https://github.com/AmadeusITGroup/spark-perf-gazer/blob/main/RELEASING.md).
