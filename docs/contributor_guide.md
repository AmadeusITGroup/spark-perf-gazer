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
Report case classes are annotated with `@TableDoc` and `@ColumnDoc` to serve as the single source of truth
for the data model documentation (see [Data model documentation](#data-model-documentation) below).

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

### Data model documentation

The data model (SQL view schemas) is documented via custom annotations on the report case classes in `core/.../reports/`.
A build-time generator (`doc-generator/`) reads these annotations and produces:

- `docs/user_guide/data_model.md` — human-friendly Markdown tables with SQL types
- `docs/schema/perfgazer-schema.json` — agent-friendly structured JSON

When adding or modifying fields in a report case class, annotate them with `@ColumnDoc`:

```scala
@ColumnDoc(description = "Wall-clock duration of the task", unit = "ms")
taskDuration: Long,
```

When adding a new report case class, annotate the class with `@TableDoc`:

```scala
@TableDoc(name = "task", description = "Task-level execution metrics. One row per completed Spark task.")
case class TaskReport(
  ...
```

Both generated files are gitignored — they are produced by CI and by the local preview script.

### Local preview

First, generate the data model schemas from the annotated case classes:

```bash
sbt docGenerator/run
```

Then serve the site locally:

```bash
pip install mkdocs mkdocs-material
mkdocs serve
```

Open http://127.0.0.1:8000 in your browser.

Alternatively, `./scripts/docs-serve-local.sh` runs both steps in sequence.

### Full build

To reproduce the full CI documentation build (including `llms.txt` for AI agents):

```bash
./scripts/docs-build.sh
```

This runs schema generation, `mkdocs build`, and `generate-llms-txt.sh`. Output goes to `site/`.

### Deployment

Documentation is versioned using [mike](https://github.com/jimporter/mike) and deployed to GitHub Pages automatically under the following conditions:

- When pushing to `main` with changes in `docs/`, `mkdocs.yml`, report classes, or `doc-generator/`, the `dev` version is deployed.
- When publishing a GitHub Release, a versioned copy (e.g. `v0.1.0`) is deployed and the `latest` alias is updated.

The doc site is available at [amadeusitgroup.github.io/spark-perf-gazer](https://amadeusitgroup.github.io/spark-perf-gazer/).

#### Removing a published version

Deleting a GitHub Release does **not** remove the version from the docs site — versioned docs live as static files on the `gh-pages` branch, independent of GitHub Releases.

To remove a version from the docs site:

```bash
mike delete <version>   # e.g. mike delete v0.1.0
git push origin gh-pages
```

If you also want to clean up the GitHub Release and its tag, do that separately via the GitHub UI or CLI.

### Scripts reference

| Script | Purpose |
|--------|---------|
| `scripts/docs-serve-local.sh` | Full local docs build + live preview server (`mkdocs serve`) |
| `scripts/docs-build.sh` | Full docs build (schemas + MkDocs + llms.txt), same as CI |
| `scripts/generate-llms-txt.sh` | Generate `llms.txt` and `llms-full.txt` in `docs/` for agent consumption |

## Contributing

To contribute to this project, see [CONTRIBUTING.md](https://github.com/AmadeusITGroup/spark-perf-gazer/blob/main/CONTRIBUTING.md).

## Releasing

To release a new version of this project, see [RELEASING.md](https://github.com/AmadeusITGroup/spark-perf-gazer/blob/main/RELEASING.md).
