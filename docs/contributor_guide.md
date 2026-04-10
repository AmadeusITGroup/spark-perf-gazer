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
Report case classes are annotated with `@SchemaReport` and `@SchemaDoc` to serve as the single source of truth
for the data model documentation (see [Data model documentation](#data-model-documentation) below).

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
# (optional) clean previous local publishes and publish, for example
find ~/.ivy2 -type f -name *perfgazer* | xargs rm
# publish a local snapshot version
sbt publishLocal
# run spark shell with the listener (change the version accordingly) using the snippet provided above
spark-shell --packages io.github.amadeusitgroup:perfgazer_spark_3.5.2_2.12:0.0.2-SNAPSHOT ...
```

## Documentation

The project uses [MkDocs](https://www.mkdocs.org/) with the [Material theme](https://squidfunk.github.io/mkdocs-material/).

### Data model documentation

The data model (SQL view schemas) is documented via custom annotations on the report case classes in `core/.../reports/`.
A build-time generator (`doc-generator/`) reads these annotations and produces:

- `docs/user_guide/data_model.md` — human-friendly Markdown tables with SQL types
- `docs/schema/perfgazer-schema.json` — agent-friendly structured JSON

When adding or modifying fields in a report case class, annotate them with `@SchemaDoc`:

```scala
@SchemaDoc("Wall-clock duration of the task", unit = "ms")
taskDuration: Long,
```

When adding a new report case class, annotate the class with `@SchemaReport`:

```scala
@SchemaReport("task", "Task-level execution metrics. One row per completed Spark task.")
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

Documentation is versioned using [mike](https://github.com/jimporter/mike) and deployed to GitHub Pages automatically:

- Push to `main` (changes in `docs/`, `mkdocs.yml`, report classes, or `doc-generator/`) → deploys the `dev` version
- Publishing a GitHub Release → deploys a versioned copy (e.g. `v0.1.0`) and updates the `latest` alias

The doc site is available at [amadeusitgroup.github.io/spark-perf-gazer](https://amadeusitgroup.github.io/spark-perf-gazer/).

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
