# Precog

Precog is an advanced analytics engine for NoSQL data. It's sort of like
a traditional analytics database, but instead of working with
normalized, tabular data, it works with denormalized data that may not
have a uniform schema.

You can plop large amounts of JSON into Precog and start doing analytics
without any preprocessing, such as time series analytics, filtering,
rollups, statistics, and even some kinds of machine learning.

There's an API for developer integration, and a high-level application
called *Labcoat* for doing ad hoc and exploratory analytics.

Precog has been used by developers to build reporting features into
applications (since Precog has very comprehensive, developer-friendly APIs),
and together with *Labcoat*, Precog has been used by data scientists to perform
ad hoc analysis of semi-structured data.

This is the Community Edition of Precog. For more information about
commercial support and maintenance options, check out [SlamData,
Inc](<http://www.slamdata.com>), the official sponsor of the Precog open
source project.

# Roadmap

## Phase 1: Simplified Deployment

Precog was originally designed to be offered exclusively via the cloud in
a multi-tenant offering. As such, it has made certain tradeoffs that make it
much harder for individuals and casual users to install and maintain.

In the current roadmap, Phase 1 involves simplifying Precog to the point where
there are so few moving pieces, anyone can install and launch Precog, and keep
Precog running without anything more than an occasional restart.

The work is currently divided into the following tickets:

- [Remove MongoDB dependency](https://github.com/precog/platform/issues/523)
- [Remove Kafka dependency](https://github.com/precog/platform/issues/524)
- [Remove Zookeeper dependency](https://github.com/precog/platform/issues/525)
- [Separate ingest from query](https://github.com/precog/platform/issues/526)
- [Simplify file system model](https://github.com/precog/platform/issues/527)
- [Query directly from raw files](https://github.com/precog/platform/issues/528)
- [Conversion from raw files to NihDB file format](https://github.com/precog/platform/issues/529)

 
Many of these tickets indirectly contribute to Phase 2, by bringing the foundations 
of Precog closer into alignment with HDFS.

## Phase 2: Support for Big Data

Currently, Precog can only handle the amount of data that can reside on a single machine. 
While there are many optimizations that still need to be made (such as support for
indexes, type-specific columnar compression, etc.), a bigger win with more immediate
impact will be making Precog "big data-ready", where it can compete head-to-head with Hive,
Pig, and other analytics options for Hadoop.

Spark is an in-memory computational framework that runs as a YARN application inside
a Hadoop cluster. It can read from and write to the Hadoop file system (HDFS), and
exposes a wide range of primitives for performing data processing. Several high-performance,
scalable query systems have been built on Spark, such as Shark and BlinkDB. 

Given that Spark's emphasis is on fast, in-memory computation, that it's written in Scala,
and that it has already been used to implement several query languages, it seems an ideal target 
for Precog.
 
# Developer Guide

A few landmarks:

-   **common** - Data structures and service interfaces that are shared
    between multiple submodules.

-   **quirrel** - The Quirrel compiler, including the parser, static
    analysis code and bytecode emitter

    -   `Parser`
    -   `Binder`
    -   `ProvenanceChecker`

-   **mimir** - The Quirrel optimizer, evaluator and standard library

    -   `EvaluatorModule`
    -   `StdLibModule`
    -   `StaticInlinerModule`

-   **yggdrasil** - Core data access and manipulation layer

    -   `TableModule`
    -   `ColumnarTableModule`
    -   `Slice`
    -   `Column`

-   **niflheim** - Low-level columnar block store. (NIHDB)
    -   `NIHDB`

-   **ingest** - BlueEyes service front-end for data ingest.

-   **muspelheim** - Convergence point for the compiler and evaluator
    stacks; integration test sources and data

    -   `ParseEvalStack`
    -   `MiscStackSpecs`

-   **surtr** - Integration tests that run on the NIHDB backend.
    Surtr also provides a (somewhat defunct) REPL that gives access
    to the evaluator and other parts of the precog environment.

    -   `NIHDBPlatformSpecs`
    -   `REPL`

-   **bifrost** - BlueEyes service front-end for the 

-   **miklagard** - Standalone versions for the desktop and alternate 
    backend data stores -- see local README.rst. These need a bit of 
    work to bring them up to date; they were disabled some time ago
    and may have bitrotted.

-   **util** - Generic utility functions and data structures that
    are not specific to any particular function of the Precog codebase;
    convenience APIs for external libraries.

Thus, to work on the evaluator, one would be in the **mimir** project,
writing tests in the **mimir** and **muspelheim** projects. The tests in
the **muspelheim** project would be run from the **surtr** project
(*not* from **muspelheim**), but using the test data stored in
**muspelheim**. All of the other projects are significantly saner.

## Getting Started

Step one: obtain [PaulP's
script](https://github.com/paulp/sbt-extras/blob/master/sbt). At this
point, you should be able to run `$ ./build-test.sh` as a sanity check,
but this will take a long time. Instead, run `$ sbt`. Once it is up and
running, run `test:compile`. This should take about 5-10 minutes. After
this, run `ratatoskr/assembly`, followed by `test`. The build should be
green once your machine stops burning.

In order to more easily navigate the codebase, it is highly recommended
that you install [CTAGS](http://ctags.sourceforge.net/), if your editor
supports it. Our filename conventions are…inconsistent.

## Building and Running

These instructions are at best rudimentary, but should be sufficient to 
get started in a minimal way. More will be coming soon!

The Precog environment is organized in a modular, service-oriented
fashion with loosely coupled components that are relatively tolerant
to the failure of any single component (with likely degraded function).
Most of the components allow for redundant instances of the relevant
service, although in some cases (bifrost in particular) some tricky
configuration is required, which will not be detailed here.

Services:

-   **bifrost** - The primary service for evaluating NIHDB
-   **auth** - Authentication provider (checks tokens and grants; to be
    merged with accounts in the near term)
-   **accounts** - Account provider (records association between user 
    information and an account root token; to be merged with auth in 
    the near term)
-   **dvergr** - A simple job tracking service that is used to track
    batch query completion. 
-   **ingest** - The primary service for adding data to the Precog database.

Runnable jar files for all of these services can be built using the 
`sbt assembly` target from the root (platform) project. Sample configuration
files for each can be found in the `<projectname>/configs/dev` directory
for each relevant project; to run a simple test instance you can use the
start-shard.sh script. Note that this will download, configure, and run
local instances of mongodb, apache kafka, and zookeeper. Additional instructions
for running the precog database in a server environment will be coming soon.

## Contributing

All Contributions are bound by the terms and conditions of the [Precog
Contributor License Agreement](CLA.md).

### Pull Request Process

We use a pull request model for development. When you want to work on a
new feature or bug, create a new branch based off of `master` (do not
base off of another branch unless you absolutely need the work in
progress on that branch). Collaboration is highly encouraged; accidental
branch dependencies are not. Your branch name should be given one of the
following prefixes:

-   `topic/` - For features, changes, refactorings, etc (e.g.
    `topic/parse-function`)
-   `bug/` - For things that are broken, investigations, etc (e.g.
    `bug/double-allocation`)
-   `wip/` - For code that is not ready for team-wide sharing (e.g.
    `wip/touch-me-and-die`)

If you see a `topic/` or `bug/` branch on someone else's repository that
has changes you need, it is safe to base off of that branch instead of
`master`, though you should still base off of `master` if at all
possible. Do not *ever* base off of a `wip/` branch! This is because the
commits in a `wip/` branch may be rewritten, rearranged or discarded
entirely, and thus the history is not stable.

Do your work on your local branch, committing as frequently as you like,
squashing and rebasing off of updated `master` (or any other `topic/` or
`bug/` branch) at your discretion. 

When you are confident in your changes and ready for them to land, push
your `topic/` or `bug/` branch to your *own* fork of **platform** (you
can [create a fork here](https://github.com/precog/platform/fork_select)). 

Once you have pushed to your fork, submit a Pull Request using GitHub's
interface. Take a moment to describe your changes as a whole,
particularly highlighting any API or Quirrel language changes which land
as part of the changeset. 

Once your pull request is ready to be merged, it will be brought into the 
`staging` branch, which is a branch on the mainline repository that
exists *purely* for the purposes of aggregating pull requests. It should
not be considered a developer branch, but is used to run the full build 
as a final sanity check before the changes are pushed as a *fast forward* 
to `master` once the build has completed successfully. 
This process ensures a minimum of friction between concurrent tasks
while simultaneously making it extremely difficult to break the build in
`master`. Build problems are generally caught and resolved in pull
requests, and in very rare cases, in `staging`. This process also
provides a very natural and fluid avenue for code review and discussion,
ensuring that the entire team is involved and aware of everything that
is happening. Code review is *everyone's* responsibility.

### Rebase Policy

There is one hard and fast rule: **if the commits have been pushed, do
not rebase.** Once you push a set of commits, either to the mainline
repository or your own fork, you cannot rebase those commits any more.
The only exception to this rule is if you have pushed a `wip/` branch,
in which case you are allowed to rebase and/or delete the branch as
needed.

The reason for this policy is to encourage collaboration and avoid merge
conflicts. Rewriting history is a lovely Git trick, but it is extremely
distruptive to others if you rewrite history out from under their feet.
Thus, you should only ever rebase commits which are *local* to your
machine. Once a commit has been pushed on a non-`wip/` branch, you no
longer control that commit and you cannot rewrite it.

With that said, rebasing locally is *highly* encouraged, assuming you're
fluent enough with Git to know how to use the tool. As a rule of thumb,
always rebase against the branch that you initial cut your local branch
from whenever you are ready to push. Thus, my workflow looks something
like the following:

    $ git checkout -b topic/doin-stuff
    ...
    # hack commit hack commit hack commit hack
    ...
    $ git fetch upstream
    $ git branch -f master upstream/master
    $ git rebase -i master
    # squash checkpoint commits, etc
    $ git push origin topic/doin-stuff

If I had based off a branch other than `master`, such as a `topic/`
branch on another fork, then obviously the branch names would be
different. The basic workflow remains the same though.

Once I get beyond the last command though, everything changes. I can no
longer rebase the `topic/doin-stuff` branch. Instead, if I need to bring
in changes from another branch, or even just resolve conflicts with
`master`, I need to use `git merge`. This is because someone else may
have decided to start a project based on `topic/doin-stuff`, and I
cannot just rewrite commits which they are now depending on.

To summarize: rebase privately, merge publicly.

# License

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see \<<http://www.gnu.org/licenses/>\>.

# Legalese

Copyright (C) 2010 - 2013 SlamData, Inc. All Rights Reserved. Precog is
a registered trademark of SlamData, Inc, licensed to this open source
project.
