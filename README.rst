===================
The Precog Platform
===================

A few landmarks:

* **quirrel** - The Quirrel compiler, including parser, static analysis and
  emitter

  * ``Parser``
  * ``Binder``
  * ``ProvenanceChecker``

* **daze** - The Quirrel evaluator and standard library, including optimization
  passes

  * ``EvaluatorModule``
  * ``StdLibModule``
  * ``StaticInlinerModule``

* **yggdrasil** - Core data access and manipulation layer

  * ``ColumnarTableModule``
  * ``Slice``
  * ``Column``
  
* **niflheim** - Low-level block store and filesystem (NIHDB)  
* **ingest** - Unimaginatively, exactly what it seems to be
* **muspelheim** - Convergence point for the compiler and evaluator stacks;
  integration test *source* and data

  * ``ParseEvalStack``
  * ``MiscStackSpecs``
  
* **pandora** - Vestigial convergence point; integration test *binaries*

  * ``NIHDBPlatformSpecs``
  * ``REPL``

* **shard** - BlueEyes services and final instantiation of ALL THE THINGS
* **desktop** - Build scripts for standalone desktop version -- see local README.rst

Thus, to work on the evaluator, one would be in the **daze** project, writing
tests in the **daze** and **muspelheim** projects.  The tests in the **muspelheim**
project would be run from the **pandora** project (*not* from **muspelheim**),
but using the test data stored in **muspelheim**.  All of the other projects are
significantly saner.


Getting Started
===============

Step one: obtain `PaulP's script`_.  At this point, you should be able to run
``$ ./build-test.sh`` as a sanity check, but this will take a long time.  Instead,
run ``$ sbt``.  Once it is up and running, run ``test:compile``.  This should take
about 5-10 minutes.  After this, run ``ratatoskr/assembly``, followed by ``test``.
The build should be green once your machine stops burning.

In order to more easily navigate the codebase, it is highly recommended that you
install CTAGS_, if your editor supports it.  Our filename conventions are…inconsistent.

.. _PaulP's script: https://github.com/paulp/sbt-extras/blob/master/sbt
.. _CTAGS: http://ctags.sourceforge.net/


Process
=======

We use a pull request model for development.  When you want to work on a new
feature or bug, create a new branch based off of ``master`` (do not base off of
another branch unless you absolutely need the work in progress on that branch).
Collaboration is highly encouraged; accidental branch dependencies are not.
Your branch name should be given one of the following prefixes:

* ``topic/`` - For features, changes, refactorings, etc (e.g. ``topic/parse-function``)
* ``bug/`` - For things that are broken, investigations, etc (e.g. ``bug/double-allocation``)
* ``wip/`` - For code that is not ready for team-wide sharing (e.g. ``wip/touch-me-and-die``)

If you see a ``topic/`` or ``bug/`` branch on someone else's repository that has
changes you need, it is safe to base off of that branch instead of ``master``,
though you should still base off of ``master`` if at all possible.  Do not *ever*
base off of a ``wip/`` branch!  This is because the commits in a ``wip/`` branch
may be rewritten, rearranged or discarded entirely, and thus the history is not
stable.

Do your work on your local branch, committing as frequently as you like, squashing
and rebasing off of updated ``master`` (or any other ``topic/`` or ``bug/``
branch) at your discretion.  Any commits which are related to JIRA_ issues should
have a commit message of the following form::
    
    <message>
    
    PLATFORM-<issue#>
    
Substitute ``<message>`` and ``<issue#>`` with the desired commit message and
the JIRA issue number (e.g. ``PLATFORM-819``).  If a commit affects more
than one issue, simply include text for both separated by a single newline.  
Please always ensure that the issue number in your commit messages is accurate.
This information is used by the CI server and JIRA to automatically
update the workflow status of the relevant issue(s).  There are numerous examples
of JIRA-tagged commits in the Git history of ``master``.

When you are confident in your changes and ready for them to land, push your
``topic/`` or ``bug/`` branch to your *own* fork of **platform** (you can
`create a fork here`_).  Do not push to the main repository.

Once you have pushed to your fork, submit a Pull Request using GitHub's interface.
Take a moment to describe your changes as a whole, particularly highlighting any
API or Quirrel language changes which land as part of the changeset.  Your Pull
Request will be automatically merged with current master (but not yet pushed!)
and built by our `CI server`_.  The build status will appear in GitHub's interface,
telling you whether or not your changes built cleanly.  If your changes did *not*
build cleanly, or your branch no longer merges cleanly with ``master``, you will
need to either fix the issues or close the pull request until such time as the
issues are resolved.  If you do close the pull request and subsequently resolve
the problems, simply push the updated changes to your branch (do *not* rebase!)
and reopen the old pull request (there is a button for this).

While the CI server is busy blessing your pull request, other developers will
review your changes.  This is a standard code review process.  At the very least,
Kris will need to sign off on any change before it lands.  Generally, other
developers will review changes that touch their specific areas of expertise (e.g.
Daniel reviewing changes to **quirrel**), or if they are interested in the commit,
or if they're bored.  The more code review, the better!  Don't be shy about taking
action on their suggestions and pushing to your branch.  The pull request will
automatically update and rebuild.

If you would like to request a code review from a specific person, you should
mention them by GitHub handle within the pull request description (e.g. "Review
by **@dchenbecker**").  This sort of thing lets Kris know that your pull request
should not be merged until Derek has a chance to comment and sign off on the
changes.

Once your pull request is ready to be merged, Kris will merge your branch into
``staging``, which is a branch on the mainline repository that exists *purely*
for the purposes of aggregating pull requests.  It should not be considered a
developer branch, and only Kris should ever push to it or use its commits directly.
Whenever ``staging`` is pushed, the CI server automatically runs the full build
as a final sanity check and then pushes the changes as a *fast forward* to
``master`` once the build has completed successfully.  An automatic trigger builds
``master`` and triggers the deploy chain once everything is double-blessed.

This process ensures a minimum of friction between concurrent tasks while
simultaneously making it extremely difficult to break the build in ``master``.
Build problems are generally caught and resolved in pull requests, and in very
rare cases, in ``staging``.  This process also provides a very natural and fluid
avenue for code review and discussion, ensuring that the entire team is involved
and aware of everything that is happening.  Code review is *everyone's* responsibility.


Rebase Policy
=============

There is one hard and fast rule: **if the commits have been pushed, do not rebase.**
Once you push a set of commits, either to the mainline repository or your own
fork, you cannot rebase those commits any more.  The only exception to this rule
is if you have pushed a ``wip/`` branch, in which case you are allowed to rebase
and/or delete the branch as needed.

The reason for this policy is to encourage collaboration and avoid merge conflicts.
Rewriting history is a lovely Git trick, but it is extremely distruptive to others
if you rewrite history out from under their feet.  Thus, you should only ever
rebase commits which are *local* to your machine.  Once a commit has been pushed
on a non-``wip/`` branch, you no longer control that commit and you cannot rewrite it.

With that said, rebasing locally is *highly* encouraged, assuming you're fluent
enough with Git to know how to use the tool.  As a rule of thumb, always rebase
against the branch that you initial cut your local branch from whenever you are
ready to push.  Thus, my workflow looks something like the following::
    
    $ git checkout -b topic/doin-stuff
    ...
    # hack commit hack commit hack commit hack
    ...
    $ git fetch reportgrid
    $ git branch -f master reportgrid/master
    $ git rebase -i master
    # squash checkpoint commits, etc
    $ git push origin topic/doin-stuff

If I had based off a branch other than ``master``, such as a ``topic/`` branch
on another fork, then obviously the branch names would be different.  The basic
workflow remains the same though.

Once I get beyond the last command though, everything changes.  I can no longer
rebase the ``topic/doin-stuff`` branch.  Instead, if I need to bring in changes
from another branch, or even just resolve conflicts with ``master``, I need to
use ``git merge``.  This is because someone else may have decided to start a
project based on ``topic/doin-stuff``, and I cannot just rewrite commits which
they are now depending on.

To summarize: rebase privately, merge publicly.

.. _JIRA: https://precog.atlassian.net/secure/Dashboard.jspa
.. _create a fork here: https://github.com/precog/platform/fork_select
.. _CI server: https://jenkins.reportgrid.com
