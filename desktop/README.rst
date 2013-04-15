Platform Desktop
================

This subproject is responsible for the generation of stand-alone versions of
Platform and Labcoat, intended to be installed on Desktop for single user.
There are multiple steps to this build, and a few pre-requisites which are not
bundled with the platform repository.

The binaries generated allow installation on OS X, Debian package managers,
Redhat RPM package managers, and Windows 32 and 64 bits. The windows version
alone bundles the requires JRE with the installer, but not the other versions.

**WARNING**

Because these packages are intended for external users, the build includes
a Proguard step which make source code retrieval difficult. Failure to execute
this step may result in the distribution of binaries from which Platform
source code may be decompiled.

Pre-requisites
==============

Before you proceed to build, there are some requisites that need to be fulfilled.

1. Download and install "install4j", which is used to produce the installers.
2. Update install4j's license with the command `install4jc -L <license>`. Please
   see our Google Docs for the license string.
3. Download the java runtime environment (JRE) for Windows 32 and 64 bits. If
   the windows binaries are not to be produced, this can be skipped. Do not
   download the `.exe` versions, but the `.tar.gz`, as install4j requires the
   latter.
4. Move the JRE to install4j's installation directory's `jres` subdirectory, or
   `~/.install4j5/jres` (assuming version5 of install4j), or to this directory.
5. Edit `desktop.install4j`, search for `includedJRE`, and fill in the file
   names, without the `.tar.gz` extension, of the downloaded JREs in the
   appropriate places.
6. Clone the project `quirrelide` to the same base directory as `platform` (that
   is, `../../quirrelide` from this directory).

Building Desktop
================

Building desktop involves copying Labcoat's files, building a distribution,
running proguard on it, and then running Install4J to produce the installation
binaries. These steps are accomplished by running the following commands:

    ./build-dist-jar.sh
    install4jc desktop.install4j

The installation binaries will be generated on the directory
`../../installer`.

