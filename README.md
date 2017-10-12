# Graphalytics OpenG platform extension

[![Build Status](https://jenkins.tribler.org/buildStatus/icon?job=Graphalytics/Platforms/OpenG_master)](https://jenkins.tribler.org/job/Graphalytics/job/Platforms/job/OpenG_master/)

## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.

The following dependencies are required for this platform extension:

* [GraphBIG](https://github.com/graphbig/graphBIG/) **(checkout the graphalytics branch)**


Download [GraphBIG](https://github.com/graphbig/graphBIG/) and unpack into any directory. Modify `platform.openg.home` in `config/platform.properties` to point to this directory or set the environment variable `OPENG_HOME` to this directory.

Finally, refer to the documation of the Graphayltics core on how to build and run this platform repository.


## OpenG-specific benchmark configuration

Edit `config/platform.properties` to change the following settings:

- `platform.openg.home`: Directory where OpenG has been installed.
- `platform.openg.intermediate-dir`:  Directory where intermediate conversion files are stored. During the benchmark, graphs are converted from Graphalytics format to OpenG csv format.
- `platform.openg.num-worker-threads`: Number of threads to use when running OpenG.

