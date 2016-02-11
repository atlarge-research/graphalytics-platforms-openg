# Graphalytics OpenG platform extension


## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.

The following dependencies are required for this platform extension (in parentheses are the tested versions):

* [GraphBIG](https://github.com/graphbig/graphBIG/) (commit 318744c)


Download [GraphBIG](https://github.com/graphbig/graphBIG/) and unpack into any directory. Modify `openg.home` in `config/openg.properties` to point to this directory or set the environment variable `OPENG_HOME` to this directory.

Finally, refer to the documation of the Graphayltics core on how to build and run this platform repository.


## OpenG-specific benchmark configuration

Edit `config/openg.properties` to change the following settings:

- `openg.home`: Directory where OpenG has been installed.
- `openg.intermediate-dir`:  Directory where intermediate conversion files are stored. During the benchmark, graphs are converted from Graphalytics format to OpenG csv format.
- `openg.num-worker-threads`: Number of threads to use when running OpenG.

