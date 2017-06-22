#!/bin/sh
#
# Copyright 2015 Delft University of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Ensure the configuration file exists
if [ ! -f "$config/platform.properties" ]; then
	echo "Missing mandatory configuration file: $config/platform.properties" >&2
	exit 1
fi

# Get the first specification of platform.openg.home
OPENG_HOME=$(grep -E "^platform.openg.home[	 ]*[:=]" $config/platform.properties | sed 's/platform.openg.home[\t ]*[:=][\t ]*\([^\t ]*\).*/\1/g' | head -n 1)
if [ -z $OPENG_HOME ]; then
    echo "Error: home directory for OpenG not specified."
    echo "Define the environment variable \$OPENG_HOME or modify platform.openg.home in $config/platform.properties"
    exit 1
fi

# Set the "platform" variable
export platform="openg"

# Set Library jar
export LIBRARY_JAR=`ls lib/graphalytics-*default*.jar`
GRANULA_ENABLED=$(grep -E "^benchmark.run.granula.enabled[	 ]*[:=]" $config/granula.properties | sed 's/benchmark.run.granula.enabled[\t ]*[:=][\t ]*\([^\t ]*\).*/\1/g' | head -n 1)
if [ "$GRANULA_ENABLED" = "true" ] ; then
 if ! find lib -name "graphalytics-*granula*.jar" | grep -q '.'; then
    echo "Granula cannot be enabled due to missing library jar" >&2
 else
    export LIBRARY_JAR=`ls lib/graphalytics-*granula*.jar`
 fi
fi

# Build binaries
mkdir -p bin/standard
(cd bin/standard && cmake -DCMAKE_BUILD_TYPE=Release ../../src/main/c -DOPENG_HOME=$OPENG_HOME && make all VERBOSE=1)

if [ "$GRANULA_ENABLED" = "true" ] ; then
 mkdir -p bin/granula
 (cd bin/granula && cmake -DCMAKE_BUILD_TYPE=Release -DGRANULA=1 ../../src/main/c -DOPENG_HOME=$OPENG_HOME && make all VERBOSE=1)
fi


if [ $? -ne 0 ]
then
    echo "Compilation failed"
    exit 1
fi
