function buildProgram {

module unload intel-mpi
module unload gcc
module load openmpi/open64/64/1.10.1 
module load openmpi/gcc/64/1.10.1 
module list


OPENG_HOME=/var/scratch/wlngai/graphalytics/runner/app/openg/src/

mkdir -p bin/standard
(cd bin/standard && cmake -DCMAKE_BUILD_TYPE=Release ../../src/main/c -DOPENG_HOME=$OPENG_HOME && make all VERBOSE=1)

mkdir -p bin/granula
(cd bin/granula && cmake -DCMAKE_BUILD_TYPE=Release -DGRANULA=1 ../../src/main/c -DOPENG_HOME=$OPENG_HOME && make all VERBOSE=1)


rm -f bin/*/CMakeCache.txt
sed -i '47,54d' sh/prepare-benchmark.sh 

}


buildProgram
