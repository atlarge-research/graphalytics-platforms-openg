# set the collections of algorithms
declare -A algs='()'
algs["bfs"]=bench_BFS/bfs
algs["lcc"]=bench_triangleCount/tc
algs["cc"]=bench_connectedComp/connectedcomponent

# remove existing build
rm -Rf graphBIG
rm -Rf bin

# download and build graphBig (and openG)
mkdir bin
git clone https://github.com/graphbig/graphBIG
make PERF=0 all -C graphBIG/benchmark

# copying algorithms binaries into bin/ directory
for alg in ${!algs[@]}; do
    echo copying ${algs[${alg}]} to bin/
    cp graphBIG/benchmark/${algs[${alg}]} bin/
done

rm -Rf graphBIG



