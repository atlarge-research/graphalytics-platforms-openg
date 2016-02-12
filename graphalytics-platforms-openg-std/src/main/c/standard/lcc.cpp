//====== Graph Benchmark Suites ======//
//======== Local Clustering Coefficient =======//
//
// Usage: ./lcc.exe --dataset <dataset path>

#include "common.h"
#include "def.h"
#include "openG.h"
#include "omp.h"
#include <set>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

using namespace std;

class vertex_property
{
public:
    vertex_property():count(0){}

    unsigned long count;
    unordered_set<uint64_t> unq_set;
    double lcc;
};

class edge_property
{
public:
    edge_property():value(0){}
    edge_property(uint8_t x):value(x){}

    uint8_t value;
};

typedef openG::extGraph<vertex_property, edge_property> graph_t;
typedef graph_t::vertex_iterator    vertex_iterator;
typedef graph_t::edge_iterator      edge_iterator;

//==============================================================//

//==============================================================//
size_t get_intersect_cnt(unordered_set<uint64_t> & setA, unordered_set<uint64_t> & setB)
{
    unordered_set<uint64_t> setC;
    setC.clear();

    for ( auto it1 = setA.begin(); it1 != setA.end(); ++it1 ) {
        for ( auto it2 = setB.begin(); it2 != setB.end(); ++it2 ) {
            if(*it1 == *it2) {
                setC.insert(*it1);
            }
        }
    }
    return setC.size();
}

void gen_workset(graph_t& g, vector<unsigned>& workset, unsigned threadnum)
{
    unsigned chunk = (unsigned)ceil(g.num_edges()/(double)threadnum);
    unsigned last=0, curr=0, th=1;
    workset.clear();
    workset.resize(threadnum+1,0);
    for (vertex_iterator vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        curr += vit->edges_size();
        if ((curr-last)>=chunk)
        {
            last = curr;
            workset[th] = vit->id();
            if (th<threadnum) th++;
        }
    }
    workset[threadnum] = g.num_vertices();
}

void parallel_lcc_init(graph_t &g, unsigned threadnum)
{
    vector<unsigned> ws;
    gen_workset(g, ws, threadnum);

    #pragma omp parallel num_threads(threadnum)
    {
        unsigned tid = omp_get_thread_num();

        // prepare neighbor set for each vertex
        for (uint64_t vid=ws[tid];vid<ws[tid+1];vid++)
        {
            vertex_iterator vit = g.find_vertex(vid);
            if (vit == g.vertices_end()) continue;

            vit->property().count = 0;

            unordered_set<uint64_t>& cur_set = vit->property().unq_set;
            cur_set.reserve(vit->in_edges_size() + vit->out_edges_size());
            for (edge_iterator eit=vit->in_edges_begin();eit!=vit->in_edges_end();eit++)
            {
                cur_set.insert(eit->target());
            }
            for (edge_iterator eit=vit->out_edges_begin();eit!=vit->out_edges_end();eit++)
            {
                cur_set.insert(eit->target());
            }
        }
    }
}


void parallel_lcc(graph_t &g, unsigned threadnum, vector<unsigned> &workset,
                  gBenchPerf_multi &perf, int perf_group)
{

    #pragma omp parallel num_threads(threadnum)
    {
        unsigned tid = omp_get_thread_num();

        perf.open(tid, perf_group);
        perf.start(tid, perf_group);
        unsigned start = workset[tid];
        unsigned end = workset[tid+1];
        if (end > g.num_vertices()) end = g.num_vertices();

        // run lcc now
        for (uint64_t vid=start;vid<end;vid++)
        {
            vertex_iterator vit = g.find_vertex(vid);

            unordered_set<uint64_t> &u_set = vit->property().unq_set;

            for (auto it = u_set.begin(); it != u_set.end(); ++it) {

                vertex_iterator vit_targ = g.find_vertex(*it);

                unordered_set<uint64_t> w_set;
                for (edge_iterator eit=vit_targ->edges_begin();eit!=vit_targ->edges_end();eit++)
                {
                    w_set.insert(eit->target());
                }
                size_t cnt = get_intersect_cnt(u_set, w_set);
                __sync_fetch_and_add(&(vit->property().count), cnt);
            }

            int degree = vit->property().unq_set.size();
            vit->property().lcc = 0;
            if(degree >= 2) {
                vit->property().lcc = (double) vit->property().count / (degree * (degree - 1));
            }
        }
        #pragma omp barrier
        perf.stop(tid, perf_group);
    }

}

void output(graph_t& g)
{
    cout<<"LCC Results: \n";
    for (vertex_iterator vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        cout<<vit->id()<<" "<<vit->property().lcc<<endl;
    }
}

void reset_graph(graph_t & g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().count = 0;
    }

}

int main(int argc, char * argv[])
{
    graphBIG::print();
    cout<<"Benchmark: LCC\n";

    argument_parser arg;
    gBenchPerf_event perf;
    if (arg.parse(argc,argv,perf,false)==false)
    {
        arg.help();
        return -1;
    }
    string path, separator;
    arg.get_value("dataset",path);
    arg.get_value("separator",separator);

    size_t threadnum;
    arg.get_value("threadnum",threadnum);

    double t1, t2;
    graph_t graph;

    cout<<"loading data... \n";

    t1 = timer::get_usec();
    string vfile = path + "/vertex.csv";
    string efile = path + "/edge.csv";

#ifndef EDGES_ONLY
    if (graph.load_csv_vertices(vfile, true, separator, 0) == -1)
        return -1;
    if (graph.load_csv_edges(efile, true, separator, 0, 1) == -1)
        return -1;
#else
    if (graph.load_csv_edges(path, true, separator, 0, 1) == -1)
        return -1;
#endif

    uint64_t vertex_num = graph.num_vertices();
    uint64_t edge_num = graph.num_edges();
    t2 = timer::get_usec();
    cout<<"== "<<vertex_num<<" vertices  "<<edge_num<<" edges\n";
#ifndef ENABLE_VERIFY
    cout<<"== time: "<<t2-t1<<" sec\n";
#endif

    cout<<"\ninitializing lcc"<<endl;
    vector<unsigned> workset;
    parallel_lcc_init(graph, threadnum);
    gen_workset(graph, workset, threadnum);

    cout<<"\ncomputing lcc..."<<endl;

    gBenchPerf_multi perf_multi(threadnum, perf);
    unsigned run_num = ceil(perf.get_event_cnt() /(double) DEFAULT_PERF_GRP_SZ);
    if (run_num==0) run_num = 1;
    double elapse_time = 0;

    for (unsigned i=0;i<run_num;i++)
    {
        t1 = timer::get_usec();
        parallel_lcc(graph, threadnum, workset, perf_multi, i);
        t2 = timer::get_usec();

        elapse_time += t2 - t1;
        if ((i+1)<run_num) reset_graph(graph);
    }
#ifndef ENABLE_VERIFY
    cout<<"== time: "<<elapse_time/run_num<<" sec\n";
    if (threadnum == 1)
        perf.print();
    else
        perf_multi.print();
#endif

#ifdef ENABLE_OUTPUT
    cout<<"\n";
    //output(graph);
#endif
    cout<<"==================================================================\n";
    return 0;
}  // end main

