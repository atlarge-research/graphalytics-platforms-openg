//====== Graph Benchmark Suites ======//
//======== Local Clustering Coefficient =======//
//
// Usage: ./lcc.exe --dataset <dataset path>

#include "common.h"
#include "def.h"
#include "openG.h"
#include "omp.h"
#include "util.hpp"
#include <set>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <unordered_map>

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;

class vertex_property
{
public:
    vertex_property():count(0){}

    unsigned long count;
    unordered_set<uint64_t> unq_set;
    double lcc;

    friend ostream& operator<< (ostream &strm, const vertex_property &that) {
        return strm << that.lcc;
    }
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
void arg_init(argument_parser & arg)
{
    arg.add_arg("output", "", "Absolute path to the file where the output will be stored");
}
//==============================================================//
size_t get_intersect_cnt(unordered_set<uint64_t> & setA, vertex_iterator & vit_targ)
{
    unordered_set<uint64_t> setC;
    setC.clear();

    for ( auto it1 = setA.begin(); it1 != setA.end(); ++it1 ) {

                    for (edge_iterator eit=vit_targ->edges_begin();eit!=vit_targ->edges_end();eit++)
                    {
                                if(*it1 == eit->target()) {
                                    setC.insert(eit->target());
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
            for (auto it = vit->property().unq_set.begin(); it != vit->property().unq_set.end(); ++it) {
               vertex_iterator vit_targ = g.find_vertex(*it);
                size_t cnt = get_intersect_cnt(vit->property().unq_set, vit_targ);
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

#ifdef GRANULA
    granula::operation opengJob("OpenG", "Id.Unique", "Job", "Id.Unique");
    granula::operation loadGraph("OpenG", "Id.Unique", "LoadGraph", "Id.Unique");
    granula::operation processGraph("OpenG", "Id.Unique", "ProcessGraph", "Id.Unique");
    granula::operation offloadGraph("OpenG", "Id.Unique", "OffloadGraph", "Id.Unique");
    cout<<opengJob.getOperationInfo("StartTime", opengJob.getEpoch())<<endl;
#endif

    argument_parser arg;
    gBenchPerf_event perf;
    arg_init(arg);
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

#ifdef GRANULA
    cout<<loadGraph.getOperationInfo("StartTime", loadGraph.getEpoch())<<endl;
#endif

    t1 = timer::get_usec();
    string vfile = path + "/vertex.csv";
    string efile = path + "/edge.csv";

    if (!load_graph_vertices(graph, vfile))
        return -1;
    if (!load_graph_edges(graph, efile))
        return -1;

    uint64_t vertex_num = graph.num_vertices();
    uint64_t edge_num = graph.num_edges();
    t2 = timer::get_usec();
    cout<<"== "<<vertex_num<<" vertices  "<<edge_num<<" edges\n";

#ifdef GRANULA
    cout<<"== time: "<<t2-t1<<" sec\n";
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
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

#ifdef GRANULA
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    for (unsigned i=0;i<run_num;i++)
    {
        t1 = timer::get_usec();
        parallel_lcc(graph, threadnum, workset, perf_multi, i);
        t2 = timer::get_usec();

        elapse_time += t2 - t1;
        if ((i+1)<run_num) reset_graph(graph);
    }

#ifdef GRANULA
    cout<<processGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
    cout<<"== time: "<<elapse_time/run_num<<" sec\n";
    if (threadnum == 1)
        perf.print();
    else
        perf_multi.print();
#endif

#ifdef GRANULA
    cout<<offloadGraph.getOperationInfo("StartTime", offloadGraph.getEpoch())<<endl;
#endif

    string output_file;
    arg.get_value("output", output_file);

    if (!output_file.empty()) {
        write_graph_vertices(graph, output_file);
    }

#ifdef GRANULA
    cout<<offloadGraph.getOperationInfo("EndTime", offloadGraph.getEpoch())<<endl;
    cout<<opengJob.getOperationInfo("EndTime", opengJob.getEpoch())<<endl;
#endif

    cout<<"==================================================================\n";
    return 0;
}  // end main

