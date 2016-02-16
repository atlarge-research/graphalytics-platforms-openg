//====== Graph Benchmark Suites ======//
//======= Weakly Connected Component =======//
//
// Usage: ./wcc.exe --dataset <dataset path> --root <root vertex id>

#include "common.h"
#include "def.h"
#include "perf.h"
#include "util.hpp"

#include "openG.h"
#include <queue>
#include "omp.h"
#include <stdint.h>

#ifdef SIM
#include "SIM.h"
#endif

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;

#define MY_INFINITY 0xffffff00

class vertex_property
{
public:
    vertex_property(){}
    uint64_t root;
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

}
//==============================================================//


inline unsigned vertex_distributor(uint64_t vid, unsigned threadnum)
{
    return vid%threadnum;
}

void parallel_init(graph_t& g, unsigned threadnum,
                   vector<vector<uint64_t> >& global_input_tasks)
{
    global_input_tasks.resize(threadnum);
    for (vertex_iterator vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().root = vit->id();
        global_input_tasks[vertex_distributor(vit->id(), threadnum)].push_back(vit->id());

    }
}

void parallel_wcc(graph_t &g, unsigned threadnum, vector<vector<uint64_t> > &global_input_tasks, gBenchPerf_multi &perf,
                  int perf_group)
{

    vector<vector<uint64_t> > global_output_tasks(threadnum*threadnum);

    bool stop = false;
    #pragma omp parallel num_threads(threadnum) shared(stop,global_input_tasks,global_output_tasks,perf)
    {
        unsigned tid = omp_get_thread_num();
        vector<uint64_t> & input_tasks = global_input_tasks[tid];

        perf.open(tid, perf_group);
        perf.start(tid, perf_group);
        while(!stop)
        {
            #pragma omp barrier
            // process local queue
            stop = true;


            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];
                vertex_iterator vit = g.find_vertex(vid);

                for (edge_iterator eit=vit->in_edges_begin();eit!=vit->in_edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator destvit = g.find_vertex(dest_vid);
                    if(destvit->property().root > vit->property().root) {
                        __sync_bool_compare_and_swap(&(destvit->property().root), destvit->property().root, vit->property().root);
                        global_output_tasks[vertex_distributor(dest_vid,threadnum)+tid*threadnum].push_back(dest_vid);
                    }
                }
                for (edge_iterator eit=vit->out_edges_begin();eit!=vit->out_edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator destvit = g.find_vertex(dest_vid);


                    bool done = false;
                    while(!done) {
                        if(destvit->property().root > vit->property().root) {
                            done = __sync_bool_compare_and_swap(&(destvit->property().root), destvit->property().root, vit->property().root);
                            if(done) {
                                global_output_tasks[vertex_distributor(dest_vid,threadnum)+tid*threadnum].push_back(dest_vid);
                            }
                        } else {
                            done = true;
                        }
                    }
                }
            }
            #pragma omp barrier
            input_tasks.clear();
            for (unsigned i=0;i<threadnum;i++)
            {
                if (global_output_tasks[i*threadnum+tid].size()!=0)
                {
                    stop = false;
                    input_tasks.insert(input_tasks.end(),
                                       global_output_tasks[i*threadnum+tid].begin(),
                                       global_output_tasks[i*threadnum+tid].end());
                    global_output_tasks[i*threadnum+tid].clear();
                }
            }
#pragma omp barrier

        }
        perf.stop(tid, perf_group);
    }

}

void output(graph_t& g)
{
    cout<<"WCC Results: \n";
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        cout << vit->id() << " " << vit->property().root << "\n";
    }
}

void reset_graph(graph_t & g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().root = vit->id();
    }

}

//==============================================================//
int main(int argc, char * argv[])
{
    graphBIG::print();
    cout<<"Benchmark: WCC\n";

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

    graph_t graph;
    double t1, t2;

    cout<<"loading data... \n";

#ifdef GRANULA
    granula::operation loadGraph("OpenG", "Id.Unique", "LoadGraph", "Id.Unique");
    cout<<loadGraph.getOperationInfo("StartTime", loadGraph.getEpoch())<<endl;
#endif

    t1 = timer::get_usec();
    string vfile = path + "/vertex.csv";
    string efile = path + "/edge.csv";

    if (!load_graph_vertices(graph, vfile))
        return -1;
    if (!load_graph_edges(graph, efile))
        return -1;

    size_t vertex_num = graph.vertex_num();
    size_t edge_num = graph.edge_num();
    t2 = timer::get_usec();
    cout<<"== "<<vertex_num<<" vertices  "<<edge_num<<" edges\n";

#ifdef GRANULA
    cout<<"== time: "<<t2-t1<<" sec\n";
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    gBenchPerf_multi perf_multi(threadnum, perf);
    unsigned run_num = ceil(perf.get_event_cnt() /(double) DEFAULT_PERF_GRP_SZ);
    if (run_num==0) run_num = 1;
    double elapse_time = 0;

#ifdef GRANULA
    granula::operation processGraph("OpenG", "Id.Unique", "ProcessGraph", "Id.Unique");
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    for (unsigned i=0;i<run_num;i++)
    {
        vector<vector<uint64_t> > global_input_tasks(threadnum);
        parallel_init(graph,threadnum,global_input_tasks);

        t1 = timer::get_usec();
        parallel_wcc(graph, threadnum, global_input_tasks, perf_multi, i);
        t2 = timer::get_usec();
        elapse_time += t2-t1;
        if ((i+1)<run_num) reset_graph(graph);
    }
    cout<<"WCC finish: \n";

#ifdef GRANULA
    cout<<processGraph.getOperationInfo("EndTime", processGraph.getEpoch())<<endl;
    cout<<"== time: "<<elapse_time/run_num<<" sec\n";
    if (threadnum == 1)
        perf.print();
    else
        perf_multi.print();
#endif

#ifdef GRANULA
    granula::operation offloadGraph("OpenG", "Id.Unique", "OffloadGraph", "Id.Unique");
    cout<<offloadGraph.getOperationInfo("StartTime", offloadGraph.getEpoch())<<endl;
    cout<<"\n";
    cout<<offloadGraph.getOperationInfo("EndTime", offloadGraph.getEpoch())<<endl;
#endif

#ifdef ENABLE_OUTPUT
    cout<<"\n";
    //output(graph);
#endif

    cout<<"=================================================================="<<endl;
    return 0;
}  // end main

