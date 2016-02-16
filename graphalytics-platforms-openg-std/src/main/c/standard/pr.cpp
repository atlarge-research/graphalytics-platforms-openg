//====== Graph Benchmark Suites ======//
//======== PageRank =======//
//
// Usage: ./pr.exe --dataset <dataset path> --dampingfactor <damping factor> --iteration <iteration>

#include "common.h"
#include "def.h"
#include "openG.h"
#include <queue>
#include "omp.h"
#include <stdint.h>
#include "util.hpp"

using namespace std;

class vertex_property
{
public:
    vertex_property():degree(0){}

    uint64_t degree;
    double rank;
    double sum;
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
    arg.add_arg("dampingfactor","0.85","damping factor of pagerank");
    arg.add_arg("iteration","10","pagerank iterations");
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
        size_t degree = vit->edges_size();
        vit->property().degree = degree;
        vit->property().rank = 1.0 / g.num_vertices();
        vit->property().sum = 0.0;

        global_input_tasks[vertex_distributor(vit->id(), threadnum)].push_back(vit->id());

    }
}

void parallel_pagerank(graph_t &g, size_t iteration, double damping_factor, unsigned threadnum,
                       vector<vector<uint64_t> > &global_input_tasks,
                       gBenchPerf_multi &perf, int perf_group)
{
    vector<vector<uint64_t> > global_output_tasks(threadnum*threadnum);
    size_t step = 0;
    bool stop = false;
    double dangling_sum = 0.0;
    #pragma omp parallel num_threads(threadnum) shared(stop,global_input_tasks,global_output_tasks)
    {
        unsigned tid = omp_get_thread_num();
        vector<uint64_t> & input_tasks = global_input_tasks[tid];

        perf.open(tid, perf_group);
        perf.start(tid, perf_group);
        while(!stop)
        {

            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];
                vertex_iterator vit = g.find_vertex(vid);

                if(vit->property().degree <= 0) {
                    #pragma omp atomic
                    dangling_sum += vit->property().rank;
                }
            }

            #pragma omp barrier
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];
                vertex_iterator vit = g.find_vertex(vid);

                for (edge_iterator eit=vit->edges_begin();eit!=vit->edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator destvit = g.find_vertex(dest_vid);

                    #pragma omp atomic
                    destvit->property().sum += vit->property().rank / vit->property().degree;
                }
            }

            #pragma omp barrier
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];
                vertex_iterator vit = g.find_vertex(vid);


                vit->property().rank = (1.0 - damping_factor) / g.num_vertices() +
                                       damping_factor * (vit->property().sum + dangling_sum / g.num_vertices());
                vit->property().sum = 0;
                global_output_tasks[vertex_distributor(vid,threadnum)+tid*threadnum].push_back(vid);
            }
            #pragma omp barrier
            dangling_sum = 0;
            input_tasks.clear();
            for (unsigned i=0;i<threadnum;i++)
            {
                    input_tasks.insert(input_tasks.end(),
                                       global_output_tasks[i*threadnum+tid].begin(),
                                       global_output_tasks[i*threadnum+tid].end());
                    global_output_tasks[i*threadnum+tid].clear();
            }
            if(tid==0) {
                if(step < iteration) {
                    step++;
                } else {
                    stop = true;
                }
            }
            #pragma omp barrier
        }
        perf.stop(tid, perf_group);
    }
}
//==============================================================//
void output(graph_t& g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        cout<<vit->id()<<" "<<vit->property().rank<<endl;
    }
}
void reset_graph(graph_t & g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().degree = 0;
    }

}

//==============================================================//
int main(int argc, char * argv[])
{
    graphBIG::print();
    cout<<"Benchmark: PageRank\n";
    double t1, t2;

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

    size_t threadnum, iteration;
    double damping_factor;
    arg.get_value("dampingfactor", damping_factor);
    arg.get_value("iteration", iteration);
    arg.get_value("threadnum",threadnum);

    graph_t graph;
    cout<<"loading data... \n";

    t1 = timer::get_usec();
    string vfile = path + "/vertex.csv";
    string efile = path + "/edge.csv";

    if (!load_graph_vertices(graph, vfile))
        return -1;
    if (!load_graph_edges(graph, efile))
        return -1;

    size_t vertex_num = graph.num_vertices();
    size_t edge_num = graph.num_edges();
    t2 = timer::get_usec();

    cout<<"== "<<vertex_num<<" vertices  "<<edge_num<<" edges\n";

#ifndef ENABLE_VERIFY
    cout<<"== time: "<<t2-t1<<" sec\n\n";
#endif

    cout<<"\nComputing pagerank..."<<endl;
    gBenchPerf_multi perf_multi(threadnum, perf);
    unsigned run_num = ceil(perf.get_event_cnt() /(double) DEFAULT_PERF_GRP_SZ);
    if (run_num==0) run_num = 1;
    double elapse_time = 0;

    for (unsigned i=0;i<run_num;i++)
    {
        queue<vertex_iterator> process_q;
        vector<vector<uint64_t> > global_input_tasks(threadnum);

        parallel_init(graph,threadnum,global_input_tasks);

        t1 = timer::get_usec();
        parallel_pagerank(graph, iteration, damping_factor, threadnum, global_input_tasks, perf_multi, i);
        t2 = timer::get_usec();
        elapse_time += t2-t1;
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
