//====== Graph Benchmark Suites ======//
//======== Community Detection by Label Propagation =======//
//
// Usage: ./cdlp.exe --dataset <dataset path> --dampingfactor <damping factor> --iteration <iteration>

#include "common.h"
#include "def.h"
#include "openG.h"
#include <queue>
#include <atomic>
#include <unordered_map>
#include "omp.h"
#include "util.hpp"

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;

class vertex_property
{
public:
    vertex_property(){}
    uint64_t label;

    friend ostream& operator<< (ostream &strm, const vertex_property &that) {
        return strm << that.label;
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
    arg.add_arg("iteration","10","cdlp iterations");
    arg.add_arg("output", "", "Absolute path to the file where the output will be stored");
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
        vit->property().label = vit->id();
        global_input_tasks[vertex_distributor(vit->id(), threadnum)].push_back(vit->id());

    }
}

void parallel_cdlp(graph_t &g, size_t iteration, unsigned threadnum,
                   vector<vector<uint64_t> > &global_input_tasks,
                   gBenchPerf_multi &perf, int perf_group)
{
    vector<vector<uint64_t> > global_output_tasks(threadnum*threadnum);
    size_t step = 0;
    bool stop = false;
    #pragma omp parallel num_threads(threadnum) shared(stop,global_input_tasks,global_output_tasks)
    {
        unsigned tid = omp_get_thread_num();
        vector<uint64_t> & input_tasks = global_input_tasks[tid];

        perf.open(tid, perf_group);
        perf.start(tid, perf_group);
        while(!stop)
        {

            #pragma omp barrier
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];
                vertex_iterator vit = g.find_vertex(vid);

                unordered_map<uint64_t, uint64_t> histogram;

                for (edge_iterator eit=vit->in_edges_begin();eit!=vit->in_edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator destvit = g.find_vertex(dest_vid);
                    if(histogram.find(destvit->property().label) == histogram.end()) {
                        histogram[destvit->property().label] = 1;
                    } else {
                        histogram[destvit->property().label] += 1;
                    }


                }
                for (edge_iterator eit=vit->out_edges_begin();eit!=vit->out_edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator destvit = g.find_vertex(dest_vid);
                    if(histogram.find(destvit->property().label) == histogram.end()) {
                        histogram[destvit->property().label] = 1;
                    } else {
                        histogram[destvit->property().label] += 1;
                    }


                }

                uint64_t bestLabel = 0;
                uint64_t highest_freq = 0;
                for ( auto it = histogram.begin(); it != histogram.end(); ++it ) {
                    uint64_t label = it->first;
                    uint64_t freq = it->second;
                    if (freq > highest_freq || (freq == highest_freq && label < bestLabel)) {
                        bestLabel = label;
                        highest_freq = freq;
                    }
                }
                vit->property().label = bestLabel;

                histogram.clear();
                global_output_tasks[vertex_distributor(vid,threadnum)+tid*threadnum].push_back(vid);
            }
            #pragma omp barrier
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
        cout<<vit->id()<<" "<<vit->property().label<<endl;
    }
}
void reset_graph(graph_t & g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().label = vit->id();
    }

}

//==============================================================//
int main(int argc, char * argv[])
{
    graphBIG::print();
    cout<<"Benchmark: CDLP\n";

#ifdef GRANULA
    granula::operation opengJob("OpenG", "Id.Unique", "Job", "Id.Unique");
    granula::operation loadGraph("OpenG", "Id.Unique", "LoadGraph", "Id.Unique");
    granula::operation processGraph("OpenG", "Id.Unique", "ProcessGraph", "Id.Unique");
    granula::operation offloadGraph("OpenG", "Id.Unique", "OffloadGraph", "Id.Unique");
    cout<<opengJob.getOperationInfo("StartTime", opengJob.getEpoch())<<endl;
#endif

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

#ifdef GRANULA
    cout<<opengJob.getOperationInfo("StartTime", opengJob.getEpoch())<<endl;
    cout<<loadGraph.getOperationInfo("StartTime", loadGraph.getEpoch())<<endl;
#endif

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

#ifdef GRANULA
    cout<<"== time: "<<t2-t1<<" sec\n";
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

    cout<<"\nComputing cdlp..."<<endl;
    gBenchPerf_multi perf_multi(threadnum, perf);
    unsigned run_num = ceil(perf.get_event_cnt() /(double) DEFAULT_PERF_GRP_SZ);
    if (run_num==0) run_num = 1;
    double elapse_time = 0;

#ifdef GRANULA
    cout<<processGraph.getOperationInfo("StartTime", processGraph.getEpoch())<<endl;
#endif

    for (unsigned i=0;i<run_num;i++)
    {
        queue<vertex_iterator> process_q;
        vector<vector<uint64_t> > global_input_tasks(threadnum);

        parallel_init(graph,threadnum,global_input_tasks);

        t1 = timer::get_usec();
        parallel_cdlp(graph, iteration, threadnum, global_input_tasks, perf_multi, i);
        t2 = timer::get_usec();
        elapse_time += t2-t1;
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

