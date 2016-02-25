//====== Graph Benchmark Suites ======//
//======= Breadth-first Search =======//
//
// Usage: ./bfs.exe --dataset <dataset path> --root <root vertex id>

#include "common.h"
#include "def.h"
#include "perf.h"
#include "util.hpp"

#include "openG.h"
#include <queue>
#include "omp.h"

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
    vertex_property():color(COLOR_WHITE),order(0),level(MY_INFINITY){}
    vertex_property(uint8_t x):color(x),order(0),level(MY_INFINITY){}

    uint8_t color;
    uint64_t order;
    uint64_t level;

    friend ostream& operator<< (ostream &strm, const vertex_property &that) {
        // According to graphalytics specifications, the output
        // for BFS should the maximum value for a signed 64-bit
        // integer if a vertex is unreachable.
        if (that.level == MY_INFINITY) {
            return strm << numeric_limits<int64_t>::max();
        } else {
            return strm << that.level;
        }
    }
    uint64_t output_value(void)
    {
        if (level == MY_INFINITY) 
            return numeric_limits<int64_t>::max();
        else
            return level;
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
    arg.add_arg("root","0","root/starting vertex");
    arg.add_arg("output", "", "Absolute path to the file where the output will be stored");
}
//==============================================================//


inline unsigned vertex_distributor(uint64_t vid, unsigned threadnum)
{
    return vid%threadnum;
}
#ifdef USE_CSR
void parallel_bfs(graph_t& g, size_t root, unsigned threadnum, gBenchPerf_multi & perf, int perf_group)
{
    // initializzation
    g.csr_vertex_property(root).level = 0;

    vector<vector<uint64_t> > global_input_tasks(threadnum);
    global_input_tasks[vertex_distributor(root, threadnum)].push_back(root);
    
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
                uint16_t curr_level = g.csr_vertex_property(vid).level;
                uint64_t edges_begin = g.csr_out_edges_begin(vid);
                uint64_t size = g.csr_out_edges_size(vid);

                for (unsigned i=0;i<size;i++)
                {
                    uint64_t dest_vid = g.csr_out_edge(edges_begin, i);
                    if (__sync_bool_compare_and_swap(&(g.csr_vertex_property(dest_vid).level), 
                                MY_INFINITY,curr_level+1))
                    {
                        global_output_tasks[vertex_distributor(dest_vid,threadnum)+tid*threadnum].push_back(dest_vid);
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

#else
void parallel_bfs(graph_t& g, size_t root, unsigned threadnum, gBenchPerf_multi & perf, int perf_group)
{
    // initializzation
    vertex_iterator rootvit=g.find_vertex(root);
    if (rootvit==g.vertices_end()) return;

    rootvit->property().level = 0;

    vector<vector<uint64_t> > global_input_tasks(threadnum);
    global_input_tasks[vertex_distributor(root, threadnum)].push_back(root);

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
                uint32_t curr_level = vit->property().level;

                for (edge_iterator eit=vit->edges_begin();eit!=vit->edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator destvit = g.find_vertex(dest_vid);
                    if (__sync_bool_compare_and_swap(&(destvit->property().level),
                                                     MY_INFINITY,curr_level+1))
                    {
                        global_output_tasks[vertex_distributor(dest_vid,threadnum)+tid*threadnum].push_back(dest_vid);
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
#endif

void output(graph_t& g)
{
    cout<<"BFS Results: \n";
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        cout<<vit->id()<<" "<<vit->property().level<<"\n";
    }
}

void reset_graph(graph_t & g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().color = COLOR_WHITE;
        vit->property().order = 0;
        vit->property().level = MY_INFINITY;
    }

}

//==============================================================//
int main(int argc, char * argv[])
{
    graphBIG::print();
    cout<<"Benchmark: BFS\n";
	 cout<<"Benchmark: BFS\n";


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

    size_t root,threadnum;
    arg.get_value("root",root);
    arg.get_value("threadnum",threadnum);

    graph_t graph;
    double t1, t2;

    cout<<"loading data... \n";

#ifdef GRANULA
    cout<<loadGraph.getOperationInfo("StartTime", loadGraph.getEpoch())<<endl;
#endif

    t1 = timer::get_usec();
    string vfile = path + "/vertex.csv";
    string efile = path + "/edge.csv";

#ifdef USE_CSR
    if (!graph.load_CSR_Graph(path))
        return -1;
#else
    if (!graph.load_csv_vertices(vfile, false, " ", 0))
        return -1;
    if (!graph.load_csv_edges(efile, false, " ", 0, 1))
        return -1;
#endif
    size_t vertex_num = graph.vertex_num();
    size_t edge_num = graph.edge_num();
    t2 = timer::get_usec();
    cout<<"== "<<vertex_num<<" vertices  "<<edge_num<<" edges\n";

#ifdef GRANULA
    cout<<"== time: "<<t2-t1<<" sec\n";
    cout<<loadGraph.getOperationInfo("EndTime", loadGraph.getEpoch())<<endl;
#endif

#ifdef USE_CSR
    uint64_t newroot;

    if (!csr_external_to_internal_id(threadnum, graph, root, newroot)) {
        cerr << "failed find vertex with external id: " << root << endl;
        return 1;
    }

    root = newroot;
#endif

    cout<<"\nBFS root: "<<root<<"\n";

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
        parallel_bfs(graph, root, threadnum, perf_multi, i);
        t2 = timer::get_usec();
        elapse_time += t2-t1;
        if ((i+1)<run_num) reset_graph(graph);
    }
    cout<<"BFS finish: \n";

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
#ifdef USE_CSR
        write_csr_graph_vertices(graph, output_file);
#else
        write_graph_vertices(graph, output_file);
#endif
    }

#ifdef GRANULA
    cout<<offloadGraph.getOperationInfo("EndTime", offloadGraph.getEpoch())<<endl;
    cout<<opengJob.getOperationInfo("EndTime", opengJob.getEpoch())<<endl;
#endif


    cout<<"=================================================================="<<endl;
    return 0;
}  // end main
