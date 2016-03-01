//====== Graph Benchmark Suites ======//
//========== Shortest Path ===========//
//
// Single-source shortest path
//
// Usage: ./sssp    --dataset <dataset path>
//                  --root <root vertex id>

#include "common.h"
#include "def.h"
#include "omp.h"
#include "openG.h"
#include <limits>
#include <queue>

#include "util.hpp"

#ifdef HMC
#include "HMC.h"
#endif

#ifdef SIM
#include "SIM.h"
#endif

#ifdef GRANULA
#include "granula.hpp"
#endif

using namespace std;
size_t beginiter = 0;
size_t enditer = 0;

typedef double distance_t;
#define MY_INFINITY (numeric_limits<distance_t>::max())

class vertex_property
{
public:
    vertex_property():distance(MY_INFINITY),update(MY_INFINITY){}

    distance_t distance;
    distance_t update;

    friend ostream& operator<< (ostream &strm, const vertex_property &that) {

        // According to Graphalytics specifications, SSSP should output
        // the string 'infinity' if a vertex is unreachable.
        if (that.distance == MY_INFINITY) {
            return strm << "infinity";
        } else {
            return strm << that.distance;
        }
    }
    distance_t output_value(void)
    {
        return distance;
    }
};
class edge_property
{
public:
    edge_property():weight(0.0){}

    distance_t weight;
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
typedef pair<size_t,size_t> data_pair;
class comp
{
public:
    bool operator()(data_pair a, data_pair b)
    {
        return a.second > b.second;
    }
};

/*
void sssp(graph_t& g, size_t src, gBenchPerf_event & perf, int perf_group)
{
    priority_queue<data_pair, vector<data_pair>, comp> PQ;

    perf.open(perf_group);
    perf.start(perf_group);
#ifdef SIM
    SIM_BEGIN(true);
#endif
    // initialize
    vertex_iterator src_vit = g.find_vertex(src);
    src_vit->property().distance = 0;
    PQ.push(data_pair(src,0));

    // for every un-visited vertex, try relaxing the path
    while (!PQ.empty())
    {
        size_t u = PQ.top().first;
        PQ.pop();

        vertex_iterator u_vit = g.find_vertex(u);

        for (edge_iterator eit = u_vit->edges_begin(); eit != u_vit->edges_end(); eit++)
        {
            size_t v = eit->target();
            vertex_iterator v_vit = g.find_vertex(v);

            distance_t alt = u_vit->property().distance + eit->property().weight;
            if (alt < v_vit->property().distance)
            {
                v_vit->property().distance = alt;
                PQ.push(data_pair(v,alt));
            }
        }
    }
#ifdef SIM
    SIM_END(true);
#endif
    perf.stop(perf_group);
    return;
}
*/

inline unsigned vertex_distributor(uint64_t vid, unsigned threadnum)
{
    return vid%threadnum;
}
#ifdef USE_CSR
void parallel_sssp(graph_t& g, size_t root, unsigned threadnum, gBenchPerf_multi & perf, int perf_group)
{
    g.csr_vertex_property(root).distance = 0;
    g.csr_vertex_property(root).update = 0;

    bool * locks = new bool[g.num_vertices()];
    memset(locks, 0, sizeof(bool)*g.num_vertices());

    vector<vector<uint64_t> > global_input_tasks(threadnum);
    global_input_tasks[vertex_distributor(root,threadnum)].push_back(root);

    vector<vector<uint64_t> > global_output_tasks(threadnum*threadnum);


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
            // process local queue
            stop = true;
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];

                distance_t curr_dist = g.csr_vertex_property(vid).distance;

                uint64_t edges_begin = g.csr_out_edges_begin(vid);
                for (uint64_t i=0;i<g.csr_out_edges_size(vid);i++)
                {
                    uint64_t dest_vid = g.csr_out_edge(edges_begin,i);
                    distance_t new_dist = curr_dist + g.csr_out_edge_weight(edges_begin, i);
                    bool active=false;

                    // spinning lock for critical section
                    //  can be replaced as an atomicMin operation
                    while(__sync_lock_test_and_set(&(locks[dest_vid]),1));
                    if (g.csr_vertex_property(dest_vid).update>new_dist)
                    {
                        active = true;
                        g.csr_vertex_property(dest_vid).update = new_dist;
                    }
                    __sync_lock_release(&(locks[dest_vid]));

                    if (active)
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
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid = input_tasks[i];
                g.csr_vertex_property(vid).distance = g.csr_vertex_property(vid).update;
            }
            #pragma omp barrier
        }
        perf.stop(tid, perf_group);
    }


    delete[] locks;
}

#else
void parallel_sssp(graph_t& g, size_t root, unsigned threadnum, gBenchPerf_multi & perf, int perf_group)
{
    vertex_iterator rootvit=g.find_vertex(root);
    rootvit->property().distance = 0;
    rootvit->property().update = 0;

    //vector<uint16_t> update(g.num_vertices(), MY_INFINITY);
    //update[root] = 0;

    bool * locks = new bool[g.num_vertices()];
    memset(locks, 0, sizeof(bool)*g.num_vertices());

    vector<vector<uint64_t> > global_input_tasks(threadnum);
    global_input_tasks[vertex_distributor(root,threadnum)].push_back(root);

    vector<vector<uint64_t> > global_output_tasks(threadnum*threadnum);


    bool stop = false;
    #pragma omp parallel num_threads(threadnum) shared(stop,global_input_tasks,global_output_tasks)
    {
        unsigned tid = omp_get_thread_num();
        vector<uint64_t> & input_tasks = global_input_tasks[tid];

        perf.open(tid, perf_group);
        perf.start(tid, perf_group);
#ifdef SIM
        unsigned iter = 0;
#endif
        while(!stop)
        {
            #pragma omp barrier
            // process local queue
            stop = true;
#ifdef SIM
            SIM_BEGIN(iter==beginiter);
            iter++;
#endif
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                uint64_t vid=input_tasks[i];
                vertex_iterator vit = g.find_vertex(vid);

                distance_t curr_dist = vit->property().distance;
                for (edge_iterator eit=vit->edges_begin();eit!=vit->edges_end();eit++)
                {
                    uint64_t dest_vid = eit->target();
                    vertex_iterator dvit = g.find_vertex(dest_vid);
                    distance_t new_dist = curr_dist + eit->property().weight;
                    bool active=false;

                    // spinning lock for critical section
                    //  can be replaced as an atomicMin operation
                    while(__sync_lock_test_and_set(&(locks[dest_vid]),1));
                    if (dvit->property().update>new_dist)
                    {
                        active = true;
                        dvit->property().update = new_dist;
                    }
                    __sync_lock_release(&(locks[dest_vid]));

                    if (active)
                    {
                        global_output_tasks[vertex_distributor(dest_vid,threadnum)+tid*threadnum].push_back(dest_vid);
                    }
                }
            }
#ifdef SIM
            SIM_END(iter==enditer);
#endif
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
            for (unsigned i=0;i<input_tasks.size();i++)
            {
                vertex_iterator vit = g.find_vertex(input_tasks[i]);
                vit->property().distance = vit->property().update;
            }
            #pragma omp barrier
        }
#ifdef SIM
        SIM_END(enditer==0);
#endif
        perf.stop(tid, perf_group);
    }


    delete[] locks;
}
#endif
//==============================================================//
void output(graph_t& g)
{
    cout<<"Results: \n";
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        cout<<"== vertex "<<vit->id()<<": distance-";
        if (vit->property().distance == MY_INFINITY)
            cout<<"INF";
        else
            cout<<vit->property().distance;
        cout<<"\n";
    }
    return;
}

void reset_graph(graph_t & g)
{
    vertex_iterator vit;
    for (vit=g.vertices_begin(); vit!=g.vertices_end(); vit++)
    {
        vit->property().distance = MY_INFINITY;
        vit->property().update = MY_INFINITY;
    }

}

bool edge_parser(const string &line, edge_property &prop) {
    const char *start = line.c_str();
    char *end;

    prop.weight = strtod(start, &end);

    if (start >= end) {
        cerr << "erorr while parsing invalid floating-point value: " << line << endl;
        return false;
    }

    return true;
}

//==============================================================//
int main(int argc, char * argv[])
{
    graphBIG::print();
    cout<<"Benchmark: sssp shortest path\n";

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

    size_t root,threadnum;
    arg.get_value("root",root);
    arg.get_value("threadnum",threadnum);

    graph_t graph;
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

    size_t vertex_num = graph.num_vertices();
    size_t edge_num = graph.num_edges();
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

    cout<<"Shortest Path: source-"<<root;
    cout<<"...\n";

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

        parallel_sssp(graph, root, threadnum, perf_multi, i);

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

    cout<<"==================================================================\n";
    return 0;
}  // end main


