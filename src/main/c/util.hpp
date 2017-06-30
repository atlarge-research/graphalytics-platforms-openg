/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef UTIL_H
#define UTIL_H

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdint.h>
#include <string>
#include <limits>

#include "openG.h"

template <typename G>
bool write_graph_vertices(G &graph, const std::string &file) {
    typedef typename G::vertex_iterator vertex_iterator;

    std::ofstream f(file.c_str());

    if (!f) {
        std::cerr << "failed to open file: " << file << std::endl;
        return false;
    }

    for (vertex_iterator it = graph.vertices_begin(); it != graph.vertices_end(); it++) {
        f << it->id() << " " << it->property() << "\n";
    }

    f.close();

    if (f.bad()) {
        std::cerr << "error while writing to file: " << file << std::endl;
        return false;
    }

    return true;
}

#ifdef USE_CSR

template <typename G>
bool write_csr_graph_vertices(G &graph, const std::string &file, bool value_convert=false) {
    std::ofstream f(file.c_str());

    if (!f) {
        std::cerr << "failed to open file: " << file << std::endl;
        return false;
    }

    if (value_convert)
    {
        for (uint64_t vid=0;vid<graph.vertex_num();vid++)
        {
            uint64_t value = graph.csr_vertex_property(vid).output_value();
            f << graph.csr_external_id(vid) << " " << graph.csr_external_id(value) << "\n";
        }

    }
    else
    {
        for (uint64_t vid=0;vid<graph.vertex_num();vid++)
        {
            f << graph.csr_external_id(vid) << " " << graph.csr_vertex_property(vid) << "\n";
        }
    }
    f.close();

    if (f.bad()) {
        std::cerr << "error while writing to file: " << file << std::endl;
        return false;
    }

    return true;
}

template <typename G>
bool csr_external_to_internal_id(size_t threadnum, G &graph, uint64_t ext_id, uint64_t &int_id) {
    size_t vertex_num = graph.vertex_num();
    bool success = false;
    uint64_t result_id = true;

    #pragma omp parallel num_threads(threadnum)
    for (uint64_t vid = 0; vid < vertex_num; vid++) {
        if (graph.csr_external_id(vid) == ext_id) {
            #pragma omp critical
            {
                success = true;
                result_id = vid;
            }
        }
    }

    int_id = result_id;
    return success;
}

#endif

#endif
