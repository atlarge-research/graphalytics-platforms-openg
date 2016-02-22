#ifndef UTIL_H
#define UTIL_H

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdint.h>
#include <string>

#include "openG.h"

static bool safe_strtoull(const char **str, size_t &val) {
    const char *before = *str;
    char *after;

    val = strtoull(before, &after, 10);
    *str = after;

    return before < after;
}

static void skip_whitespace(const char **str) {
    while (isspace(**str)) {
        (*str)++;
    }
}

template <typename G, typename P>
bool load_graph_vertices(G &graph, const std::string &file, P *value_parser) {
    typedef typename G::vproperty_t vprop_t;
    typedef typename G::vertex_iterator vertex_t;

    std::ifstream f(file.c_str());

    if (!f) {
        std::cerr << "failed to open file: " << file << std::endl;
        return false;
    }

    std::string line;
    while (getline(f, line)) {
        const char *str = line.c_str();
        uint64_t id;

        skip_whitespace(&str);

        if (*str == '\0' || *str == '#') {
            continue;
        }

        if (!safe_strtoull(&str, id)) {
            std::cerr << "error while parsing line: " << line << std::endl;
            return false;
        }

        skip_whitespace(&str);

        vertex_t vit = graph.add_vertex();

        if (value_parser) {
            vprop_t val;
            if (!(*value_parser)(std::string(str), val)) {
                return false;
            }
            vit->set_property(val);
        }
    }

    f.close();

    if (f.bad()) {
        std::cerr << "erorr while reading: " << file << std::endl;
        return false;
    }

    return true;
}

template <typename G>
bool load_graph_vertices(G &graph, const std::string &file) {
    typedef typename G::vproperty_t vprop_t;
    typedef bool (*parser_t)(const std::string&, vprop_t&);

    return load_graph_vertices(graph, file, (parser_t*) NULL);
}

template <typename G, typename P>
bool load_graph_edges(G &graph, const std::string &file, P *value_parser) {
    typedef typename G::eproperty_t eprop_t;
    typedef typename G::edge_iterator edge_t;

    std::ifstream f(file.c_str());

    if (!f) {
        std::cerr << "failed to open file: " << file << std::endl;
        return false;
    }


    std::string line;
    while (getline(f, line)) {
        const char *str = line.c_str();
        uint64_t src;
        uint64_t dst;

        skip_whitespace(&str);

        if (*str == '\0' || *str == '#') {
            continue;
        }

        if (!safe_strtoull(&str, src)) {
            std::cerr << "error while parsing line: " << line << std::endl;
            return false;
        }

        skip_whitespace(&str);

        if (!safe_strtoull(&str, dst)) {
            std::cerr << "error while parsing line: " << line << std::endl;
            return false;
        }

        skip_whitespace(&str);

        edge_t eit;
        //FIXME:  add_edge should use internal vertex-id,
        //  not external id.
        // Nedd to add an extra mapping (external id -> internal id) here.
        // NOTE: actually openG load_csv_edges covers the same functionality
        //   (check its code comments)
        graph.add_edge(src, dst, eit);

        if (value_parser) {
            eprop_t val;
            if (!(*value_parser)(std::string(str), val)) {
                return false;
            }

            eit->set_property(val);
        }
    }

    f.close();

    if (f.bad()) {
        std::cerr << "error while reading: " << file << std::endl;
        return false;
    }

    return true;
}

template <typename G>
bool load_graph_edges(G &graph, const std::string &file) {
    typedef typename G::eproperty_t eprop_t;
    typedef bool (*parser_t)(const std::string&, eprop_t&);

    return load_graph_edges(graph, file, (parser_t*) NULL);
}

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
template <typename G>
bool write_csr_graph_vertices(G &graph, const std::string &file) {
    std::ofstream f(file.c_str());

    if (!f) {
        std::cerr << "failed to open file: " << file << std::endl;
        return false;
    }

    for (uint64_t vid=0;vid<graph.vertex_num();vid++)
    {
        f << vid << " " << graph.csr_vertex_property(vid) << "\n";
    }

    f.close();

    if (f.bad()) {
        std::cerr << "error while writing to file: " << file << std::endl;
        return false;
    }

    return true;
}


#endif
