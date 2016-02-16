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
package nl.tudelft.graphalytics.openg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

/**
 * Utility class for converting graphs in Graphalytics' VE format to the adjacency list format supported by PGX.DIST.
 * In addition, vertex IDs are normalized to range [0, number of vertices) to ensure compatibility with PGX.DIST.
 */
public final class GraphParser {

	private static final Logger LOG = LogManager.getLogger();

	private final String vertexFilePath;
	private final String edgeFilePath;
	private final String outputPath;
	private final boolean isGraphDirected;
	private final int numberOfVertices;

	private GraphParser(String vertexFilePath, String edgeFilePath, String outputPath, boolean isGraphDirected,
						int numberOfVertices) {
		this.vertexFilePath = vertexFilePath;
		this.edgeFilePath = edgeFilePath;
		this.outputPath = outputPath;
		this.isGraphDirected = isGraphDirected;
		this.numberOfVertices = numberOfVertices;
	}

	private Long2LongMap parseAndWrite() throws IOException {
		LOG.debug("- Reading vertex list to construct ID mapping");
		Long2LongMap vertexIdTranslation = new Long2LongOpenHashMap(numberOfVertices);

		LOG.debug("- Writing OpenG csv format");

		if(!(new File(outputPath)).exists()) {
			(new File(outputPath)).mkdirs();
		}

		try (BufferedReader vertexData = new BufferedReader(new FileReader(vertexFilePath));
			 PrintWriter vertexListWriter = new PrintWriter(new File(outputPath + "/vertex.csv"))) {
				parseVertices(vertexData, vertexIdTranslation, vertexListWriter);
		}

		try (BufferedReader edgeData = new BufferedReader(new FileReader(edgeFilePath));
			 PrintWriter edgeListWriter = new PrintWriter(new File(outputPath + "/edge.csv"))) {
			parseEdges(edgeData, vertexIdTranslation, edgeListWriter, isGraphDirected);
		}

		return vertexIdTranslation;
	}


	private static void parseVertices(BufferedReader vertexData, Long2LongMap vertexIdTranslation,
									  PrintWriter vertexListWriter) throws IOException {
		Pattern delimiterPattern = Pattern.compile(" ");
		long nextVertexId = 0;

		for (String vertexLine = vertexData.readLine(); vertexLine != null; vertexLine = vertexData.readLine()) {
			if (vertexLine.isEmpty()) {
				continue;
			}

			String[] vertexLineChunks = delimiterPattern.split(vertexLine, 2);

			long vertexId = Long.parseLong(vertexLineChunks[0]);
			vertexIdTranslation.put(vertexId, nextVertexId++);

			vertexListWriter.print(vertexIdTranslation.get(vertexId));
			if (vertexLineChunks.length > 1) {
				vertexListWriter.append(' ').print(vertexLineChunks[1]);
			}
			vertexListWriter.println();
		}
	}

	private static void parseEdges(BufferedReader edgeData, Long2LongMap vertexIdTranslation,
											 PrintWriter edgeListWriter, boolean directed) throws IOException {
		Pattern delimiterPattern = Pattern.compile(" ");

		for (String edgeLine = edgeData.readLine(); edgeLine != null; edgeLine = edgeData.readLine()) {
			if (edgeLine.isEmpty()) {
				continue;
			}

			String[] edgeLineChunks = delimiterPattern.split(edgeLine, 3);

			long sourceId = Long.parseLong(edgeLineChunks[0]);
			long destinationId = Long.parseLong(edgeLineChunks[1]);

			edgeListWriter.print(vertexIdTranslation.get(sourceId));
			edgeListWriter.append(' ');
			edgeListWriter.print(vertexIdTranslation.get(destinationId));
			if (edgeLineChunks.length > 2) {
				edgeListWriter.append(' ').print(edgeLineChunks[2]);
			}
			edgeListWriter.println();

			if (!directed) {
				edgeListWriter.print(vertexIdTranslation.get(destinationId));
				edgeListWriter.append(' ');
				edgeListWriter.print(vertexIdTranslation.get(sourceId));
				if (edgeLineChunks.length > 2) {
					edgeListWriter.append(' ').print(edgeLineChunks[2]);
				}
				edgeListWriter.println();
			}
		}
	}

	/**
	 * Parses a graph in Graphalytics' VE format, writes the graph to a new file in adjacency list format, and returns
	 * a mapping of vertex ids from the original graph to vertex ids in the output graph.
	 *
	 * @param vertexFilePath   the path of the vertex list for the input graph
	 * @param edgeFilePath     the path of the edge list for the input graph
	 * @param outputPath       the path to write the graph in adjacency list format to
	 * @param isGraphDirected  true iff the graph is directed
	 * @param numberOfVertices the number of vertices in the graph
	 * @return a mapping of vertex ids from the original graph to vertex ids in the output graph
	 * @throws IOException iff an exception occurred while parsing the input graph or writing the output graph
	 */
	public static Long2LongMap parseGraphAndWriteAdjacencyList(String vertexFilePath, String edgeFilePath,
															   String outputPath, boolean isGraphDirected, int numberOfVertices) throws IOException {
		return new GraphParser(vertexFilePath, edgeFilePath, outputPath, isGraphDirected, numberOfVertices).parseAndWrite();
	}

}