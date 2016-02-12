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

import it.unimi.dsi.fastutil.longs.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.regex.Pattern;

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
		LongList sortedVertexIds = new LongArrayList(numberOfVertices);
		try (BufferedReader vertexData = new BufferedReader(new FileReader(vertexFilePath))) {
			long nextVertexId = 0;
			for (String vertexLine = vertexData.readLine(); vertexLine != null; vertexLine = vertexData.readLine()) {
				if (vertexLine.isEmpty()) {
					continue;
				}

				long vertexId = Long.parseLong(vertexLine);
				vertexIdTranslation.put(vertexId, nextVertexId);
				sortedVertexIds.add(vertexId);
				nextVertexId++;
			}
		}

		LOG.debug("- Writing OpenG csv format");

		if(!(new File(outputPath)).exists()) {
			(new File(outputPath)).mkdirs();
		}

		try (BufferedReader edgeData = new BufferedReader(new FileReader(edgeFilePath));
			 PrintWriter vertexListWriter = new PrintWriter(new File(outputPath + "/vertex.csv"))) {
				 vertexListWriter.print("id");
				 vertexListWriter.println();
				parseVertices(sortedVertexIds, vertexIdTranslation, vertexListWriter);
		}

		try (BufferedReader edgeData = new BufferedReader(new FileReader(edgeFilePath));
			 PrintWriter edgeListWriter = new PrintWriter(new File(outputPath + "/edge.csv"))) {
			edgeListWriter.print("id|id");
			edgeListWriter.println();
			if (isGraphDirected) {
				parseDirectedEdges(edgeData, vertexIdTranslation, sortedVertexIds, edgeListWriter);
			} else {
				parseUndirectedEdges(edgeData, vertexIdTranslation, sortedVertexIds, edgeListWriter);
			}
		}

		return vertexIdTranslation;
	}


	private static void parseVertices(LongList sortedVertexIds, Long2LongMap vertexIdTranslation,
									  PrintWriter vertexListWriter) throws IOException {
		Pattern delimiterPattern = Pattern.compile(" ");

		for (Long sortedVertexId : sortedVertexIds) {
			vertexListWriter.print(vertexIdTranslation.get(sortedVertexId));
			vertexListWriter.println();
		}
	}


	private static void parseDirectedEdges(BufferedReader edgeData, Long2LongMap vertexIdTranslation,
										   LongList sortedVertexIds, PrintWriter edgeListWriter) throws IOException {
		Pattern delimiterPattern = Pattern.compile(" ");

		for (String edgeLine = edgeData.readLine(); edgeLine != null; edgeLine = edgeData.readLine()) {
			if (edgeLine.isEmpty()) {
				continue;
			}

			String[] edgeLineChunks = delimiterPattern.split(edgeLine);
			if (edgeLineChunks.length != 2) {
				throw new IOException("Invalid data found in edge list: \"" + edgeLine + "\"");
			}

			long sourceId = Long.parseLong(edgeLineChunks[0]);
			long destinationId = Long.parseLong(edgeLineChunks[1]);

			edgeListWriter.print(vertexIdTranslation.get(sourceId));
			edgeListWriter.append('|').print(vertexIdTranslation.get(destinationId));
			edgeListWriter.println();
		}
	}

	private static void parseUndirectedEdges(BufferedReader edgeData, Long2LongMap vertexIdTranslation,
											 LongList sortedVertexIds, PrintWriter edgeListWriter) throws IOException {
		Pattern delimiterPattern = Pattern.compile(" ");

		for (String edgeLine = edgeData.readLine(); edgeLine != null; edgeLine = edgeData.readLine()) {
			if (edgeLine.isEmpty()) {
				continue;
			}

			String[] edgeLineChunks = delimiterPattern.split(edgeLine);
			if (edgeLineChunks.length != 2) {
				throw new IOException("Invalid data found in edge list: \"" + edgeLine + "\"");
			}

			long sourceId = Long.parseLong(edgeLineChunks[0]);
			long destinationId = Long.parseLong(edgeLineChunks[1]);

			edgeListWriter.print(vertexIdTranslation.get(sourceId));
			edgeListWriter.append('|').print(vertexIdTranslation.get(destinationId));
			edgeListWriter.println();

			edgeListWriter.print(vertexIdTranslation.get(destinationId));
			edgeListWriter.append('|').print(vertexIdTranslation.get(sourceId));
			edgeListWriter.println();
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