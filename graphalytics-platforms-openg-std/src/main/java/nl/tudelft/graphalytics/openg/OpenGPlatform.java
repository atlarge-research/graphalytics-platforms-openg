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

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import nl.tudelft.graphalytics.domain.Algorithm;
import nl.tudelft.graphalytics.domain.Benchmark;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.NestedConfiguration;
import nl.tudelft.graphalytics.domain.PlatformBenchmarkResult;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.openg.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.openg.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.openg.algorithms.lcc.LocalClusteringCoefficientJob;
import nl.tudelft.graphalytics.openg.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.openg.algorithms.sssp.SingleSourceShortestPathsJob;
import nl.tudelft.graphalytics.openg.algorithms.wcc.WeaklyConnectedComponentsJob;
import nl.tudelft.graphalytics.openg.config.JobConfiguration;
import nl.tudelft.graphalytics.openg.config.JobConfigurationParser;

/**
 * OpenG platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public class OpenGPlatform implements Platform {

	protected static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "openg";
	public static final String PROPERTIES_FILENAME = PLATFORM_NAME + ".properties";

	public static final String OPENG_INTERMEDIATE_DIR_KEY = "openg.intermediate-dir";
	public static final String OPENG_OUTPUT_DIR_KEY = "openg.output-dir";

	public static String OPENG_BINARY_DIRECTORY = "./bin/standard";

	protected Configuration opengConfig;
	protected JobConfiguration jobConfiguration;
	protected String intermediateGraphDirectory;
	protected String graphOutputDirectory;

	protected String currentGraphPath;
	protected Long2LongMap currentGraphVertexIdTranslation;

	public OpenGPlatform() {
		try {
			loadConfiguration();
		} catch (InvalidConfigurationException e) {
			// TODO: Implement cleaner exit procedure
			LOG.fatal(e);
			System.exit(-1);
		}
	}

	protected void loadConfiguration() throws InvalidConfigurationException {
		LOG.info("Parsing OpenG configuration file.");

		// Load OpenG-specific configuration
		try {
			opengConfig = new PropertiesConfiguration(PROPERTIES_FILENAME);
		} catch (ConfigurationException e) {
			// Fall-back to an empty properties file
			LOG.warn("Could not find or load \"{}\"", PROPERTIES_FILENAME);
			opengConfig = new PropertiesConfiguration();
		}

		// Parse generic job configuration from the OpenG properties file
		jobConfiguration = JobConfigurationParser.parseOpenGPropertiesFile(opengConfig);

		intermediateGraphDirectory = ConfigurationUtil.getString(opengConfig, OPENG_INTERMEDIATE_DIR_KEY);
		graphOutputDirectory = ConfigurationUtil.getString(opengConfig, OPENG_OUTPUT_DIR_KEY);

		ensureDirectoryExists(intermediateGraphDirectory, OPENG_INTERMEDIATE_DIR_KEY);
		ensureDirectoryExists(graphOutputDirectory, OPENG_OUTPUT_DIR_KEY);
	}

	private String createIntermediateFile(String name) throws IOException {
		return Paths.get(intermediateGraphDirectory, name).toString();
	}

	private void tryDeleteIntermediateFile(String path) {
		if (!new File(path).delete()) {
			LOG.warn("failed to delete intermediate file '{}'", path);
		}
	}

	protected static void ensureDirectoryExists(String directory, String property) throws InvalidConfigurationException {
		File directoryFile = new File(directory);
		if (directoryFile.exists()) {
			if (!directoryFile.isDirectory()) {
				throw new InvalidConfigurationException("Path \"" + directory + "\" set as property \"" + property +
						"\" already exists, but is not a directory");
			}
			return;
		}

		if (!directoryFile.mkdirs()) {
			throw new InvalidConfigurationException("Unable to create directory \"" + directory +
					"\" set as property \"" + property + "\"");
		}
		LOG.info("Created directory \"{}\" and any missing parent directories", directory);
	}

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		LOG.info("Preprocessing graph \"{}\".", graph.getName());

		//TODO check if this is true.
		if (graph.getNumberOfVertices() > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Graphalytics for OpenG does not currently support graphs with more than " + Integer.MAX_VALUE + " vertices");
		}

		currentGraphPath = createIntermediateFile(graph.getName() + ".txt");

		currentGraphVertexIdTranslation = GraphParser.parseGraphAndWriteAdjacencyList(
				graph.getVertexFilePath(),
				graph.getEdgeFilePath(),
				currentGraphPath,
				graph.isDirected(),
				(int)graph.getNumberOfVertices());
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {

		Algorithm algorithm = benchmark.getAlgorithm();
		Graph graph = benchmark.getGraph();
		Object parameters = benchmark.getAlgorithmParameters();

		OpenGJob job;
		String outputGraphPath = Paths.get(graphOutputDirectory, graph.getName() + "-" + algorithm).toString();
		switch (algorithm) {
			case BFS:
				long sourceVertex = ((BreadthFirstSearchParameters)parameters).getSourceVertex();
				sourceVertex = currentGraphVertexIdTranslation.get(sourceVertex);
				job = new BreadthFirstSearchJob(sourceVertex, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath);
				break;
			case CDLP:
				long maxIteration = ((CommunityDetectionLPParameters)parameters).getMaxIterations();
				job = new CommunityDetectionLPJob(maxIteration, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath);
				break;
			case LCC:
				job = new LocalClusteringCoefficientJob(jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath);
				break;
			case PR:
				float dampingFactor = ((PageRankParameters)parameters).getDampingFactor();
				long iteration = ((PageRankParameters)parameters).getNumberOfIterations();
				job = new PageRankJob(iteration, dampingFactor, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath);
				break;
			case WCC:
				job = new WeaklyConnectedComponentsJob(jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath);
				break;
			case SSSP:
				sourceVertex = ((SingleSourceShortestPathsParameters)parameters).getSourceVertex();
				sourceVertex = currentGraphVertexIdTranslation.get(sourceVertex);
				job = new SingleSourceShortestPathsJob(sourceVertex, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath);
				break;
			default:
				// TODO: Implement other algorithms
				throw new PlatformExecutionException("Not yet implemented.");
		}

		LOG.info("Executing algorithm \"{}\" on graph \"{}\".", algorithm.getName(), graph.getName());

		String intermediateOutputPath = null;

		try {
			if (benchmark.isOutputRequired()) {
				intermediateOutputPath = createIntermediateFile("output.txt");
				job.setOutputPath(intermediateOutputPath);
			}

			int exitCode = job.execute();

			if (exitCode != 0) {
				throw new PlatformExecutionException("OpenG completed with a non-zero exit code: " + exitCode);
			}

			if (benchmark.isOutputRequired()) {
				OutputConverter.parseAndWrite(
						intermediateOutputPath,
						benchmark.getOutputPath(),
						currentGraphVertexIdTranslation);
			}
		} catch(IOException e) {
			throw new PlatformExecutionException("Failed to launch OpenG", e);
		} finally {
			if (intermediateOutputPath != null) {
				tryDeleteIntermediateFile(intermediateOutputPath);
			}
		}

		return new PlatformBenchmarkResult(NestedConfiguration.empty());
	}

	@Override
	public void deleteGraph(String graphName) {
		tryDeleteIntermediateFile(currentGraphPath);
	}

	@Override
	public String getName() {
		return PLATFORM_NAME;
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		return NestedConfiguration.fromExternalConfiguration(opengConfig, PROPERTIES_FILENAME);
	}

}
