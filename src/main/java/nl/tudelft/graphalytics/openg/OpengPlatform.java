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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import nl.tudelft.granula.archiver.PlatformArchive;
import nl.tudelft.granula.modeller.job.JobModel;
import nl.tudelft.granula.modeller.platform.Openg;
import nl.tudelft.graphalytics.BenchmarkMetrics;
import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.granula.GranulaAwarePlatform;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import nl.tudelft.graphalytics.domain.graph.PropertyList;
import nl.tudelft.graphalytics.domain.graph.PropertyType;
import nl.tudelft.graphalytics.openg.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.openg.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.openg.algorithms.lcc.LocalClusteringCoefficientJob;
import nl.tudelft.graphalytics.openg.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.openg.algorithms.sssp.SingleSourceShortestPathsJob;
import nl.tudelft.graphalytics.openg.algorithms.wcc.WeaklyConnectedComponentsJob;
import nl.tudelft.graphalytics.openg.config.JobConfiguration;
import nl.tudelft.graphalytics.openg.config.JobConfigurationParser;
import org.json.simple.JSONObject;

/**
 * OpenG platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public class OpengPlatform implements GranulaAwarePlatform {


	private static PrintStream console;

	protected static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "openg";
	public static final String PROPERTIES_FILENAME = PLATFORM_NAME + ".properties";

	public static final String OPENG_INTERMEDIATE_DIR_KEY = "openg.intermediate-dir";

	public static String OPENG_BINARY_DIRECTORY = "./bin/standard";

	protected Configuration opengConfig;
	protected JobConfiguration jobConfiguration;
	protected String intermediateDirectory;
	protected String graphOutputDirectory;

	protected String intermediatePath;
	protected String currentGraphPath;
	protected Long2LongMap currentGraphVertexIdTranslation;

	public OpengPlatform() {
		try {
			loadConfiguration();
		} catch (InvalidConfigurationException e) {
			// TODO: Implement cleaner exit procedure
			LOG.fatal(e);
			System.exit(-1);
		}
		OPENG_BINARY_DIRECTORY = "./bin/granula";
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

		intermediateDirectory = ConfigurationUtil.getString(opengConfig, OPENG_INTERMEDIATE_DIR_KEY);
		ensureDirectoryExists(intermediateDirectory, OPENG_INTERMEDIATE_DIR_KEY);
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

	private static boolean deleteDirectory(File dir) {
	    if(! dir.exists() || !dir.isDirectory())    {
	        return false;
	    }

	    String[] files = dir.list();
	    for(int i = 0, len = files.length; i < len; i++)    {
	        File f = new File(dir, files[i]);
	        if(f.isDirectory()) {
	            deleteDirectory(f);
	        } else {
	            f.delete();
	        }
	    }

	    return dir.delete();
	}

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		LOG.info("Preprocessing graph \"{}\".", graph.getName());

		if (graph.hasVertexProperties()) {
			throw new IllegalArgumentException("OpenG does not support vertices with properties");
		}

		if (graph.hasEdgeProperties()) {
			PropertyList list = graph.getEdgeProperties();

			if (list.size() > 1) {
				throw new IllegalArgumentException("OpenG does not support more than one edge property");
			}

            PropertyType type = list.get(0).getType();

			if (!type.equals(PropertyType.REAL)) {
				throw new IllegalArgumentException("OpenG does not support edge properties of type: "
						+ type);
			}
		}

		File dir = new File(intermediateDirectory + "/" + graph.getName());

		if (dir.exists() && dir.isDirectory()) {
			if (!deleteDirectory(dir)) {
				throw new Exception("Failed to delete existing directory :" + dir);
			}
		}

		LOG.info("Creating intermediate directory: " + dir);
		if (!dir.mkdirs()) {
			throw new Exception("Failed to create intermediate directory :" + dir);
		}

		String vertexPath = dir + "/vertex.csv";
		LOG.info("Creating symbolic link: " + graph.getVertexFilePath() + " -> " + vertexPath);
		Files.createSymbolicLink(Paths.get(vertexPath), Paths.get(graph.getVertexFilePath()));

		String edgePath = dir + "/edge.csv";
		LOG.info("Creating symbolic link: " + graph.getEdgeFilePath() + " -> " + edgePath);
		Files.createSymbolicLink(Paths.get(edgePath), Paths.get(graph.getEdgeFilePath()));

		CommandLine cmd = new CommandLine(OPENG_BINARY_DIRECTORY + "/genCSR");
		cmd.addArgument("--dataset");
		cmd.addArgument(dir.getAbsolutePath());
		cmd.addArgument("--outpath");
		cmd.addArgument(dir.getAbsolutePath());
		cmd.addArgument("--undirected");
		cmd.addArgument(graph.isDirected() ? "0" : "1");
		cmd.addArgument("--weight");
		cmd.addArgument(graph.hasEdgeProperties() ? "1" : "0");

		DefaultExecutor executor = new DefaultExecutor();
		executor.setExitValue(0);

		LOG.info("Executing command: " + cmd.toString());
		executor.execute(cmd);

		currentGraphPath = dir.getAbsolutePath();
	}

	private void setupGraph(Graph graph) {
		File dir = new File(intermediateDirectory + "/" + graph.getName());
		currentGraphPath = dir.getAbsolutePath();
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {

		setupGraph(benchmark.getGraph());

		Algorithm algorithm = benchmark.getAlgorithm();
		Graph graph = benchmark.getGraph();
		Object parameters = benchmark.getAlgorithmParameters();

		OpengJob job;
		switch (algorithm) {
			case BFS:
				long sourceVertex = ((BreadthFirstSearchParameters)parameters).getSourceVertex();
				job = new BreadthFirstSearchJob(sourceVertex, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmark.getId());
				break;
			case CDLP:
				long maxIteration = ((CommunityDetectionLPParameters)parameters).getMaxIterations();
				job = new CommunityDetectionLPJob(maxIteration, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmark.getId());
				break;
			case LCC:
				job = new LocalClusteringCoefficientJob(jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmark.getId());
				break;
			case PR:
				float dampingFactor = ((PageRankParameters)parameters).getDampingFactor();
				long iteration = ((PageRankParameters)parameters).getNumberOfIterations();
				job = new PageRankJob(iteration, dampingFactor, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmark.getId());
				break;
			case WCC:
				job = new WeaklyConnectedComponentsJob(jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmark.getId());
				break;
			case SSSP:
				sourceVertex = ((SingleSourceShortestPathsParameters)parameters).getSourceVertex();
				job = new SingleSourceShortestPathsJob(sourceVertex, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmark.getId());
				break;
			default:
				throw new PlatformExecutionException("Not yet implemented.");
		}

		LOG.info("Executing algorithm \"{}\" on graph \"{}\".", algorithm.getName(), graph.getName());

		try {
			if (benchmark.isOutputRequired()) {
				job.setOutputPath(benchmark.getOutputPath());
			}

			int exitCode = job.execute();

			if (exitCode != 0) {
				throw new PlatformExecutionException("OpenG completed with a non-zero exit code: " + exitCode);
			}
		} catch(IOException e) {
			throw new PlatformExecutionException("Failed to launch OpenG", e);
		}

		return new PlatformBenchmarkResult(NestedConfiguration.empty());
	}

	@Override
	public void deleteGraph(String graphName) {
		if (!deleteDirectory(new File(currentGraphPath))) {
			LOG.warn("Failed to delete intermediate directory: " + currentGraphPath);
		}
	}

	@Override
	public BenchmarkMetrics retrieveMetrics() {
		return new BenchmarkMetrics();
	}

	@Override
	public String getName() {
		return PLATFORM_NAME;
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		return NestedConfiguration.fromExternalConfiguration(opengConfig, PROPERTIES_FILENAME);
	}


	@Override
	public void preBenchmark(Benchmark benchmark, Path logDirectory) {
		startPlatformLogging(logDirectory.resolve("platform").resolve("driver.logs"));
	}

	@Override
	public void postBenchmark(Benchmark benchmark, Path logDirectory) {
		stopPlatformLogging();
	}


	@Override
	public JobModel getJobModel() {
		return new JobModel(new Openg());
	}


	public static void startPlatformLogging(Path fileName) {
		console = System.out;
		try {
			File file = null;
			file = fileName.toFile();
			file.getParentFile().mkdirs();
			file.createNewFile();
			FileOutputStream fos = new FileOutputStream(file);
			PrintStream ps = new PrintStream(fos);
			System.setOut(ps);
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("cannot redirect to output file");
		}
		System.out.println("StartTime: " + System.currentTimeMillis());
	}

	public static void stopPlatformLogging() {
		System.setOut(console);
	}


	@Override
	public void enrichMetrics(BenchmarkResult benchmarkResult, Path arcDirectory) {
		try {
			PlatformArchive platformArchive = PlatformArchive.readArchive(arcDirectory);
			JSONObject processGraph = platformArchive.operation("ProcessGraph");
			Integer procTime = Integer.parseInt(platformArchive.info(processGraph, "Duration"));
			BenchmarkMetrics metrics = benchmarkResult.getMetrics();
			metrics.setProcessingTime(procTime);
		} catch(Exception e) {
			LOG.error("Failed to enrich metrics.");
		}
	}

}
