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
package science.atlarge.graphalytics.openg;

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
import org.apache.commons.io.output.TeeOutputStream;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;
import science.atlarge.graphalytics.report.result.BenchmarkRunResult;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.granula.GranulaAwarePlatform;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.configuration.ConfigurationUtil;
import science.atlarge.graphalytics.configuration.InvalidConfigurationException;
import science.atlarge.graphalytics.domain.graph.PropertyList;
import science.atlarge.graphalytics.domain.graph.PropertyType;
import science.atlarge.graphalytics.openg.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.openg.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.openg.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.openg.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.openg.algorithms.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.openg.algorithms.wcc.WeaklyConnectedComponentsJob;
import science.atlarge.graphalytics.openg.config.JobConfiguration;
import science.atlarge.graphalytics.openg.config.JobConfigurationParser;
import org.json.simple.JSONObject;
import science.atlarge.graphalytics.domain.algorithms.*;

/**
 * OpenG platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public class OpengPlatform implements GranulaAwarePlatform {


	private static PrintStream sysOut;
	private static PrintStream sysErr;

	protected static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "openg";
	private static final String BENCHMARK_PROPERTIES_FILE = "benchmark.properties";
	private static final String GRANULA_PROPERTIES_FILE = "granula.properties";

	public static final String GRANULA_ENABLE_KEY = "benchmark.run.granula.enabled";
	public static final String OPENG_INTERMEDIATE_DIR_KEY = "platform.openg.intermediate-dir";

	public static String OPENG_BINARY_DIRECTORY = "./bin/standard";

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

	}

	protected void loadConfiguration() throws InvalidConfigurationException {
		LOG.info("Parsing OpenG configuration file.");

		Configuration benchmarkConfig;
		Configuration granulaConfig;

		// Load OpenG-specific configuration
		try {
			benchmarkConfig = ConfigurationUtil.loadConfiguration(BENCHMARK_PROPERTIES_FILE);
			granulaConfig = ConfigurationUtil.loadConfiguration(GRANULA_PROPERTIES_FILE);
		} catch (InvalidConfigurationException e) {
			// Fall-back to an empty properties file
			LOG.warn("Could not find or load \"{}\"", BENCHMARK_PROPERTIES_FILE);
			LOG.warn("Could not find or load \"{}\"", GRANULA_PROPERTIES_FILE);
			benchmarkConfig = new PropertiesConfiguration();
			granulaConfig = new PropertiesConfiguration();
		}

		// Parse generic job configuration from the OpenG properties file
		jobConfiguration = JobConfigurationParser.parseOpenGPropertiesFile(benchmarkConfig);

		intermediateDirectory = ConfigurationUtil.getString(benchmarkConfig, OPENG_INTERMEDIATE_DIR_KEY);
		ensureDirectoryExists(intermediateDirectory, OPENG_INTERMEDIATE_DIR_KEY);

		boolean granulaEnabled = granulaConfig.getBoolean(GRANULA_ENABLE_KEY, false);
		OPENG_BINARY_DIRECTORY = granulaEnabled ? "./bin/granula": OPENG_BINARY_DIRECTORY;
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
	public void uploadGraph(FormattedGraph formattedGraph) throws Exception {
		LOG.info("Preprocessing graph \"{}\".", formattedGraph.getName());

		if (formattedGraph.hasVertexProperties()) {
			throw new IllegalArgumentException("OpenG does not support vertices with properties");
		}

		if (formattedGraph.hasEdgeProperties()) {
			PropertyList list = formattedGraph.getEdgeProperties();

			if (list.size() > 1) {
				throw new IllegalArgumentException("OpenG does not support more than one edge property");
			}

			PropertyType type = list.get(0).getType();

			if (!type.equals(PropertyType.REAL)) {
				throw new IllegalArgumentException("OpenG does not support edge properties of type: "
						+ type);
			}
		}

		File dir = new File(intermediateDirectory + "/" + formattedGraph.getName());

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
		LOG.info("Creating symbolic link: " + formattedGraph.getVertexFilePath() + " -> " + vertexPath);
		Files.createSymbolicLink(Paths.get(vertexPath), Paths.get(formattedGraph.getVertexFilePath()));

		String edgePath = dir + "/edge.csv";
		LOG.info("Creating symbolic link: " + formattedGraph.getEdgeFilePath() + " -> " + edgePath);
		Files.createSymbolicLink(Paths.get(edgePath), Paths.get(formattedGraph.getEdgeFilePath()));

		CommandLine cmd = new CommandLine(OPENG_BINARY_DIRECTORY + "/genCSR");
		cmd.addArgument("--dataset");
		cmd.addArgument(dir.getAbsolutePath());
		cmd.addArgument("--outpath");
		cmd.addArgument(dir.getAbsolutePath());
		cmd.addArgument("--undirected");
		cmd.addArgument(formattedGraph.isDirected() ? "0" : "1");
		cmd.addArgument("--weight");
		cmd.addArgument(formattedGraph.hasEdgeProperties() ? "1" : "0");

		DefaultExecutor executor = new DefaultExecutor();
		executor.setExitValue(0);

		LOG.info("Executing command: " + cmd.toString());
		executor.execute(cmd);

		currentGraphPath = dir.getAbsolutePath();
	}

	private void setupGraph(FormattedGraph formattedGraph) {
		File dir = new File(intermediateDirectory + "/" + formattedGraph.getName());
		currentGraphPath = dir.getAbsolutePath();
	}

	@Override
	public boolean execute(BenchmarkRun benchmarkRun) throws PlatformExecutionException {

		setupGraph(benchmarkRun.getFormattedGraph());

		Algorithm algorithm = benchmarkRun.getAlgorithm();
		FormattedGraph formattedGraph = benchmarkRun.getFormattedGraph();
		Object parameters = benchmarkRun.getAlgorithmParameters();

		OpengJob job;
		switch (algorithm) {
			case BFS:
				long sourceVertex = ((BreadthFirstSearchParameters)parameters).getSourceVertex();
				job = new BreadthFirstSearchJob(sourceVertex, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmarkRun.getId());
				break;
			case CDLP:
				long maxIteration = ((CommunityDetectionLPParameters)parameters).getMaxIterations();
				job = new CommunityDetectionLPJob(maxIteration, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmarkRun.getId());
				break;
			case LCC:
				job = new LocalClusteringCoefficientJob(jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmarkRun.getId());
				break;
			case PR:
				float dampingFactor = ((PageRankParameters)parameters).getDampingFactor();
				long iteration = ((PageRankParameters)parameters).getNumberOfIterations();
				job = new PageRankJob(iteration, dampingFactor, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmarkRun.getId());
				break;
			case WCC:
				job = new WeaklyConnectedComponentsJob(jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmarkRun.getId());
				break;
			case SSSP:
				sourceVertex = ((SingleSourceShortestPathsParameters)parameters).getSourceVertex();
				job = new SingleSourceShortestPathsJob(sourceVertex, jobConfiguration, OPENG_BINARY_DIRECTORY, currentGraphPath, benchmarkRun.getId());
				break;
			default:
				throw new PlatformExecutionException("Not yet implemented.");
		}

		LOG.info("Executing algorithm \"{}\" on graph \"{}\".", algorithm.getName(), formattedGraph.getName());

		try {
			if (benchmarkRun.isOutputRequired()) {
				Path outputFile = benchmarkRun.getOutputDir().resolve(benchmarkRun.getName());
				job.setOutputPath(outputFile.toAbsolutePath().toString());
			}

			int exitCode = job.execute();

			if (exitCode != 0) {
				throw new PlatformExecutionException("OpenG completed with a non-zero exit code: " + exitCode);
			}
		} catch(IOException e) {
			throw new PlatformExecutionException("Failed to launch OpenG", e);
		}

		return true;
	}

	@Override
	public void deleteGraph(FormattedGraph formattedGraph) {
		if (!deleteDirectory(new File(currentGraphPath))) {
			LOG.warn("Failed to delete intermediate directory: " + currentGraphPath);
		}
	}


	@Override
	public String getPlatformName() {
		return PLATFORM_NAME;
	}

	@Override
	public void prepare(BenchmarkRun benchmarkRun) {

	}

	@Override
	public void preprocess(BenchmarkRun benchmarkRun) {
		startPlatformLogging(benchmarkRun.getLogDir().resolve("platform").resolve("driver.logs"));
	}

	@Override
	public void cleanup(BenchmarkRun benchmarkRun) {

	}

	@Override
	public BenchmarkMetrics postprocess(BenchmarkRun benchmarkRun) {
		stopPlatformLogging();
		return new BenchmarkMetrics();
	}


	@Override
	public JobModel getJobModel() {
		return new JobModel(new Openg());
	}


	public static void startPlatformLogging(Path fileName) {
		sysOut = System.out;
		sysErr = System.err;
		try {
			File file = null;
			file = fileName.toFile();
			file.getParentFile().mkdirs();
			file.createNewFile();
			FileOutputStream fos = new FileOutputStream(file);
			TeeOutputStream bothStream =new TeeOutputStream(System.out, fos);
			PrintStream ps = new PrintStream(bothStream);
			System.setOut(ps);
			System.setErr(ps);
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException("cannot redirect to output file");
		}
		System.out.println("StartTime: " + System.currentTimeMillis());
	}

	public static void stopPlatformLogging() {
		System.setOut(sysOut);
		System.setErr(sysErr);
	}


	@Override
	public void enrichMetrics(BenchmarkRunResult benchmarkRunResult, Path arcDirectory) {
		try {
			PlatformArchive platformArchive = PlatformArchive.readArchive(arcDirectory);
			JSONObject processGraph = platformArchive.operation("ProcessGraph");
			Integer procTime = Integer.parseInt(platformArchive.info(processGraph, "Duration"));
			BenchmarkMetrics metrics = benchmarkRunResult.getMetrics();
			metrics.setProcessingTime(procTime);
		} catch(Exception e) {
			LOG.error("Failed to enrich metrics.");
		}
	}

}
