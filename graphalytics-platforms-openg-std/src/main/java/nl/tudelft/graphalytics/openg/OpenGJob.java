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

import nl.tudelft.graphalytics.openg.config.JobConfiguration;
import org.apache.commons.exec.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Base class for all jobs in the OpenG benchmark suite. Configures and executes a OpenG job using the parameters
 * and executable specified by the subclass for a specific algorithm.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public abstract class OpenGJob {

	private static final Logger LOG = LogManager.getLogger();
	private static final Marker OPENG_OUTPUT_MARKER = MarkerManager.getMarker("OPENG-OUTPUT");

	protected final JobConfiguration jobConfiguration;
	private final String binaryPath;
	private final String graphInputPath;
	private final String graphOutputPath;

	/**
	 * Initializes the generic parameters required for running any OpenG job.
	 *
	 * @param jobConfiguration the generic OpenG configuration to use for this job
	 * @param graphInputPath   the path of the input graph
	 * @param graphOutputPath  the path of the output graph
	 */
	public OpenGJob(JobConfiguration jobConfiguration, String binaryPath, String graphInputPath, String graphOutputPath) {
		this.jobConfiguration = jobConfiguration;
		this.binaryPath = binaryPath;
		this.graphInputPath = graphInputPath;
		this.graphOutputPath = graphOutputPath;
	}

	/**
	 * Executes the algorithm defined by this job, with the parameters defined by the user.
	 *
	 * @return the exit code of OpenG
	 * @throws IOException if OpenG failed to run
	 */
	public int execute() throws IOException {
		CommandLine commandLine = createCommandLineForExecutable();
		appendGraphPathParameters(commandLine);;
		appendThreadingParameters(commandLine);
		appendAlgorithmParameters(commandLine);

		LOG.debug("Starting job with command line: {}", commandLine.toString());

		Executor executor = createCommandExecutor();
		executor.setStreamHandler(new PumpStreamHandler(System.out));
		return executor.execute(commandLine);
	}

	protected CommandLine createCommandLineForExecutable() {
		Path executablePath = Paths.get(binaryPath, getExecutableName());
		return new CommandLine(executablePath.toFile());
	}

	private Executor createCommandExecutor() {
		DefaultExecutor executor = new DefaultExecutor();
		executor.setStreamHandler(new PumpStreamHandler(
				(LOG.isDebugEnabled() ? new JobOutLogger() : null),
				(LOG.isInfoEnabled() ? new JobErrLogger() : null)
		));
		executor.setExitValues(null);
		return executor;
	}

	private void appendGraphPathParameters(CommandLine commandLine) {
		commandLine.addArgument("--dataset ", false);
		commandLine.addArgument(Paths.get(graphInputPath).toString(), false);
	}

//	private void appendGraphTypeParameters(CommandLine commandLine) {
//		commandLine.addArgument("-load_eprops=" + (usesEdgeProperties() ? "1" : "0"));
//		commandLine.addArgument("-graph_export_type=" + getGraphExportType());
//	}

	private void appendThreadingParameters(CommandLine commandLine) {

		if (jobConfiguration.getNumberOfWorkerThreads() > 0) {
			commandLine.addArgument("--threadnum", false);
			commandLine.addArgument(String.valueOf(jobConfiguration.getNumberOfWorkerThreads()), false);
		} else {
			commandLine.addArgument("--threadnum", false);
			commandLine.addArgument("1", false);
		}
	}

	/**
	 * Appends the algorithm-specific parameters for the OpenG executable to a CommandLine object.
	 *
	 * @param commandLine the CommandLine to append arguments to
	 */
	protected abstract void appendAlgorithmParameters(CommandLine commandLine);

	/**
	 * @return the name of the algorithm-specific OpenG executable
	 */
	protected abstract String getExecutableName();


	/**
	 * @return true iff the algorithm requires edge properties to be read from the input graph
	 */
	protected abstract boolean usesEdgeProperties();

	/**
	 * Helper class for logging standard output from OpenG to log4j.
	 */
	private static final class JobOutLogger extends LogOutputStream {

		@Override
		protected void processLine(String line, int logLevel) {
			LOG.debug(OPENG_OUTPUT_MARKER, "[OPENG-OUT] {}", line);
		}

	}

	/**
	 * Helper class for logging standard error from OpenG to log4j.
	 */
	private static final class JobErrLogger extends LogOutputStream {

		@Override
		protected void processLine(String line, int logLevel) {
			LOG.info(OPENG_OUTPUT_MARKER, "[OPENG-ERR] {}", line);
		}

	}

}
