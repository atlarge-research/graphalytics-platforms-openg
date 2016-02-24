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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import nl.tudelft.graphalytics.openg.config.JobConfiguration;

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
	private String outputPath = null;

	/**
	 * Initializes the generic parameters required for running any OpenG job.
	 *
	 * @param jobConfiguration the generic OpenG configuration to use for this job
	 * @param graphInputPath   the path of the input graph
	 * @param graphOutputPath  the path of the output graph
	 */
	public OpenGJob(JobConfiguration jobConfiguration, String binaryPath, String graphInputPath) {
		this.jobConfiguration = jobConfiguration;
		this.binaryPath = binaryPath;
		this.graphInputPath = graphInputPath;
	}

	/**
	 * Set the path to the file were the output of this job should be stored.
	 *
	 * @param path The path to the output file.
	 */
	public void setOutputPath(String path) {
		this.outputPath = path;
	}

	/**
	 * Executes the algorithm defined by this job, with the parameters defined by the user.
	 *
	 * @return the exit code of OpenG
	 * @throws IOException if OpenG failed to run
	 */
	public int execute() throws IOException {
		CommandLine commandLine = createCommandLineForExecutable();
		appendGraphPathParameters(commandLine);
		appendThreadingParameters(commandLine);
		appendAlgorithmParameters(commandLine);
		appendOutputPathParameters(commandLine);

		LOG.debug("Starting job with command line: {}", commandLine.toString());

		Executor executor = createCommandExecutor();
		executor.setStreamHandler(new PumpStreamHandler(System.out));
		executor.setExitValue(0);
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

	private void appendOutputPathParameters(CommandLine commandLine) {
		if (outputPath != null) {
			commandLine.addArgument("--output ", false);
			commandLine.addArgument(Paths.get(outputPath).toString(), false);
		}
	}

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
