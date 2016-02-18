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
package nl.tudelft.graphalytics.openg.algorithms.cdlp;

import org.apache.commons.exec.CommandLine;

import nl.tudelft.graphalytics.openg.OpenGJob;
import nl.tudelft.graphalytics.openg.config.JobConfiguration;

/**
 * Breadth-first search job implementation for OpenG. This class is responsible for formatting BFS-specific
 * arguments to be passed to the OpenG executable, and does not include the implementation of the algorithm.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public final class CommunityDetectionLPJob extends OpenGJob {

	private final long iteration;

	/**
	 * Creates a new BreadthFirstSearchJob object with all mandatory parameters specified.
	 *
	 * @param jobConfiguration the generic OpenG configuration to use for this job
	 * @param graphInputPath   the path to the input graph
	 */
	public CommunityDetectionLPJob(long iteration, JobConfiguration jobConfiguration, String binaryPath,
								   String graphInputPath) {
		super(jobConfiguration, binaryPath, graphInputPath);
		this.iteration = iteration;
	}

	@Override
	protected void appendAlgorithmParameters(CommandLine commandLine) {
		commandLine.addArgument("--iteration", false);
		commandLine.addArgument(String.valueOf(iteration), false);
	}

	@Override
	protected String getExecutableName() {
		return "cdlp";
	}
}
