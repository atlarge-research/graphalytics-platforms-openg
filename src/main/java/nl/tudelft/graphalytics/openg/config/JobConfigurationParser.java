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
package nl.tudelft.graphalytics.openg.config;

import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import org.apache.commons.configuration.Configuration;

import java.nio.file.Paths;

/**
 * Parser for the Graphalytics OpenG properties file. This class extracts all properties related to OpenG jobs
 * and stores them in a JobConfiguration object.
 *
 * @author Wing Lung Ngai
 * @author Tim Hegeman
 */
public final class JobConfigurationParser {

	public static final String OPENG_HOME_KEY = "openg.home";
	public static final String OPENG_NUM_WORKER_THREADS_KEY = "openg.num-worker-threads";

	private final Configuration opengConfig;

	private JobConfigurationParser(Configuration opengConfig) {
		this.opengConfig = opengConfig;
	}

	private JobConfiguration parse() throws InvalidConfigurationException {
		// Load mandatory configuration properties
		String opengHome = ConfigurationUtil.getString(opengConfig, OPENG_HOME_KEY);
		String opengBinariesPath = Paths.get(opengHome, "bin").toString();
		JobConfiguration jobConfiguration = new JobConfiguration(opengBinariesPath);

		// Load optional configuration properties
		parseNumWorkerThreads(jobConfiguration);

		return jobConfiguration;
	}


	private void parseNumWorkerThreads(JobConfiguration jobConfiguration) {
		Integer numWorkers = opengConfig.getInteger(OPENG_NUM_WORKER_THREADS_KEY, null);
		if (numWorkers != null) {
			jobConfiguration.setNumberOfWorkerThreads(numWorkers);
		}
	}

	public static JobConfiguration parseOpenGPropertiesFile(Configuration opengConfig)
			throws InvalidConfigurationException {
		return new JobConfigurationParser(opengConfig).parse();
	}

}
