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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.configuration.ConfigurationUtil;
import nl.tudelft.graphalytics.configuration.InvalidConfigurationException;
import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.granula.GranulaAwarePlatform;
import nl.tudelft.graphalytics.openg.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.openg.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.openg.algorithms.pr.PageRankJob;
import nl.tudelft.graphalytics.openg.algorithms.wcc.WeaklyConnectedComponentsJob;
import nl.tudelft.graphalytics.openg.algorithms.lcc.LocalClusteringCoefficientJob;
import nl.tudelft.graphalytics.openg.config.JobConfiguration;
import nl.tudelft.graphalytics.openg.config.JobConfigurationParser;
import nl.tudelft.graphalytics.openg.reporting.logging.OpenGLogger;
import nl.tudelft.pds.granula.modeller.model.job.JobModel;
import nl.tudelft.pds.granula.modeller.openg.job.OpenG;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * OpenG platform integration for the Graphalytics benchmark.
 *
 * @author Yong Guo
 * @author Tim Hegeman
 */
public final class OpenGGranulaPlatform extends OpenGPlatform implements GranulaAwarePlatform {

	public OpenGGranulaPlatform() {
		super();
		OPENG_BINARY_DIRECTORY = "./bin/granula";
	}

	@Override
	public void setBenchmarkLogDirectory(Path logDirectory) {
			OpenGLogger.startPlatformLogging(logDirectory.resolve("OperationLog").resolve("driver.logs"));
	}

	@Override
	public void finalizeBenchmarkLogs(Path logDirectory) {
//			OpenGLogger.collectYarnLogs(logDirectory);
			// TODO replace with collecting logs from openg
//			OpenGLogger.collectUtilLog(null, null, 0, 0, logDirectory);
			OpenGLogger.stopPlatformLogging();

	}

	@Override
	public JobModel getGranulaModel() {
		return new OpenG();
	}
}
