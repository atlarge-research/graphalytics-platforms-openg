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
package science.atlarge.granula.modeller;


import science.atlarge.granula.archiver.GranulaArchiver;
import science.atlarge.granula.modeller.entity.BasicType;
import science.atlarge.granula.modeller.job.JobModel;
import science.atlarge.granula.modeller.job.Overview;
import science.atlarge.granula.modeller.platform.Openg;
import science.atlarge.granula.modeller.source.JobDirectorySource;

public class ModelTester {
    public static void main(String[] args) {

        String inputPath = "/home/wlngai/Workstation/Exec/Granula/debug/archiver/openg/log";
        String outputPath = "/home/wlngai/Workstation/Repo/tudelft-atlarge/granula/granula-visualizer/data/";

        // job
        JobDirectorySource jobDirSource = new JobDirectorySource(inputPath);
        jobDirSource.load();

        Overview overview = new Overview();
        overview.setStartTime(1466000574008l);
        overview.setEndTime(1466001551190l);
        overview.setName("OpenG Job");
        overview.setDescription("A OpenG Job");

        GranulaArchiver granulaArchiver = new GranulaArchiver(
                jobDirSource, new JobModel(new Openg()), outputPath, BasicType.ArchiveFormat.JS);
        granulaArchiver.setOverview(overview);
        granulaArchiver.archive();

    }
}