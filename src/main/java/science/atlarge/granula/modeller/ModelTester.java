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