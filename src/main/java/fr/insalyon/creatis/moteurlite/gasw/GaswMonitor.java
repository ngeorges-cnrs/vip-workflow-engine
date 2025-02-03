package fr.insalyon.creatis.moteurlite.gasw;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.Gasw;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswExitCode;
import fr.insalyon.creatis.gasw.GaswOutput;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.moteurlite.MoteurLiteException;

public class GaswMonitor extends Thread {
    private static final Logger logger = Logger.getLogger(GaswMonitor.class);

    private String workflowId;
    private String applicationName;
    private int numberOfInvocations;
    private Gasw gasw;
    private WorkflowsDBRepository workflowsDbRepository;

    private Integer finishedJobsNumber = 0;
    private Integer successfulJobsNumber = 0;
    private Integer failedJobsNumber = 0;

    public GaswMonitor(Gasw gasw, WorkflowsDBRepository workflowsDbRepository, String workflowId, String applicationName, int numberOfInvocations) {
        this.gasw = gasw;
        this.workflowsDbRepository = workflowsDbRepository;
        this.workflowId = workflowId;
        this.applicationName = applicationName;
        this.numberOfInvocations = numberOfInvocations;
    }

    @Override
    public void run() {
        while (finishedJobsNumber < numberOfInvocations) {
            waitForGasw();

            List<GaswOutput> finishedJobs = gasw.getFinishedJobs();
            logger.info("Number of finished jobs: " + finishedJobs.size());

            if (finishedJobs.isEmpty()) {
                continue;
            } else {
                try {
                    processFinishedJobs(finishedJobs);
                    workflowsDbRepository.persistProcessors(workflowId, applicationName, numberOfInvocations - finishedJobsNumber, successfulJobsNumber, failedJobsNumber);
                } catch (MoteurLiteException e) {
                    logger.error("Error while persisting processors during processing: ", e);
                }
            }
        }
        terminate();
    }

    private synchronized void waitForGasw() {
        try {
            gasw.waitForNotification();
            wait();
        } catch (InterruptedException e) {
            logger.error("Interrupted exception while waiting for notification: ", e);
        }
    }

    private void processFinishedJobs(List<GaswOutput> finishedJobs) {
        GaswExitCode exitCode;
        Map<String, URI> uploadedResults;

        for (GaswOutput gaswOutput : finishedJobs) {
            logger.info("Status: " + gaswOutput.getJobID() + " " + gaswOutput.getExitCode());
            try {
                exitCode = gaswOutput.getExitCode();
                if (exitCode == GaswExitCode.SUCCESS) {
                    successfulJobsNumber++;
                } else {
                    failedJobsNumber++;
                }

                uploadedResults = gaswOutput.getUploadedResultsAsMap();
                if (uploadedResults != null && !uploadedResults.isEmpty()) {
                    workflowsDbRepository.persistOutputs(workflowId, uploadedResults);
                }
            } catch (MoteurLiteException e) {
                logger.error("Error while processing finished job output: ", e);
            }
        }
        finishedJobsNumber += finishedJobs.size();
    }

    private void terminate() {
        try {
            GaswStatus finalStatus = successfulJobsNumber > 0 ? GaswStatus.COMPLETED : GaswStatus.ERROR;

            workflowsDbRepository.persistWorkflow(workflowId, finalStatus);
            gasw.terminate();
            logger.info("XXX 2 Completed execution of workflow");
        } catch (GaswException e) {
            logger.error("Error while terminating Gasw: ", e);
        } catch (MoteurLiteException e) {
            logger.error("Error while persisting final workflow status: ", e);
        }
    }
}