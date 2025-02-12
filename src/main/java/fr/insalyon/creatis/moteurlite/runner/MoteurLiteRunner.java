package fr.insalyon.creatis.moteurlite.runner;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fr.insalyon.creatis.gasw.Gasw;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.WorkflowsDBException;
import fr.insalyon.creatis.moteur.plugins.workflowsdb.dao.WorkflowsDBDAOException;
import fr.insalyon.creatis.moteurlite.MoteurLite;
import fr.insalyon.creatis.moteurlite.MoteurLiteConstants;
import fr.insalyon.creatis.moteurlite.MoteurLiteException;
import fr.insalyon.creatis.moteurlite.boutiques.BoutiquesService;
import fr.insalyon.creatis.moteurlite.boutiques.model.BoutiquesDescriptor;
import fr.insalyon.creatis.moteurlite.boutiques.model.Input;
import fr.insalyon.creatis.moteurlite.boutiques.model.OutputFile;
import fr.insalyon.creatis.moteurlite.gasw.GaswMonitor;
import fr.insalyon.creatis.moteurlite.gasw.WorkflowsDBRepository;
import fr.insalyon.creatis.moteurlite.iteration.IterationService;

public class MoteurLiteRunner {
    private static final Logger logger = Logger.getLogger(MoteurLite.class);

    private final WorkflowsDBRepository workflowsDBRepo;
    private final BoutiquesService boutiquesService;
    private final InputsFileService inputsFileService;
    private final IterationService iterationService;

    public MoteurLiteRunner() throws MoteurLiteException {
        boutiquesService = new BoutiquesService();
        inputsFileService = new InputsFileService();
        iterationService = new IterationService(boutiquesService);

        try {
            workflowsDBRepo = WorkflowsDBRepository.getInstance();
        } catch (WorkflowsDBDAOException | WorkflowsDBException e) {
            logger.error("Error creating workflows db repo", e);
            throw new MoteurLiteException("Error creating workflows db repo", e);
        }
    }

    public void run(String workflowId, String boutiquesFilePath, String inputsFilePath) throws MoteurLiteException {
        Gasw gasw;
        Map<String, List<String>> allInputs = inputsFileService.parseInputData(inputsFilePath);
        BoutiquesDescriptor descriptor = boutiquesService.parseFile(boutiquesFilePath);
        Map<String, Input> boutiquesInputs = boutiquesService.getInputsMap(descriptor);

        List<Map<String, String>> invocationsInputs = iterationService.compute(allInputs, descriptor);

        workflowsDBRepo.persistProcessors(workflowId, descriptor.getName(), 0, 0, 0);
        workflowsDBRepo.persistInputs(workflowId, allInputs, boutiquesInputs);

        try {
            gasw = Gasw.getInstance();
            GaswMonitor gaswMonitor = new GaswMonitor(gasw, workflowsDBRepo, workflowId, descriptor.getName(), invocationsInputs.size());
            gasw.setNotificationClient(gaswMonitor);
            gaswMonitor.start();
        } catch (GaswException e) {
            logger.error("Error launching gasw", e);
            throw new MoteurLiteException("Error launching gasw", e);
        }

        createJobs(gasw, descriptor.getName(), invocationsInputs, boutiquesInputs);
    }

    private void createJobs(Gasw gasw, String applicationName, List<Map<String, String>> allInvocationsInputs, Map<String, Input> boutiquesInputs) throws MoteurLiteException {
        for (Map<String, String> invocationInputs : allInvocationsInputs) {
            URI resultsDirectoryURI = null;
            List<URI> downloads = new ArrayList<>();
            Map<String, String> finalInvocationInputs = new HashMap<>();

            for (String inputId : invocationInputs.keySet()) {
                String inputValue = invocationInputs.get(inputId);
                if (MoteurLiteConstants.RESULTS_DIRECTORY.equals(inputId)) {
                    resultsDirectoryURI = getURI(inputValue);
                } else {
                    if (Input.Type.FILE.equals(boutiquesInputs.get(inputId).getType())) {
                        URI downloadURI = getURI(inputValue);
                        String filename = Paths.get(downloadURI.getPath()).getFileName().toString();
                        downloads.add(downloadURI);
                        inputValue = filename;
                    }
                    finalInvocationInputs.put(inputId, inputValue);
                }
            }

            String invocationString = convertMapToJson(finalInvocationInputs, boutiquesInputs);
            String jobId = applicationName + "-" + System.nanoTime() + ".sh";

            logger.info("XXX gaswInput applicationName="+applicationName+", downloads="+downloads+", r="+resultsDirectoryURI+", inv="+invocationString+", jobId="+jobId);
            GaswInput gaswInput = new GaswInput(applicationName, applicationName + ".json", downloads, resultsDirectoryURI, invocationString, jobId);
            try {
                gasw.submit(gaswInput);
            } catch (GaswException e) {
                logger.error("Error submitting gasw job", e);
                throw new MoteurLiteException("Error submitting gasw job", e);
            }
        }
    }

    private URI getURI(String inputValue) throws MoteurLiteException {
        try {
            return new URI(inputValue);
        } catch (URISyntaxException e) {
            logger.error("Error parsing URI : " + inputValue, e);
            throw new MoteurLiteException("Error parsing URI : " + inputValue, e);
        }
    }

    private String convertMapToJson(Map<String, String> invocationInputs, Map<String, Input> boutiquesInputs) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();

        for (String inputId : invocationInputs.keySet()) {
            String value = invocationInputs.get(inputId);
            Input.Type type = boutiquesInputs.get(inputId).getType();

            if (type == Input.Type.NUMBER) {
                if (boutiquesInputs.get(inputId).getInteger() != null && boutiquesInputs.get(inputId).getInteger()) {
                    jsonNode.put(inputId, Integer.parseInt(value));
                } else {
                    jsonNode.put(inputId, Float.parseFloat(value));
                }
            } else if (type == Input.Type.FLAG) {
                jsonNode.put(inputId, Boolean.parseBoolean(value));
            } else {
                jsonNode.put(inputId, value);
            }
        }
        return jsonNode.toString();
    }
}
