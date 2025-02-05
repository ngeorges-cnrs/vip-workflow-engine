package fr.insalyon.creatis.moteurlite.runner;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
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
import fr.insalyon.creatis.moteurlite.boutiques.scheme.BoutiquesDescriptor;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.Input;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.OutputFile;
import fr.insalyon.creatis.moteurlite.gasw.GaswMonitor;
import fr.insalyon.creatis.moteurlite.gasw.WorkflowsDBRepository;
import fr.insalyon.creatis.moteurlite.iteration.IterationService;

import java.io.File;
import java.nio.file.Paths;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;

import fr.insalyon.creatis.grida.common.bean.GridData;
import fr.insalyon.creatis.grida.client.GRIDAClient;
import fr.insalyon.creatis.grida.client.GRIDAClientException;
import fr.insalyon.creatis.grida.client.StandaloneGridaClient;

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

    private Map<String, List<String>> listDir(Map<String, List<String>> inputsMap, BoutiquesDescriptor boutiquesDescriptor) {
        // . get activation condition from descriptor: list of (input key name + list of patterns)
        // . for each matching input, list files through Grida:
        //   . keep only files that match the pattern, ideally check type instead of just name
        //   . check how to use standalone mode instead of client
        // . expand inputsMap with whatever was found
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        logger.info("XXX inputsMap.0=" + inputsMap);
        logger.info("XXX calling GridaClient...");
        GRIDAClient client = new GRIDAClient("localhost", 9006, "/var/www/html/workflows/x509up_server");
        // GRIDAClient client = new StandaloneGridaClient("/var/www/html/workflows/x509up_server", new File("/var/www/prod/grida/grida-server.conf"));
        // GRIDAClient client = new StandaloneGridaClient("/var/www/html/workflows/x509up_server", new File("/vip/grida/grida-server.conf"));
        logger.info("XXX GridaClient created");
        for (String key: inputsMap.keySet()) {
            List<String> val = inputsMap.get(key);
            if (key.equals("input1") &&
                    val.size() == 1 &&
                    val.getFirst().equals("file:/var/www/html/workflows/SharedData/users/admin_test")) {
                try {
                    String dir = "/var/www/html/workflows/SharedData/users/admin_test";
                    List<String> resultFiles = new ArrayList<String>();
                    logger.info("XXX start grida listing, dir=" + dir);
                    List<GridData> files = client.getFolderData(dir, true);
                    for (GridData file : files) {
                        String filename = file.getName();
                        logger.info("XXX files: name=" + filename + ", type=" + file.getType());
                        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:*.txt");
                        if (matcher.matches(Paths.get(filename))) {
                            resultFiles.add("file:/var/www/html/workflows/SharedData/users/admin_test" + "/" + filename);
                        }
                    }
                    result.put(key, resultFiles);
                    logger.info("XXX end grida listing, files=" + resultFiles);
                } catch (GRIDAClientException e) {
                }
            } else { // leave key as is
                result.put(key, val);
            }
        }
        return result;
    }

    public void run(String workflowId, String boutiquesFilePath, String inputsFilePath) throws MoteurLiteException {
        Gasw gasw;
        Map<String, List<String>> allInputs = inputsFileService.parseInputData(inputsFilePath);
        BoutiquesDescriptor descriptor = boutiquesService.parseFile(boutiquesFilePath);
        Map<String, Input> boutiquesInputs = boutiquesService.getInputsMap(descriptor);

        allInputs = listDir(allInputs, descriptor);
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

            logger.info("XXX createJobs: invocationInputs=" + invocationInputs);
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
            logger.info("XXX createJobs: gaswInput resultsDirectoryURI=" + resultsDirectoryURI);

            String invocationString = convertMapToJson(finalInvocationInputs, boutiquesInputs);
            String jobId = applicationName + "-" + System.nanoTime() + ".sh";

            GaswInput gaswInput = new GaswInput(applicationName, applicationName + ".json", downloads, resultsDirectoryURI, invocationString, jobId);
            logger.info("XXX createJobs: gaswInput uploadURI=" + gaswInput.getUploadURI());
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
