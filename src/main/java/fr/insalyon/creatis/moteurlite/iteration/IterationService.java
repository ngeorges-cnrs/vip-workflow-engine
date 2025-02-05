package fr.insalyon.creatis.moteurlite.iteration;

import java.util.*;
import java.util.stream.Collectors;
import java.io.File;
import java.nio.file.Paths;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;

import fr.insalyon.creatis.moteurlite.MoteurLite;
import fr.insalyon.creatis.moteurlite.MoteurLiteException;
import fr.insalyon.creatis.moteurlite.boutiques.BoutiquesService;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.BoutiquesDescriptor;
import org.apache.log4j.Logger;

public class IterationService {
    private static final Logger logger = Logger.getLogger(MoteurLite.class);
    private final BoutiquesService boutiquesService;
    private final IterationTypes iterationTypes;

    public IterationService(BoutiquesService boutiquesService) {
        this.boutiquesService = boutiquesService;
        this.iterationTypes = new IterationTypes();
    }

    public List<Map<String, String>> compute(Map<String, List<String>> inputsMap, BoutiquesDescriptor boutiquesDescriptor) throws MoteurLiteException {
        Set<String> crossKeys = boutiquesService.getCrossMap(boutiquesDescriptor);
        Set<String> dotKeys = boutiquesService.getDotMap(boutiquesDescriptor);
        Set<String> allKeys = new HashSet<>(inputsMap.keySet());

        allKeys.removeAll(crossKeys);
        allKeys.removeAll(dotKeys);

        dotKeys.retainAll(inputsMap.keySet());
        crossKeys.retainAll(inputsMap.keySet());
        crossKeys.addAll(allKeys);

        // prototype directory listing: transform a single directory input into a list of file inputs
        Map<String, List<String>> i2 = new HashMap<String, List<String>>();
        for (String key: inputsMap.keySet()) {
            List<String> val = inputsMap.get(key);
            // ... XXX some "is a directory" detection here (else, regular file)
            if (key.equals("input1") &&
                    val.size() == 1 &&
                    val.getFirst().equals("file:/var/www/html/workflows/SharedData/users/admin_test")) {
                List<String> val2 = new ArrayList<String>();
                // ... XXX some ls + globbing here
                val2.add(val.getFirst() + "/" + "example.txt");
                val2.add(val.getFirst() + "/" + "example3.txt");
                i2.put(key, val2);
            } else {
                i2.put(key, val);
            }
        }
        inputsMap = i2;

        logger.info("XXX inputsMap=" + inputsMap);
        logger.info("XXX dotKeys=" + dotKeys);
        logger.info("XXX crossKeys=" + crossKeys);
        List<Map<String, String>> dotCombinations = iterationTypes.dot(getSelectedMap(inputsMap, dotKeys));
        List<Map<String, String>> crossCombinations = iterationTypes.cross(getSelectedMap(inputsMap, crossKeys));
        List<Map<String, String>> resultCombinations = iterationTypes.cross(dotCombinations, crossCombinations);
        logger.info("XXX dotCombinations=" + dotCombinations);
        logger.info("XXX crossCombinations=" + crossCombinations);
        logger.info("XXX resultCombinations=" + resultCombinations);

        return resultCombinations;
    }

    private Map<String, List<String>> getSelectedMap(Map<String, List<String>> inputMap, Set<String> keys) {
        return inputMap.entrySet().stream()
            .filter(entry -> keys.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
