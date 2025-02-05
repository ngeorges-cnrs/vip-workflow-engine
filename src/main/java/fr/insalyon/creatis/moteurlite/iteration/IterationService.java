package fr.insalyon.creatis.moteurlite.iteration;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import fr.insalyon.creatis.moteurlite.MoteurLiteException;
import fr.insalyon.creatis.moteurlite.boutiques.BoutiquesService;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.BoutiquesDescriptor;

public class IterationService {
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

        List<Map<String, String>> dotCombinations = iterationTypes.dot(getSelectedMap(inputsMap, dotKeys));
        List<Map<String, String>> crossCombinations = iterationTypes.cross(getSelectedMap(inputsMap, crossKeys));
        List<Map<String, String>> resultCombinations = iterationTypes.cross(dotCombinations, crossCombinations);

        return resultCombinations;
    }

    private Map<String, List<String>> getSelectedMap(Map<String, List<String>> inputMap, Set<String> keys) {
        return inputMap.entrySet().stream()
            .filter(entry -> keys.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
