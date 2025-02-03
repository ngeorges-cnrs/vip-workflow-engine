package fr.insalyon.creatis.moteurlite.boutiques;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import fr.insalyon.creatis.moteurlite.MoteurLiteException;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.BoutiquesDescriptor;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.Custom;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.Input;
import fr.insalyon.creatis.moteurlite.boutiques.scheme.OutputFile;


public class BoutiquesService {

    public BoutiquesService() {}

    public BoutiquesDescriptor parseFile(String boutiquesDescriptorFile) throws MoteurLiteException {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(new File(boutiquesDescriptorFile), BoutiquesDescriptor.class);
        } catch (IOException e) {
            throw new MoteurLiteException("Error parsing boutiques file " + boutiquesDescriptorFile, e);
        }
    }

    public Map<String, Input.Type> getInputTypes(BoutiquesDescriptor boutiquesDescriptor) {
        return boutiquesDescriptor.getInputs().stream()
            .collect(Collectors.toMap(Input::getId, Input::getType));
    }

    public Map<String, Input> getInputsMap(BoutiquesDescriptor boutiquesDescriptor) {
        return boutiquesDescriptor.getInputs().stream()
            .collect(Collectors.toMap(Input::getId, Function.identity()));
    }

    public Map<String, OutputFile> getOutputMap(BoutiquesDescriptor boutiquesDescriptor) {
        return boutiquesDescriptor.getOutputFiles().stream()
            .collect(Collectors.toMap(OutputFile::getId, Function.identity()));
    }

    public Set<String> getInputOptionalOfBoutiquesFile(BoutiquesDescriptor boutiquesDescriptor) {
        return boutiquesDescriptor.getInputs().stream()
            .filter((i) -> i.getOptional() != null && i.getOptional())
            .map((i) -> i.getId())
            .collect(Collectors.toSet());
    }

    public Set<String> getCrossMap(BoutiquesDescriptor boutiquesDescriptor) {
        return extractCustomField(boutiquesDescriptor, "vip:cross");
    }

    public Set<String> getDotMap(BoutiquesDescriptor boutiquesDescriptor) {
        return extractCustomField(boutiquesDescriptor, "vip:dot");
    }

    public Set<String> getContainerSet(BoutiquesDescriptor boutiquesDescriptor) {
        return extractCustomField(boutiquesDescriptor, "vip:imagepath");
    }

    private Set<String> extractCustomField(BoutiquesDescriptor boutiquesDescriptor, String key) {
        Set<String> resultSet = new HashSet<>();
        Custom c = boutiquesDescriptor.getCustom();
        Map<String, Object> customMap = null;
        if (c != null)
            customMap = c.getAdditionalProperties();

        if (customMap != null && customMap.containsKey(key)) {
            Object value = customMap.get(key);

            if (value instanceof String) {
                resultSet.add((String) value);
            } else if (value instanceof Iterable) {
                for (Object item : (Iterable<?>) value) {
                    resultSet.add(item.toString());
                }
            }
        }

        return resultSet;
    }
}