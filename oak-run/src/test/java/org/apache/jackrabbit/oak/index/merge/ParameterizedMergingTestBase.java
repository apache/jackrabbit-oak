package org.apache.jackrabbit.oak.index.merge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.commons.json.JsonObject;

public class ParameterizedMergingTestBase {

    protected final String testCaseFile;
    protected final String testCaseName;
    protected final JsonObject buildIndexes;
    protected final JsonObject runIndexes;
    protected final JsonObject testCase;

    public static Object[] testCase(String name, String testCaseFile) {
        return new Object[] {
                name,
                testCaseFile
        };
    }

    public ParameterizedMergingTestBase(String name, String testCaseFile)
            throws IOException {
        this.testCaseName = name;
        this.testCaseFile = testCaseFile;
        this.testCase = readTestCaseFile(testCaseFile);
        this.buildIndexes = getTestCaseChild("build");
        this.runIndexes = getTestCaseChild("run");
    }

    private JsonObject readTestCaseFile(String testCaseFileName) {
        return Optional.ofNullable(IndexDefMergerConflictsTest.class.getResourceAsStream(testCaseFileName))
                .map(in -> {
                    try {
                        return IOUtils.toString(in, StandardCharsets.UTF_8.toString());
                    } catch (IOException e) {
                        throw new IllegalArgumentException(
                                "Unexpected IOException reading test case file: " + testCaseFileName, e);
                    }
                })
                .map(s -> JsonObject.fromJson(s, true))
                .orElseThrow(() -> new IllegalArgumentException("Unable to read test case file: " + testCaseFileName));
    }

    protected JsonObject getTestCaseChild(String fieldName) {
        return Optional.ofNullable(testCase.getChildren().get(fieldName))
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unable to run test: " + testCaseName + ", Expected field " + fieldName + " not set"));
    }

}
