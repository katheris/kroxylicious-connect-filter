package io.github.katheris.connect.util;

import org.apache.kafka.connect.runtime.distributed.ConnectProtocol;
import org.apache.kafka.connect.runtime.distributed.ExtendedAssignment;
import org.apache.kafka.connect.runtime.distributed.ExtendedWorkerState;
import org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeConnectProtocol;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class Formatter {

    public enum Colour {
        RESET("\033[0m"),
        RED("\033[0;91m"),
        GREEN("\033[0;32m"),
        YELLOW("\033[0;33m"),
        CYAN("\033[0;36m"),
        PURPLE("\033[0;95m"),
        WHITE("\033[0;37m");

        private final String ansiCode;
        Colour(String ansiCode) {
            this.ansiCode = ansiCode;
        }

        public String print(String text) {
            return String.format("%s%s%s", ansiCode, text, RESET);
        }

        @Override
        public String toString() {
            return ansiCode;
        }

    }

    private Formatter() {
    }

    public static String format(String field, String value) {
        return String.format("%s => %s", field, value);
    }

    public static String highlight(String field, String value) {
        return Colour.GREEN.print(String.format("%s => %s", field, value));
    }

    public static String formatCollection(String field, String content) {
        return Colour.GREEN.print(String.join("", field, content));
    }

    public static String formatMetadata(byte[] value, String protocolName) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value).asReadOnlyBuffer();
        ExtendedWorkerState workerState = IncrementalCooperativeConnectProtocol.deserializeMetadata(byteBuffer);
        String url = format("url", workerState.url());
        if ("sessioned".equals(protocolName) || "compatible".equals(protocolName)) {
            return String.format("%s%n               %s", url, formatAssignment(workerState.assignment()));
        } else {
            return format("url", workerState.url());
        }
    }

    public static void printApiCall(String name, List<String> fields) {
        String content = fields
                .stream()
                .map(field -> String.format("       %s%n", field))
                .collect(Collectors.joining());
        System.out.printf("%s%n%s%n", name, content);
    }

    public static String formatProtocolName(String name) {
        return "default".equals(name) ? "eager" : name;
    }

    public static String formatAssignment(ConnectProtocol.Assignment assignment) {
        String connectorIds = String.join(",", assignment.connectors());
        String taskIds = String.join(",", String.valueOf(assignment.tasks().stream().map(connectorTaskId -> connectorTaskId.connector() + "-T" + connectorTaskId.task()).collect(Collectors.joining(","))));
        String assignmentAsString;
        if (assignment instanceof ExtendedAssignment extendedAssignment) {
            String revokedConnectorIds = String.join(",", extendedAssignment.revokedConnectors());
            String revokedTaskIds = String.join(",", String.valueOf(extendedAssignment.revokedTasks().stream().map(connectorTaskId -> connectorTaskId.connector() + "-T" + connectorTaskId.task()).collect(Collectors.joining(","))));
            assignmentAsString = String.format("connectors=[%s],tasks=[%s]%n                       revokedConnectors=[%s],revokedTasks=[%s]", connectorIds, taskIds, revokedConnectorIds, revokedTaskIds);
        } else {
            assignmentAsString = String.format("connectors=[%s],tasks=[%s]", connectorIds, taskIds);
        }
        return format("assignment", assignmentAsString);
    }

    public static ConnectProtocol.Assignment deserializeAssignment(byte[] assignment, String protocolName) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(assignment).asReadOnlyBuffer();
        if ("sessioned".equals(protocolName) || "compatible".equals(protocolName)) {
            return IncrementalCooperativeConnectProtocol.deserializeAssignment(byteBuffer);
        } else {
            return ConnectProtocol.deserializeAssignment(byteBuffer);
        }
    }
}
