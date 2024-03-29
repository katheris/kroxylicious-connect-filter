package io.github.katheris.connect.filter;


import io.github.katheris.connect.util.Formatter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FindCoordinatorRequestFilter;
import io.kroxylicious.proxy.filter.FindCoordinatorResponseFilter;
import io.kroxylicious.proxy.filter.HeartbeatResponseFilter;
import io.kroxylicious.proxy.filter.JoinGroupRequestFilter;
import io.kroxylicious.proxy.filter.JoinGroupResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.SyncGroupRequestFilter;
import io.kroxylicious.proxy.filter.SyncGroupResponseFilter;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static io.github.katheris.connect.util.Formatter.deserializeAssignment;
import static io.github.katheris.connect.util.Formatter.format;
import static io.github.katheris.connect.util.Formatter.formatAssignment;
import static io.github.katheris.connect.util.Formatter.formatCollection;
import static io.github.katheris.connect.util.Formatter.formatMetadata;
import static io.github.katheris.connect.util.Formatter.formatProtocolName;
import static io.github.katheris.connect.util.Formatter.highlight;
import static io.github.katheris.connect.util.Formatter.printApiCall;

public class RebalanceFilter implements HeartbeatResponseFilter, FindCoordinatorRequestFilter, FindCoordinatorResponseFilter,
        JoinGroupRequestFilter, JoinGroupResponseFilter,
        SyncGroupRequestFilter, SyncGroupResponseFilter {

    //////////////////////////////////////////////////
    // Heartbeat
    //////////////////////////////////////////////////
    @Override
    public CompletionStage<ResponseFilterResult> onHeartbeatResponse(short apiVersion, ResponseHeaderData header, HeartbeatResponseData response, FilterContext context) {
        if (response.errorCode() != 0) {
            printApiCall(Formatter.Colour.RED.print( "<= Heartbeat"), List.of(highlight("error_code", Errors.forCode(response.errorCode()).name())));
        }
        return context.forwardResponse(header, response);
    }

    //////////////////////////////////////////////////
    // FindCoordinator
    //////////////////////////////////////////////////
    @Override
    public CompletionStage<RequestFilterResult> onFindCoordinatorRequest(short apiVersion, RequestHeaderData header, FindCoordinatorRequestData request,
                                                                         FilterContext context) {
        if (request.coordinatorKeys().contains("connect-cluster")) {
            printApiCall(Formatter.Colour.PURPLE.print( "=> FindCoordinator"), List.of(
                    format("key_type", String.valueOf(request.keyType())),
                    format("coordinator_keys", String.join(", ", request.coordinatorKeys()))));
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onFindCoordinatorResponse(short apiVersion, ResponseHeaderData header, FindCoordinatorResponseData response,
                                                                           FilterContext context) {
        Optional<FindCoordinatorResponseData.Coordinator> connectCoordinator = response.coordinators()
                .stream()
                .filter(coordinator -> "connect-cluster".equals(coordinator.key()))
                .findFirst();
        connectCoordinator.ifPresent(coordinator -> printApiCall(Formatter.Colour.PURPLE.print("<= FindCoordinator"), List.of(
                format("key", coordinator.key()),
                format("node_id", String.valueOf(coordinator.nodeId())),
                highlight("host", coordinator.host()),
                highlight("port", String.valueOf(coordinator.port()))
        )));
        return context.forwardResponse(header, response);
    }

    //////////////////////////////////////////////////
    // JoinGroup
    //////////////////////////////////////////////////
    @Override
    public CompletionStage<RequestFilterResult> onJoinGroupRequest(short apiVersion, RequestHeaderData header, JoinGroupRequestData request, FilterContext context) {
        if ("connect".equals(request.protocolType())) {
            printApiCall(Formatter.Colour.YELLOW.print("=> JoinGroup"), List.of(
                    format("group_id", request.groupId()),
                    highlight("member_id", request.memberId()),
                    formatCollection("protocols", request.protocols()
                            .stream()
                            .map(protocol -> String.format("%n           %s%n               %s",
                                    formatProtocolName(protocol.name()),
                                    formatMetadata(protocol.metadata(), protocol.name())))
                            .collect(Collectors.joining(",")))));
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onJoinGroupResponse(short apiVersion, ResponseHeaderData header, JoinGroupResponseData response, FilterContext context) {
        if (response.memberId() != null && response.memberId().startsWith("worker")) {
            printApiCall(Formatter.Colour.YELLOW.print("<= JoinGroup"), List.of(
                    highlight("generation_id", String.valueOf(response.generationId())),
                    format("protocol_name", formatProtocolName(String.valueOf(response.protocolName()))),
                    highlight("leader", String.valueOf(response.leader())),
                    highlight("member_id", String.valueOf(response.memberId())),
                    formatCollection("members", response.members()
                            .stream()
                            .map(member -> String.format("%n           %s%n               %s",
                                    member.memberId(),
                                    formatMetadata(member.metadata(), response.protocolName())))
                            .collect(Collectors.joining("")))));
        }
        return context.forwardResponse(header, response);
    }

    //////////////////////////////////////////////////
    // SyncGroup
    //////////////////////////////////////////////////

    @Override
    public CompletionStage<RequestFilterResult> onSyncGroupRequest(short apiVersion, RequestHeaderData header, SyncGroupRequestData request, FilterContext context) {
        if ("connect".equals(request.protocolType())) {
            printApiCall(Formatter.Colour.CYAN.print("=> SyncGroup"), List.of(
                    format("group_id", request.groupId()),
                    highlight("generation_id", String.valueOf(request.generationId())),
                    format("member_id", request.memberId()),
                    format("protocol_name", formatProtocolName(request.protocolName())),
                    formatCollection("assignments", request.assignments()
                            .stream()
                            .map(member -> String.format("%n           %s%n                %s",
                                    member.memberId(),
                                    formatAssignment(deserializeAssignment(member.assignment(), request.protocolName()))))
                            .collect(Collectors.joining("")))));
        }
        return context.forwardRequest(header, request);
    }

    @Override
    public CompletionStage<ResponseFilterResult> onSyncGroupResponse(short apiVersion, ResponseHeaderData header, SyncGroupResponseData response, FilterContext context) {
        if ("connect".equals(response.protocolType())) {
            printApiCall(Formatter.Colour.CYAN.print("<= SyncGroup"), List.of(
                    format("error_code", String.valueOf(response.errorCode())),
                    format("protocol_name", formatProtocolName(response.protocolName())),
                    Formatter.Colour.GREEN.print(formatAssignment(deserializeAssignment(response.assignment(), response.protocolName())))));
        }
        return context.forwardResponse(header, response);
    }

}
