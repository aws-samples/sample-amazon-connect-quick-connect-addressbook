package com.amazonaws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.connect.ConnectClient;
import software.amazon.awssdk.services.connect.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuickConnectFunction implements RequestHandler<Object, String> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // AWS Services
    private final ConnectClient connectClient = ConnectClient.builder().build();
    private final S3Client s3Client = S3Client.builder().build();

    private final Map<String, QuickConnectRecord> quickConnectUserMap = new HashMap<>();
    private final Map<String, QuickConnectRecord> quickConnectPhoneMap = new HashMap<>();
    private final Map<String, QuickConnectRecord> quickConnectQueueMap = new HashMap<>();
    private LambdaLogger logger;
    private List<QueueSummary> allQueues = new ArrayList<>();

    /**
     * Simple retry method for updateQuickConnectName to handle the default rate limit (2 requests/sec)
     */
    private void updateQuickConnectNameWithRetry(UpdateQuickConnectNameRequest request) {
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                connectClient.updateQuickConnectName(request);
                return; // Success, exit the method
            } catch (TooManyRequestsException e) {
                retryCount++;
                logger.log("Rate limit hit, retry attempt " + retryCount + "/" + maxRetries, LogLevel.WARN);
                
                if (retryCount >= maxRetries) {
                    logger.log("Max retries reached for updateQuickConnectName", LogLevel.ERROR);
                    throw e; // Re-throw after max retries
                }
                
                try {
                    // Wait 1000ms before retry (2 requests/sec = 1000ms between requests)
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry wait", ie);
                }
            }
        }
    }

    /**
     * Simple retry method for deleteQuickConnect to handle rate limiting (2 requests/sec)
     */
    private void deleteQuickConnectWithRetry(DeleteQuickConnectRequest request) {
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                connectClient.deleteQuickConnect(request);
                return; // Success, exit the method
            } catch (TooManyRequestsException e) {
                retryCount++;
                logger.log("Rate limit hit, retry attempt " + retryCount + "/" + maxRetries, LogLevel.WARN);
                
                if (retryCount >= maxRetries) {
                    logger.log("Max retries reached for deleteQuickConnect", LogLevel.ERROR);
                    throw e; // Re-throw after max retries
                }
                
                try {
                    // Wait 1000ms before retry (2 requests/sec = 1000ms between requests)
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry wait", ie);
                }
            }
        }
    }

    /**
     * Simple retry method for associateQueueQuickConnects to handle rate limiting (2 requests/sec)
     */
    private void associateQueueQuickConnectsWithRetry(AssociateQueueQuickConnectsRequest request) {
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                connectClient.associateQueueQuickConnects(request);
                return; // Success, exit the method
            } catch (TooManyRequestsException e) {
                retryCount++;
                logger.log("Rate limit hit, retry attempt " + retryCount + "/" + maxRetries, LogLevel.WARN);
                
                if (retryCount >= maxRetries) {
                    logger.log("Max retries reached for associateQueueQuickConnects", LogLevel.ERROR);
                    throw e; // Re-throw after max retries
                }
                
                try {
                    // Wait 1000ms before retry (2 requests/sec = 1000ms between requests)
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry wait", ie);
                }
            }
        }
    }

    /**
     * Simple retry method for createQuickConnect to handle rate limiting (2 requests/sec)
     */
    private CreateQuickConnectResponse createQuickConnectWithRetry(CreateQuickConnectRequest request) {
        int maxRetries = 3;
        int retryCount = 0;
        
        while (retryCount < maxRetries) {
            try {
                return connectClient.createQuickConnect(request);
            } catch (TooManyRequestsException e) {
                retryCount++;
                logger.log("Rate limit hit, retry attempt " + retryCount + "/" + maxRetries, LogLevel.WARN);
                
                if (retryCount >= maxRetries) {
                    logger.log("Max retries reached for createQuickConnect", LogLevel.ERROR);
                    throw e; // Re-throw after max retries
                }
                
                try {
                    // Wait 1000ms before retry (2 requests/sec = 1000ms between requests)
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during retry wait", ie);
                }
            }
        }
        // This should never be reached due to the throw in the catch block
        throw new RuntimeException("Unexpected end of retry loop");
    }

    @Override
    public String handleRequest(Object event, Context context) {
        this.logger = context.getLogger();
        StringBuilder result = new StringBuilder();
        JsonNode eventJSONMap;
        String bucket = "", key = "";

        // Get the S3 bucket and key from the event
        try {
            String eventString = objectMapper.writeValueAsString(event);
            eventJSONMap = objectMapper.readTree(eventString);
            // If the Event is from S3, else it's assumed it's from EventBridge
            if (eventJSONMap.has("Records")) {
                bucket = eventJSONMap.path("Records").get(0).path("s3").path("bucket").path("name").asText();
                key = eventJSONMap.path("Records").get(0).path("s3").path("object").path("key").asText();
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        // Amazon Connect Instance Id
        String instanceId = System.getenv().getOrDefault("Instance_Id", "");

        // Agent Transfer Flow Id, if not provided 'Default agent transfer' flow Id will be considered
        String agentTransferFlowId = System.getenv().getOrDefault("Default_Agent_Transfer_Flow_Id", "");

        // Quick Connect Type to process - All or User or Phone or Queue
        String processType = System.getenv().getOrDefault("Process_Quick_Connect_Type", "All");

        if (agentTransferFlowId.trim().isBlank()) {
            agentTransferFlowId = getDefaultAgentTransferFlowId(instanceId);
        }

        // Get all Queues
        allQueues = getQueueSummaryList(instanceId);

        // Get all Quick Connect Types
        getQuickConnects(instanceId);

        // Process User Quick Connect Type, using the Users data from Amazon Connect User Management
        if (processType.equalsIgnoreCase("User") || processType.equalsIgnoreCase("All")) {
            buildUserQuickConnects(instanceId, agentTransferFlowId);
        }

        // Process both Queue and PhoneNumber Quick Connect Type, using the CSV file from Amazon S3
        if (!bucket.isEmpty() && !key.isEmpty()) {
            try {
                // Read CSV file from S3
                List<QuickConnectCSVRecord> records = readCsvFromS3(bucket, key);

                if (processType.equalsIgnoreCase("Phone") || processType.equalsIgnoreCase("All")) {
                    records.stream()
                            .filter(record -> record.quickConnectType().equalsIgnoreCase("Phone"))
                            .forEach(record -> {
                                // Create Quick Connect of Type Phone and Associate with Queues
                                buildPhoneQuickConnects(instanceId, record);
                            });

                }

                if (processType.equalsIgnoreCase("Queue") || processType.equalsIgnoreCase("All")) {
                    records.stream()
                            .filter(record -> record.quickConnectType().equalsIgnoreCase("Queue"))
                            .forEach(record -> {
                                // Create Quick Connect of Type Queue and Associate with Queues
                                buildQueueQuickConnects(instanceId, record);
                            });
                }
            } catch (Exception e) {
                context.getLogger().log("Error processing file: " + e.getMessage());
                throw new RuntimeException("Error processing CSV file", e);
            }
        }
        result.append("Quick Connects created/updated successfully");
        return result.toString();
    }

    private void buildQueueQuickConnects(String instanceId, QuickConnectCSVRecord record) {
        String queueId = record.destinationId();
        // Update existing PhoneNumber Type if its already exist
        if (quickConnectQueueMap.containsKey(queueId)) {
            //Update Quick Connect for Queue Type
            logger.log("Updating Quick Connect for Queue = " + queueId, LogLevel.INFO);
            logger.log("\n");
            updateQuickConnectNameWithRetry(UpdateQuickConnectNameRequest.builder()
                    .instanceId(instanceId)
                    .quickConnectId(quickConnectQueueMap.get(queueId).quickConnectId())
                    .name(record.quickConnectName())
                    .description("Transfer to " + record.quickConnectName())
                    .build());
            connectClient.updateQuickConnectConfig(UpdateQuickConnectConfigRequest.builder()
                    .instanceId(instanceId)
                    .quickConnectId(quickConnectQueueMap.get(queueId).quickConnectId())
                    .quickConnectConfig(QuickConnectConfig.builder()
                            .quickConnectType(QuickConnectType.QUEUE)
                            .queueConfig(QueueQuickConnectConfig.builder()
                                    .queueId(queueId)
                                    .contactFlowId(record.contactFlowId())
                                    .build())
                            .build())
                    .build());
        } else {
            //Create Quick Connect for Queue Type
            logger.log("Creating Quick Connect for Queue = " + queueId, LogLevel.INFO);
            logger.log("\n");
            CreateQuickConnectResponse createQuickConnectResponse = createQuickConnectWithRetry(CreateQuickConnectRequest.builder()
                    .instanceId(instanceId)
                    .name(record.quickConnectName())
                    .description("Transfer to " + record.quickConnectName())
                    .quickConnectConfig(QuickConnectConfig.builder()
                            .quickConnectType(QuickConnectType.QUEUE)
                            .queueConfig(QueueQuickConnectConfig.builder()
                                    .queueId(queueId)
                                    .contactFlowId(record.contactFlowId())
                                    .build())
                            .build())
                    .build());

            quickConnectQueueMap.putIfAbsent(queueId, new QuickConnectRecord(instanceId, record.quickConnectName(), createQuickConnectResponse.quickConnectId(), ""));
        }
        // Associate Quick Connects with Queues
        associateQueues(instanceId, quickConnectQueueMap);

        logger.log("Quick Connects for Queue Type created/updated successfully");
        logger.log("\n");
    }

    private void associateQueues(String instanceId, Map<String, QuickConnectRecord> quickConnectMap) {
        allQueues.forEach(queueSummary -> {
            logger.log("Associating Quick Connects with Queue = " + queueSummary.name(), LogLevel.INFO);
            logger.log("\n");
            associateQueueQuickConnectsWithRetry(AssociateQueueQuickConnectsRequest.builder()
                    .instanceId(instanceId)
                    .quickConnectIds(quickConnectMap.values().stream().map(QuickConnectRecord::quickConnectId).toList())
                    .queueId(queueSummary.id())
                    .build());
        });
    }

    private void buildPhoneQuickConnects(String instanceId, QuickConnectCSVRecord record) {
        String phoneNumber = record.destinationId();
        // Update existing PhoneNumber Type if it already exist
        if (quickConnectPhoneMap.containsKey(phoneNumber)) {
            //Update Quick Connect for Phone Type
            logger.log("Updating Quick Connect for Phone = " + phoneNumber, LogLevel.INFO);
            logger.log("\n");
            updateQuickConnectNameWithRetry(UpdateQuickConnectNameRequest.builder()
                    .instanceId(instanceId)
                    .quickConnectId(quickConnectPhoneMap.get(phoneNumber).quickConnectId())
                    .name(record.quickConnectName())
                    .description("Transfer to " + record.quickConnectName())
                    .build());
            connectClient.updateQuickConnectConfig(UpdateQuickConnectConfigRequest.builder()
                    .instanceId(instanceId)
                    .quickConnectId(quickConnectPhoneMap.get(phoneNumber).quickConnectId())
                    .quickConnectConfig(QuickConnectConfig.builder()
                            .quickConnectType(QuickConnectType.PHONE_NUMBER)
                            .phoneConfig(PhoneNumberQuickConnectConfig.builder()
                                    .phoneNumber(phoneNumber)
                                    .build())
                            .build())
                    .build());
        } else {
            //Create Quick Connect for PhoneNumber Type
            logger.log("Creating Quick Connect for Phone = " + phoneNumber, LogLevel.INFO);
            logger.log("\n");
            CreateQuickConnectResponse createQuickConnectResponse = createQuickConnectWithRetry(CreateQuickConnectRequest.builder()
                    .instanceId(instanceId)
                    .name(record.quickConnectName())
                    .description("Transfer to " + record.quickConnectName())
                    .quickConnectConfig(QuickConnectConfig.builder()
                            .quickConnectType(QuickConnectType.PHONE_NUMBER)
                            .phoneConfig(PhoneNumberQuickConnectConfig.builder()
                                    .phoneNumber(phoneNumber)
                                    .build())
                            .build())
                    .build());

            quickConnectPhoneMap.putIfAbsent(phoneNumber, new QuickConnectRecord(instanceId, record.quickConnectName(), createQuickConnectResponse.quickConnectId(), ""));

        }

        // Associate Quick Connects with Queues
        associateQueues(instanceId, quickConnectPhoneMap);

        logger.log("Quick Connects for PhoneNumber Type created/updated successfully");
        logger.log("\n");
    }


    private void buildUserQuickConnects(String instanceId, String agentTransferFlowId) {
        List<String> deletedUserIdList = new ArrayList<>();

        // Build a list of current users in the Instance
        SearchUsersResponse searchUsersResponse = connectClient.searchUsers(SearchUsersRequest.builder()
                .instanceId(instanceId)
                .maxResults(500)
                .build());

        searchUsersResponse.users().forEach(user -> {
            if (quickConnectUserMap.isEmpty() || quickConnectUserMap.get(user.id()) == null) {
                logger.log("Creating Quick Connect for User = " + user.username(), LogLevel.INFO);
                logger.log("\n");
                //Create Quick Connect for this User
                String quickConnectName = user.identityInfo().firstName() + " " + user.identityInfo().lastName();
                CreateQuickConnectResponse createQuickConnectResponse = createQuickConnectWithRetry(CreateQuickConnectRequest.builder()
                        .instanceId(instanceId)
                        .name(quickConnectName)
                        .description("Transfer to " + user.identityInfo().firstName() + " " + user.identityInfo().lastName())
                        .quickConnectConfig(QuickConnectConfig.builder()
                                .quickConnectType(QuickConnectType.USER)
                                .userConfig(UserQuickConnectConfig.builder()
                                        .contactFlowId(agentTransferFlowId)
                                        .userId(user.id())
                                        .build())
                                .build())
                        .build());
                quickConnectUserMap.put(user.id(), new QuickConnectRecord(instanceId, quickConnectName, createQuickConnectResponse.quickConnectId(), user.id()));
            } else {
                //Update Quick Connect for User Type
                logger.log("Updating Quick Connect for User = " + user.username(), LogLevel.INFO);
                logger.log("\n");
                updateQuickConnectNameWithRetry(UpdateQuickConnectNameRequest.builder()
                        .instanceId(instanceId)
                        .quickConnectId(quickConnectUserMap.get(user.id()).quickConnectId())
                        .name(user.identityInfo().firstName() + " " + user.identityInfo().lastName())
                        .description("Transfer to " + user.identityInfo().firstName() + " " + user.identityInfo().lastName())
                        .build());
                connectClient.updateQuickConnectConfig(UpdateQuickConnectConfigRequest.builder()
                        .instanceId(instanceId)
                        .quickConnectId(quickConnectUserMap.get(user.id()).quickConnectId())
                        .quickConnectConfig(QuickConnectConfig.builder()
                                .quickConnectType(QuickConnectType.USER)
                                .userConfig(UserQuickConnectConfig.builder()
                                        .contactFlowId(agentTransferFlowId)
                                        .userId(user.id())
                                        .build())
                                .build())
                        .build());
            }
        });

        // Remove Users from Quick Connect
        quickConnectUserMap.values().forEach(quickConnect -> {
            if (searchUsersResponse.users().stream().noneMatch(user -> user.id().equals(quickConnect.userId()))) {
                logger.log("Removing Quick Connect for User = " + quickConnect.quickConnectName(), LogLevel.INFO);
                logger.log("\n");
                deleteQuickConnectWithRetry(DeleteQuickConnectRequest.builder()
                        .instanceId(instanceId)
                        .quickConnectId(quickConnect.quickConnectId())
                        .build());
                deletedUserIdList.add(quickConnect.userId());
            }
        });

        // Update the quickConnectMap to remove the deleted users
        deletedUserIdList.forEach(quickConnectUserMap::remove);

        // Associate Quick Connects with Queues
        associateQueues(instanceId, quickConnectUserMap);

        logger.log("Quick Connects for User Type created/updated successfully");
        logger.log("\n");
    }

    private List<QueueSummary> getQueueSummaryList(String instanceId) {
        List<QueueSummary> allQueues = new ArrayList<>();
        String nextToken = null;

        do {
            ListQueuesResponse listQueuesResponse = connectClient.listQueues(
                    ListQueuesRequest.builder()
                            .instanceId(instanceId)
                            .queueTypes(QueueType.STANDARD)
                            .nextToken(nextToken)
                            .build()
            );

            // Add the current page of queues to our complete list
            allQueues.addAll(listQueuesResponse.queueSummaryList());

            // Get the next token for the next page
            nextToken = listQueuesResponse.nextToken();

        } while (nextToken != null);
        return allQueues;
    }

    private String getDefaultAgentTransferFlowId(String instanceId) {
        SearchContactFlowsResponse searchContactFlowsResponse = connectClient.searchContactFlows(SearchContactFlowsRequest.builder()
                .instanceId(instanceId)
                .searchCriteria(ContactFlowSearchCriteria.builder()
                        .andConditions(
                                ContactFlowSearchCriteria.builder()
                                        .typeCondition(ContactFlowType.AGENT_TRANSFER)
                                        .build(),
                                ContactFlowSearchCriteria.builder()
                                        .stateCondition(ContactFlowState.ACTIVE)
                                        .build(),
                                ContactFlowSearchCriteria.builder()
                                        .statusCondition(ContactFlowStatus.PUBLISHED)
                                        .build(),
                                ContactFlowSearchCriteria.builder()
                                        .stringCondition(StringCondition.builder()
                                                .fieldName("Name")
                                                .value("Default agent transfer")
                                                .comparisonType(StringComparisonType.CONTAINS)
                                                .build())
                                        .build()
                        )
                        .build())
                .build());
        if (searchContactFlowsResponse.contactFlows().size() == 1) {
            return searchContactFlowsResponse.contactFlows().getFirst().id();
        } else if (searchContactFlowsResponse.contactFlows().size() > 1) {
            return "More than one Default agent transfer contact flow found";
        } else {
            return "Default agent transfer contact flow not found";
        }
    }

    private void getQuickConnects(String instanceId) {
        // Build a list of all Quick Connects
        SearchQuickConnectsResponse searchQuickConnectsResponse = connectClient.searchQuickConnects(SearchQuickConnectsRequest.builder()
                .instanceId(instanceId)
                .build());

        searchQuickConnectsResponse.quickConnects().stream().filter(quickConnect -> quickConnect != null && quickConnect.quickConnectConfig().userConfig() != null).forEach(quickConnect -> {
            quickConnectUserMap.putIfAbsent(quickConnect.quickConnectConfig().userConfig().userId(), new QuickConnectRecord(instanceId, quickConnect.name(), quickConnect.quickConnectId(), quickConnect.quickConnectConfig().userConfig().userId()));
        });

        searchQuickConnectsResponse.quickConnects().stream().filter(quickConnect -> quickConnect != null && quickConnect.quickConnectConfig().phoneConfig() != null).forEach(quickConnect -> {
            quickConnectPhoneMap.putIfAbsent(quickConnect.quickConnectConfig().phoneConfig().phoneNumber(), new QuickConnectRecord(instanceId, quickConnect.name(), quickConnect.quickConnectId(), quickConnect.quickConnectConfig().phoneConfig().phoneNumber()));
        });

        searchQuickConnectsResponse.quickConnects().stream().filter(quickConnect -> quickConnect != null && quickConnect.quickConnectConfig().queueConfig() != null).forEach(quickConnect -> {
            quickConnectQueueMap.putIfAbsent(quickConnect.quickConnectConfig().queueConfig().queueId(), new QuickConnectRecord(instanceId, quickConnect.name(), quickConnect.quickConnectId(), quickConnect.quickConnectConfig().queueConfig().queueId()));
        });


    }

    private List<QuickConnectCSVRecord> readCsvFromS3(String bucket, String key) throws IOException {
        List<QuickConnectCSVRecord> records = new ArrayList<>();

        // To handle special characters in the S3 key
        String decodedKey = URLDecoder.decode(key, StandardCharsets.UTF_8);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(decodedKey)
                .build();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                s3Client.getObject(getObjectRequest)))) {

            // Skip header row
            String line = br.readLine();

            // Read data rows
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length >= 4) {
                    records.add(new QuickConnectCSVRecord(
                            values[0].trim(), // quickConnectName
                            values[1].trim(), // quickConnectType
                            values[2].trim(), // destinationId
                            values[3].trim(),  // contactFlowId
                            values[4].trim()   // description
                    ));
                }
            }
        }

        return records;
    }
}

// Record to hold record data
record QuickConnectRecord(String instanceId, String quickConnectName, String quickConnectId, String userId) {
}

record QuickConnectCSVRecord(String quickConnectName, String quickConnectType, String destinationId,
                             String contactFlowId, String description) {
}