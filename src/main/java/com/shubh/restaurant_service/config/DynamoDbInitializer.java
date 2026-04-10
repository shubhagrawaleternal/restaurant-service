
package com.shubh.restaurant_service.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@Slf4j
@Component
public class DynamoDbInitializer {

    @Autowired
    private DynamoDbClient dynamoDbClient;

    @PostConstruct
    public void initializeTables() {
        log.info("Initializing DynamoDB tables...");
        createTableIfNotExists();
    }

    private void createTableIfNotExists() {
        try {
            DescribeTableRequest describeRequest = DescribeTableRequest.builder()
                    .tableName("restaurants")
                    .build();
            
            try {
                dynamoDbClient.describeTable(describeRequest);
                log.info("DynamoDB table 'restaurants' already exists");
            } catch (ResourceNotFoundException e) {
                log.info("Creating DynamoDB table 'restaurants' with GSI...");
                
                CreateTableRequest createTableRequest = CreateTableRequest.builder()
                        .tableName("restaurants")
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName("city")
                                        .keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder()
                                        .attributeName("sortKey")
                                        .keyType(KeyType.RANGE)
                                        .build()
                        )
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName("city")
                                        .attributeType(ScalarAttributeType.S)
                                        .build(),
                                AttributeDefinition.builder()
                                        .attributeName("sortKey")
                                        .attributeType(ScalarAttributeType.S)
                                        .build(),
                                AttributeDefinition.builder()
                                        .attributeName("cityCuisine")
                                        .attributeType(ScalarAttributeType.S)
                                        .build()
                        )
                        .globalSecondaryIndexes(
                                GlobalSecondaryIndex.builder()
                                        .indexName("city-cuisine-rating-index")
                                        .keySchema(
                                                KeySchemaElement.builder()
                                                        .attributeName("cityCuisine")
                                                        .keyType(KeyType.HASH)
                                                        .build(),
                                                KeySchemaElement.builder()
                                                        .attributeName("sortKey")
                                                        .keyType(KeyType.RANGE)
                                                        .build()
                                        )
                                        .projection(Projection.builder()
                                                .projectionType(ProjectionType.ALL)
                                                .build())
                                        .provisionedThroughput(ProvisionedThroughput.builder()
                                                .readCapacityUnits(5L)
                                                .writeCapacityUnits(5L)
                                                .build())
                                        .build()
                        )
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(5L)
                                .writeCapacityUnits(5L)
                                .build())
                        .build();
                
                dynamoDbClient.createTable(createTableRequest);
                log.info("Successfully created DynamoDB table 'restaurants' with GSI");
            }
        } catch (Exception e) {
            log.error("Error initializing DynamoDB table 'restaurants'", e);
            throw new RuntimeException("Failed to initialize DynamoDB table", e);
        }
    }
}
