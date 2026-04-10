
package com.shubh.restaurant_service.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

@Slf4j
@Configuration
public class DynamoDbConfig {
    
    @Value("${aws.dynamodb.endpoint}")
    private String endpoint;
    
    @Value("${aws.dynamodb.region}")
    private String region;
    
    @Value("${aws.dynamodb.accessKey}")
    private String accessKey;
    
    @Value("${aws.dynamodb.secretKey}")
    private String secretKey;
    
    @Value("${aws.dynamodb.table.name}")
    private String tableName;
    
    @Bean
    public DynamoDbClient dynamoDbClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                    )
                )
                .build();
    }
    
    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }
    
    @Bean
    public String dynamoDbTableName() {
        return tableName;
    }
}
