
package com.shubh.restaurant_service.repository;

import com.shubh.restaurant_service.model.DynamoRestaurant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class DynamoDbRepository {
    
    private final DynamoDbEnhancedClient enhancedClient;
    private final String tableName;
    
    private DynamoDbTable<DynamoRestaurant> getTable() {
        return enhancedClient.table(tableName, TableSchema.fromBean(DynamoRestaurant.class));
    }
    
    public void save(DynamoRestaurant restaurant) {
        try {
            getTable().putItem(restaurant);
            log.info("Successfully saved restaurant to DynamoDB: {}", restaurant.getRestaurantId());
        } catch (Exception e) {
            log.error("Error saving restaurant to DynamoDB: {}", restaurant.getRestaurantId(), e);
            throw new RuntimeException("Failed to save to DynamoDB", e);
        }
    }
    
    public void deleteByCityAndSortKey(String city, String sortKey) {
        try {
            Key key = Key.builder()
                    .partitionValue(city)
                    .sortValue(sortKey)
                    .build();
            getTable().deleteItem(key);
            log.info("Deleted restaurant from DynamoDB: city={}, sortKey={}", city, sortKey);
        } catch (Exception e) {
            log.error("Error deleting restaurant from DynamoDB: city={}, sortKey={}", city, sortKey, e);
            throw new RuntimeException("Failed to delete from DynamoDB", e);
        }
    }
    
    public QueryResult queryByCity(String city, int limit, Map<String, AttributeValue> exclusiveStartKey) {
        try {
            QueryConditional queryConditional = QueryConditional.keyEqualTo(
                    Key.builder().partitionValue(city).build()
            );
            
            QueryEnhancedRequest.Builder requestBuilder = QueryEnhancedRequest.builder()
                    .queryConditional(queryConditional)
                    .limit(limit)
                    .scanIndexForward(false);
            
            if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
                requestBuilder.exclusiveStartKey(exclusiveStartKey);
            }
            
            QueryEnhancedRequest request = requestBuilder.build();
            PageIterable<DynamoRestaurant> pages = getTable().query(request);
            
            List<DynamoRestaurant> restaurants = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            
            for (Page<DynamoRestaurant> page : pages) {
                restaurants.addAll(page.items());
                lastEvaluatedKey = page.lastEvaluatedKey();
                break;
            }
            
            log.info("Retrieved {} restaurants from DynamoDB for city: {}", restaurants.size(), city);
            return QueryResult.builder()
                    .items(restaurants)
                    .lastEvaluatedKey(lastEvaluatedKey)
                    .build();
            
        } catch (Exception e) {
            log.error("Error querying restaurants by city from DynamoDB: {}", city, e);
            return QueryResult.builder()
                    .items(new ArrayList<>())
                    .build();
        }
    }
    
    public QueryResult queryByCityAndCuisine(String city, String cuisine, int limit, Map<String, AttributeValue> exclusiveStartKey) {
        try {
            DynamoDbIndex<DynamoRestaurant> index = getTable().index("city-cuisine-rating-index");
            
            String cityCuisineKey = DynamoRestaurant.generateCityCuisineKey(city, cuisine);
            QueryConditional queryConditional = QueryConditional.keyEqualTo(
                    Key.builder().partitionValue(cityCuisineKey).build()
            );
            
            QueryEnhancedRequest.Builder requestBuilder = QueryEnhancedRequest.builder()
                    .queryConditional(queryConditional)
                    .limit(limit)
                    .scanIndexForward(false);
            
            if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
                requestBuilder.exclusiveStartKey(exclusiveStartKey);
            }
            
            QueryEnhancedRequest request = requestBuilder.build();
            SdkIterable<Page<DynamoRestaurant>> pages = index.query(request);
            
            List<DynamoRestaurant> restaurants = new ArrayList<>();
            Map<String, AttributeValue> lastEvaluatedKey = null;
            
            for (Page<DynamoRestaurant> page : pages) {
                restaurants.addAll(page.items());
                lastEvaluatedKey = page.lastEvaluatedKey();
                break;
            }
            
            log.info("Retrieved {} restaurants from DynamoDB for city: {} and cuisine: {}",
                    restaurants.size(), city, cuisine);
            return QueryResult.builder()
                    .items(restaurants)
                    .lastEvaluatedKey(lastEvaluatedKey)
                    .build();
            
        } catch (Exception e) {
            log.error("Error querying restaurants by city and cuisine from DynamoDB: {} {}", city, cuisine, e);
            return QueryResult.builder()
                    .items(new ArrayList<>())
                    .build();
        }
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    public static class QueryResult {
        private List<DynamoRestaurant> items;
        private Map<String, AttributeValue> lastEvaluatedKey;
    }
}
