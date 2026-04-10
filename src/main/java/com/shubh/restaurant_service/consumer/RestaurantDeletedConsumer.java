
package com.shubh.restaurant_service.consumer;

import com.shubh.restaurant_service.event.RestaurantEvent;
import com.shubh.restaurant_service.model.DynamoRestaurant;
import com.shubh.restaurant_service.repository.DynamoDbRepository;
import com.shubh.restaurant_service.service.IdempotencyService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import com.shubh.restaurant_service.service.CacheInvalidationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RestaurantDeletedConsumer {
    
    private final DynamoDbRepository dynamoDbRepository;
    private final CacheInvalidationService cacheInvalidationService;
    private final IdempotencyService idempotencyService;
    
    private static final String EVENT_TYPE = "restaurant-deleted";
    
    @KafkaListener(
        topics = "${kafka.topic.restaurant-deleted}",
        groupId = "${kafka.consumer.group.deleted}",
        concurrency = "3"
    )
    @CircuitBreaker(name = "dynamodb", fallbackMethod = "handleDynamoDbFailure")
    public void consume(RestaurantEvent event, Acknowledgment ack) {
        try {
            String restaurantId = event.getRestaurantData().getId();
            String city = event.getCity();
            Double rating = event.getRating();
            String cuisineType = event.getCuisineType();
            
            log.info("Received restaurant.deleted event: restaurantId={}, city={}, rating={}, cuisine={}",
                    restaurantId, city, rating, cuisineType);
            
            if (idempotencyService.isProcessed(EVENT_TYPE, restaurantId)) {
                log.warn("🔴 Duplicate deletion - already processed: {}", restaurantId);
                ack.acknowledge();
                return;
            }
            
            if (city == null || city.trim().isEmpty()) {
                log.error("❌ Cannot delete from DynamoDB: city is null or empty for restaurantId={}", restaurantId);
                ack.acknowledge();
                return;
            }
            
            cacheInvalidationService.invalidateRestaurantCache(restaurantId, city, cuisineType);
            
            String sortKey = DynamoRestaurant.generateSortKey(rating, restaurantId);
            
            dynamoDbRepository.deleteByCityAndSortKey(city, sortKey);
            log.info("✅ Deleted restaurant from DynamoDB: restaurantId={}, city={}, sortKey={}",
                    restaurantId, city, sortKey);
            
            idempotencyService.markAsProcessed(EVENT_TYPE, restaurantId);
            ack.acknowledge();
            
        } catch (Exception e) {
            log.error("❌ Error processing restaurant.deleted event: {}", event.getEventId(), e);
        }
    }
    
    private void handleDynamoDbFailure(RestaurantEvent event, Acknowledgment ack, Exception e) {
        log.error("DynamoDB unavailable for restaurant deletion: {}",
                event.getRestaurantData().getId(), e);
    }
    
    
}
