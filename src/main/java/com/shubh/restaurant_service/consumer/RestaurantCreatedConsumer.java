
package com.shubh.restaurant_service.consumer;

import com.shubh.restaurant_service.event.RestaurantEvent;
import com.shubh.restaurant_service.model.DynamoRestaurant;
import com.shubh.restaurant_service.repository.DynamoDbRepository;
import com.shubh.restaurant_service.service.CacheInvalidationService;
import com.shubh.restaurant_service.service.IdempotencyService;
import com.shubh.restaurant_service.util.RestaurantMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RestaurantCreatedConsumer {
    
    private final DynamoDbRepository dynamoDbRepository;
    private final CacheInvalidationService cacheInvalidationService;
    private final IdempotencyService idempotencyService;
    
    private static final String EVENT_TYPE = "restaurant-created";
    
    @KafkaListener(
        topics = "${kafka.topic.restaurant-created}",
        groupId = "${kafka.consumer.group.created}",
        concurrency = "3"
    )
    @CircuitBreaker(name = "dynamodb", fallbackMethod = "handleDynamoDbFailure")
    public void consume(RestaurantEvent event, Acknowledgment ack) {
        try {
            log.info("Received restaurant.created event: restaurantId={}, version={}",
                    event.getRestaurantData().getId(), event.getVersion());
            
            String restaurantId = event.getRestaurantData().getId();
            
            if (idempotencyService.isProcessed(EVENT_TYPE, restaurantId)) {
                log.warn("🔴 Duplicate event - already processed: {}", restaurantId);
                ack.acknowledge();
                return;
            }
            
            log.debug("Event data before mapping: id={}, city={}, cuisineType={}, rating={}",
                    event.getRestaurantData().getId(),
                    event.getRestaurantData().getCity(),
                    event.getRestaurantData().getCuisineType(),
                    event.getRestaurantData().getRating());
            
            DynamoRestaurant dynamoRestaurant = RestaurantMapper.toDynamoModel(event.getRestaurantData());
            
            if (dynamoRestaurant.getCity() == null || dynamoRestaurant.getSortKey() == null) {
                log.error("❌ Invalid DynamoDB keys: city={}, sortKey={}",
                        dynamoRestaurant.getCity(), dynamoRestaurant.getSortKey());
                throw new IllegalStateException("DynamoDB keys cannot be null");
            }
            
            log.debug("DynamoDB keys: city={}, sortKey={}",
                    dynamoRestaurant.getCity(), dynamoRestaurant.getSortKey());
            
            dynamoDbRepository.save(dynamoRestaurant);
            log.info("✅ Saved restaurant to DynamoDB: {} (city: {})", restaurantId, dynamoRestaurant.getCity());
            
            // Invalidate list caches for this city and cuisine
            cacheInvalidationService.invalidateRestaurantCache(
                restaurantId,
                dynamoRestaurant.getCity(),
                dynamoRestaurant.getCuisineType()
            );
            
            idempotencyService.markAsProcessed(EVENT_TYPE, restaurantId);
            ack.acknowledge();
            
        } catch (Exception e) {
            log.error("❌ Error processing restaurant.created event: {}", event.getEventId(), e);
        }
    }
    
    private void handleDynamoDbFailure(RestaurantEvent event, Acknowledgment ack, Exception e) {
        log.error("DynamoDB unavailable for restaurant: {}", event.getRestaurantData().getId(), e);
    }
}
