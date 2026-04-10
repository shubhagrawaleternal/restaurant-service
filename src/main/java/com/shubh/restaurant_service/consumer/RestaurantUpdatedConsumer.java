
package com.shubh.restaurant_service.consumer;

import com.shubh.restaurant_service.event.RestaurantEvent;
import com.shubh.restaurant_service.model.DynamoRestaurant;
import com.shubh.restaurant_service.repository.DynamoDbRepository;
import com.shubh.restaurant_service.service.IdempotencyService;
import com.shubh.restaurant_service.util.RestaurantMapper;
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
public class RestaurantUpdatedConsumer {
    
    private final DynamoDbRepository dynamoDbRepository;
    private final CacheInvalidationService cacheInvalidationService;
    private final IdempotencyService idempotencyService;
    
    @KafkaListener(
        topics = "${kafka.topic.restaurant-updated}",
        groupId = "${kafka.consumer.group.updated}",
        concurrency = "3"
    )
    @CircuitBreaker(name = "dynamodb", fallbackMethod = "handleDynamoDbFailure")
    public void consume(RestaurantEvent event, Acknowledgment ack) {
        try {
            log.info("Received restaurant.updated event: restaurantId={}, version={}",
                    event.getRestaurantData().getId(), event.getVersion());
            
            String restaurantId = event.getRestaurantData().getId();
            Long eventVersion = event.getVersion();
            
            if (idempotencyService.isStaleVersion(restaurantId, eventVersion)) {
                log.warn("🔴 Stale event - skipping: restaurantId={}, eventVersion={}",
                        restaurantId, eventVersion);
                ack.acknowledge();
                return;
            }
            
            DynamoRestaurant dynamoRestaurant = RestaurantMapper.toDynamoModel(event.getRestaurantData());
            
            String oldCity = event.getCity();
            Double oldRating = event.getRating();
            String newCity = dynamoRestaurant.getCity();
            Double newRating = event.getRestaurantData().getRating();
            
            boolean cityChanged = oldCity != null && !oldCity.equals(newCity);
            boolean ratingChanged = oldRating != null && !oldRating.equals(newRating);
            
            // If city or rating changed, we need to delete the old DynamoDB entry
            // because city is the partition key and rating is part of the sort key
            if (cityChanged || ratingChanged) {
                String oldSortKey = DynamoRestaurant.generateSortKey(
                        oldRating != null ? oldRating : newRating,
                        restaurantId);
                String oldCityValue = oldCity != null ? oldCity : newCity;
                
                log.info("🔄 Keys changed - deleting old DynamoDB entry: oldCity={}, oldRating={}, oldSortKey={}",
                        oldCityValue, oldRating, oldSortKey);
                
                try {
                    dynamoDbRepository.deleteByCityAndSortKey(oldCityValue, oldSortKey);
                    log.info("✅ Deleted old DynamoDB entry: city={}, sortKey={}", oldCityValue, oldSortKey);
                } catch (Exception e) {
                    log.warn("⚠️ Failed to delete old DynamoDB entry (may not exist): city={}, sortKey={}, error={}",
                            oldCityValue, oldSortKey, e.getMessage());
                }
            }
            
            if (cityChanged || ratingChanged) {
                cacheInvalidationService.invalidateRestaurantCache(
                    restaurantId,
                    oldCity,
                    event.getCuisineType()
                );
            }
            
            // Invalidate caches for new city/cuisine
            cacheInvalidationService.invalidateRestaurantCache(
                restaurantId,
                newCity,
                event.getRestaurantData().getCuisineType()
            );
            
            dynamoDbRepository.save(dynamoRestaurant);
            log.info("✅ Updated restaurant in DynamoDB: {} (version: {}, city: {}, rating: {})",
                    restaurantId, eventVersion, dynamoRestaurant.getCity(), newRating);
            
            idempotencyService.setVersion(restaurantId, eventVersion);
            ack.acknowledge();
            
        } catch (Exception e) {
            log.error("Error processing restaurant.updated event: {}", event.getEventId(), e);
        }
    }
    
    private void handleDynamoDbFailure(RestaurantEvent event, Acknowledgment ack, Exception e) {
        log.error("DynamoDB unavailable for restaurant: {}", event.getRestaurantData().getId(), e);
    }
    
    
}
