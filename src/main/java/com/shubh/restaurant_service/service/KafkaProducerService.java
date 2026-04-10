
package com.shubh.restaurant_service.service;

import com.shubh.restaurant_service.event.RestaurantEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
    
    private final KafkaTemplate<String, RestaurantEvent> kafkaTemplate;
    
    public void publishEvent(String topic, String restaurantId, RestaurantEvent event) {
        try {
            log.info("Publishing event: {} to topic: {} for restaurant: {}",
                    event.getEventType(), topic, restaurantId);
            
            SendResult<String, RestaurantEvent> result =
                kafkaTemplate.send(topic, restaurantId, event).get();
            
            log.info("Published event: {} to partition: {} offset: {}",
                    event.getEventType(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
                    
        } catch (Exception e) {
            log.error("Failed to publish event to topic: {}", topic, e);
            throw new RuntimeException("Failed to publish event to Kafka: " + topic, e);
        }
    }
}
