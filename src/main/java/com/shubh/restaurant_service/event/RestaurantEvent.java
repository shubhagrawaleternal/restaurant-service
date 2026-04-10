
package com.shubh.restaurant_service.event;

import com.shubh.restaurant_service.dto.RestaurantDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RestaurantEvent implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private String eventId;
    private EventType eventType;
    private RestaurantDTO restaurantData;
    private Long timestamp;
    private Long version;
    private String city;
    private Double rating;
    private String cuisineType;
    
    public enum EventType {
        CREATE,
        UPDATE,
        DELETE
    }
}
