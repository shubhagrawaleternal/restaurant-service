
package com.shubh.restaurant_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaginatedRestaurantResponse {
    private List<RestaurantDTO> restaurants;
    private String nextPageToken;
    private int count;
}
