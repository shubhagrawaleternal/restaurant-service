
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
public class PaginationResult<T> {
    private List<T> items;
    private int count;
    private String nextCursor;
    private boolean hasMore;
}
