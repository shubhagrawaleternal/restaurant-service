
package com.shubh.restaurant_service.grpc;

import com.shubh.restaurant_service.dto.PaginationResult;
import com.shubh.restaurant_service.dto.RestaurantDTO;
import com.shubh.restaurant_service.service.RestaurantService;
import com.shubh.restaurant_service.util.ValidationUtil;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;

import java.time.format.DateTimeFormatter;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class RestaurantGrpcService extends RestaurantServiceGrpc.RestaurantServiceImplBase {
    
    private final RestaurantService restaurantService;
    
    @Override
    public void createRestaurant(CreateRestaurantRequest request,
                                 StreamObserver<RestaurantResponse> responseObserver) {
        log.info("gRPC CreateRestaurant called for: {} (async: {})",
                request.getName(), request.getAsyncMode());
        
        try {
            
            ValidationUtil.requireNonEmpty(request.getName(), "Restaurant name");
            ValidationUtil.requireNonEmpty(request.getContactEmail(), "Contact email");
            ValidationUtil.requireNonEmpty(request.getCity(), "City");

            RestaurantDTO dto = RestaurantDTO.builder()
                    .name(request.getName())
                    .address(request.hasAddress() ? request.getAddress() : null)
                    .contactEmail(request.getContactEmail())
                    .contactNumber(request.hasContactNumber() ? request.getContactNumber() : null)
                    .cuisineType(request.hasCuisineType() ? request.getCuisineType() : null)
                    .city(request.getCity())
                    .latitude(request.hasLatitude() ? request.getLatitude() : null)
                    .longitude(request.hasLongitude() ? request.getLongitude() : null)
                    .rating(request.hasRating() ? request.getRating() : null)
                    .totalReviews(request.hasTotalReviews() ? request.getTotalReviews() : null)
                    .priceRange(request.hasPriceRange() ? request.getPriceRange() : null)
                    .isOpen(request.hasIsOpen() ? request.getIsOpen() : true)
                    .imageUrl(request.hasImageUrl() ? request.getImageUrl() : null)
                    .description(request.hasDescription() ? request.getDescription() : null)
                    .tags(request.hasTags() ? request.getTags() : null)
                    .build();
            
            RestaurantDTO created = request.getAsyncMode()
                    ? restaurantService.createRestaurantAsync(dto)
                    : restaurantService.createRestaurantSync(dto);
            
            RestaurantResponse response = RestaurantResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage(request.getAsyncMode()
                        ? "Restaurant creation initiated (async)"
                        : "Restaurant created successfully")
                    .setData(convertToProto(created))
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (IllegalArgumentException e) {
            log.error("Validation error creating restaurant: {}", e.getMessage());
            sendErrorResponse(responseObserver, e.getMessage());
        } catch (Exception e) {
            log.error("Error creating restaurant", e);
            sendErrorResponse(responseObserver, "Failed to create restaurant: " + e.getMessage());
        }
    }
    
    @Override
    public void updateRestaurant(UpdateRestaurantRequest request,
                                 StreamObserver<RestaurantResponse> responseObserver) {
        log.info("gRPC UpdateRestaurant called for ID: {}", request.getId());
        
        try {
            if (request.getId() == null || request.getId().trim().isEmpty()) {
                sendErrorResponse(responseObserver, "Restaurant ID is required for update");
                return;
            }
            
            RestaurantDTO dto = RestaurantDTO.builder()
                    .name(request.hasName() ? request.getName() : null)
                    .address(request.hasAddress() ? request.getAddress() : null)
                    .contactEmail(request.hasContactEmail() ? request.getContactEmail() : null)
                    .contactNumber(request.hasContactNumber() ? request.getContactNumber() : null)
                    .cuisineType(request.hasCuisineType() ? request.getCuisineType() : null)
                    .city(request.hasCity() ? request.getCity() : null)
                    .latitude(request.hasLatitude() ? request.getLatitude() : null)
                    .longitude(request.hasLongitude() ? request.getLongitude() : null)
                    .rating(request.hasRating() ? request.getRating() : null)
                    .totalReviews(request.hasTotalReviews() ? request.getTotalReviews() : null)
                    .priceRange(request.hasPriceRange() ? request.getPriceRange() : null)
                    .isOpen(request.hasIsOpen() ? request.getIsOpen() : true)
                    .imageUrl(request.hasImageUrl() ? request.getImageUrl() : null)
                    .description(request.hasDescription() ? request.getDescription() : null)
                    .tags(request.hasTags() ? request.getTags() : null)
                    .build();
            
            RestaurantDTO updated = restaurantService.updateRestaurant(request.getId(), dto);
            
            RestaurantResponse response = RestaurantResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Restaurant updated successfully")
                    .setData(convertToProto(updated))
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error updating restaurant", e);
            sendErrorResponse(responseObserver, "Failed to update restaurant: " + e.getMessage());
        }
    }
    
    @Override
    public void getRestaurant(GetRestaurantRequest request,
                             StreamObserver<RestaurantResponse> responseObserver) {
        log.info("gRPC GetRestaurant called for ID: {}", request.getId());
        
        try {
            if (request.getId() == null || request.getId().trim().isEmpty()) {
                sendErrorResponse(responseObserver, "Restaurant ID is required");
                return;
            }
            
            RestaurantDTO restaurant = restaurantService.getRestaurantById(request.getId());
            
            RestaurantResponse response = RestaurantResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Restaurant retrieved successfully")
                    .setData(convertToProto(restaurant))
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error getting restaurant", e);
            sendErrorResponse(responseObserver, "Failed to get restaurant: " + e.getMessage());
        }
    }
    
    @Override
    public void listRestaurants(ListRestaurantsRequest request,
                               StreamObserver<ListRestaurantsResponse> responseObserver) {
        log.info("gRPC ListRestaurants called - city: {}, cuisine: {}, pageSize: {}, cursor: {}",
                request.getCity(),
                request.hasCuisineType() ? request.getCuisineType() : "all",
                request.getPageSize(),
                request.hasCursor() ? "present" : "null");
        
        try {
            if (request.getCity() == null || request.getCity().trim().isEmpty()) {
                ListRestaurantsResponse response = ListRestaurantsResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("City is required for DynamoDB query")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            String city = request.getCity();
            String cuisineType = request.hasCuisineType() ? request.getCuisineType() : null;
            int pageSize = request.getPageSize() > 0 ? request.getPageSize() : 10;
            String cursor = request.hasCursor() ? request.getCursor() : null;
            
            if (pageSize > 100) {
                ListRestaurantsResponse response = ListRestaurantsResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Page size cannot exceed 100")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            PaginationResult<RestaurantDTO> result = restaurantService.listRestaurantsByCityAndCuisine(
                    city, cuisineType, pageSize, cursor);
            
            ListRestaurantsResponse.Builder responseBuilder = ListRestaurantsResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Restaurants retrieved successfully")
                    .setCount(result.getCount())
                    .setHasMore(result.isHasMore());
            
            if (result.getNextCursor() != null) {
                responseBuilder.setNextCursor(result.getNextCursor());
            }
            
            result.getItems().forEach(dto ->
                responseBuilder.addRestaurants(convertToProto(dto))
            );
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
            log.info("✅ Returned {} restaurants, hasMore={}", result.getCount(), result.isHasMore());
            
        } catch (io.github.resilience4j.ratelimiter.RequestNotPermitted e) {
            log.error("Rate limit exceeded for list restaurants", e);
            
            ListRestaurantsResponse response = ListRestaurantsResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Rate limit exceeded. Please try again later.")
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Error listing restaurants", e);
            
            ListRestaurantsResponse response = ListRestaurantsResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to list restaurants: " + e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    @Override
    public void deleteRestaurant(DeleteRestaurantRequest request,
                                StreamObserver<DeleteRestaurantResponse> responseObserver) {
        log.info("gRPC DeleteRestaurant called for ID: {}", request.getId());
        
        try {
            if (request.getId() == null || request.getId().trim().isEmpty()) {
                DeleteRestaurantResponse response = DeleteRestaurantResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Restaurant ID is required")
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            restaurantService.deleteRestaurant(request.getId());
            
            DeleteRestaurantResponse response = DeleteRestaurantResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Restaurant deleted successfully")
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            log.error("Error deleting restaurant", e);
            
            DeleteRestaurantResponse response = DeleteRestaurantResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to delete restaurant: " + e.getMessage())
                    .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
    
    private RestaurantData convertToProto(RestaurantDTO dto) {
        RestaurantData.Builder builder = RestaurantData.newBuilder()
                .setId(dto.getId())
                .setName(dto.getName());
        
        if (dto.getAddress() != null) {
            builder.setAddress(dto.getAddress());
        }
        if (dto.getContactEmail() != null) {
            builder.setContactEmail(dto.getContactEmail());
        }
        if (dto.getContactNumber() != null) {
            builder.setContactNumber(dto.getContactNumber());
        }
        if (dto.getCuisineType() != null) {
            builder.setCuisineType(dto.getCuisineType());
        }
        if (dto.getCreatedAt() != null) {
            builder.setCreatedAt(dto.getCreatedAt().format(DateTimeFormatter.ISO_DATE_TIME));
        }
        if (dto.getUpdatedAt() != null) {
            builder.setUpdatedAt(dto.getUpdatedAt().format(DateTimeFormatter.ISO_DATE_TIME));
        }
        
        if (dto.getCity() != null) {
            builder.setCity(dto.getCity());
        }
        if (dto.getLatitude() != null) {
            builder.setLatitude(dto.getLatitude());
        }
        if (dto.getLongitude() != null) {
            builder.setLongitude(dto.getLongitude());
        }
        
        if (dto.getRating() != null) {
            builder.setRating(dto.getRating());
        }
        if (dto.getTotalReviews() != null) {
            builder.setTotalReviews(dto.getTotalReviews());
        }
        
        if (dto.getPriceRange() != null) {
            builder.setPriceRange(dto.getPriceRange());
        }
        if (dto.getIsOpen() != null) {
            builder.setIsOpen(dto.getIsOpen());
        }
        if (dto.getImageUrl() != null) {
            builder.setImageUrl(dto.getImageUrl());
        }
        if (dto.getDescription() != null) {
            builder.setDescription(dto.getDescription());
        }
        if (dto.getTags() != null) {
            builder.setTags(dto.getTags());
        }
        
        if (dto.getVersion() != null) {
            builder.setVersion(dto.getVersion());
        }
        
        return builder.build();
    }
    
    private void sendErrorResponse(StreamObserver<RestaurantResponse> responseObserver, String message) {
        RestaurantResponse response = RestaurantResponse.newBuilder()
                .setSuccess(false)
                .setMessage(message)
                .build();
        
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
