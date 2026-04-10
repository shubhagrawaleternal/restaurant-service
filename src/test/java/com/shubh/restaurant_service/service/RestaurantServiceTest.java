
package com.shubh.restaurant_service.service;

import com.shubh.restaurant_service.dto.RestaurantDTO;
import com.shubh.restaurant_service.entity.Restaurant;
import com.shubh.restaurant_service.repository.DynamoDbRepository;
import com.shubh.restaurant_service.repository.RestaurantRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

/**
 * Unit Tests for RestaurantService
 * 
 * Testing Strategy:
 * - Unit tests: Test individual methods in isolation
 * - Mocking: Use Mockito to mock dependencies
 * - Assertions: Verify expected behavior
 * - Coverage: Test happy path and error cases
 * 
 * Why Test?
 * - Catch bugs early
 * - Document expected behavior
 * - Enable refactoring with confidence
 * - Regression prevention
 * 
 * Test Structure (AAA Pattern):
 * - Arrange: Set up test data and mocks
 * - Act: Execute the method being tested
 * - Assert: Verify the results
 */
@ExtendWith(MockitoExtension.class)
class RestaurantServiceTest {
    
    @Mock
    private RestaurantRepository restaurantRepository;
    
    @Mock
    private DynamoDbRepository dynamoDbRepository;
    
    @Mock
    private KafkaProducerService kafkaProducerService;
    
    @Mock
    private RedisTemplate<String, String> redisTemplate;
    
    @InjectMocks
    private RestaurantService restaurantService;
    
    private RestaurantDTO testRestaurantDTO;
    private Restaurant testRestaurant;
    
    @BeforeEach
    void setUp() {
        // Arrange: Create test data
        testRestaurantDTO = RestaurantDTO.builder()
                .name("Test Restaurant")
                .address("123 Test St")
                .city("Mumbai")  // Required for DynamoDB partitioning
                .contactEmail("test@restaurant.com")
                .contactNumber("1234567890")
                .cuisineType("Italian")
                .build();
        
        testRestaurant = Restaurant.builder()
                .id("test-id-123")
                .name("Test Restaurant")
                .address("123 Test St")
                .city("Mumbai")  // Required field
                .contactEmail("test@restaurant.com")
                .contactNumber("1234567890")
                .cuisineType("Italian")
                .version(1L)
                .isDeleted(false)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }
    
    /**
     * Test: Create restaurant synchronously - Happy Path
     * Verifies that restaurant is saved to MySQL and Kafka event is published
     */
    @Test
    void createRestaurantSync_Success() {
        // Arrange
        when(restaurantRepository.existsByContactEmailAndIsDeletedFalse(anyString())).thenReturn(false);
        when(restaurantRepository.save(any(Restaurant.class))).thenReturn(testRestaurant);
        lenient().doNothing().when(kafkaProducerService).publishEvent(nullable(String.class), anyString(), any());
        
        // Act
        RestaurantDTO result = restaurantService.createRestaurantSync(testRestaurantDTO);
        
        // Assert
        assertNotNull(result);
        assertEquals("Test Restaurant", result.getName());
        assertEquals("Mumbai", result.getCity());
        assertEquals("test@restaurant.com", result.getContactEmail());
        
        // Verify interactions
        verify(restaurantRepository, times(1)).existsByContactEmailAndIsDeletedFalse(anyString());
        verify(restaurantRepository, times(1)).save(any(Restaurant.class));
        verify(kafkaProducerService, times(1)).publishEvent(nullable(String.class), anyString(), any());
    }
    
    /**
     * Test: Create restaurant with duplicate email
     * Should throw exception
     */
    @Test
    void createRestaurantSync_DuplicateEmail_ThrowsException() {
        // Arrange
        when(restaurantRepository.existsByContactEmailAndIsDeletedFalse(anyString())).thenReturn(true);
        
        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            restaurantService.createRestaurantSync(testRestaurantDTO);
        });
        
        assertTrue(exception.getMessage().contains("email already exists"));
        
        // Verify repository was checked but save was not called
        verify(restaurantRepository, times(1)).existsByContactEmailAndIsDeletedFalse(anyString());
        verify(restaurantRepository, never()).save(any());
    }
    
    /**
     * Test: Create restaurant asynchronously
     * Should publish event to Kafka without saving immediately
     */
    @Test
    void createRestaurantAsync_Success() {
        // Arrange
        lenient().doNothing().when(kafkaProducerService).publishEvent(nullable(String.class), anyString(), any());
        
        // Act
        RestaurantDTO result = restaurantService.createRestaurantAsync(testRestaurantDTO);
        
        // Assert
        assertNotNull(result);
        assertNotNull(result.getId()); // ID should be generated
        assertEquals("Test Restaurant", result.getName());
        
        // Verify Kafka event was published
        verify(kafkaProducerService, times(1)).publishEvent(nullable(String.class), anyString(), any());
        
        // Verify database operations were NOT called (async processing)
        verify(restaurantRepository, never()).save(any());
    }
    
    /**
     * Test: Get restaurant by ID - Cache miss, found in repository
     * Note: Redis cache is checked via @Cacheable annotation, then MySQL
     */
    @Test
    void getRestaurantById_Found() {
        // Arrange
        when(restaurantRepository.findById(anyString())).thenReturn(Optional.of(testRestaurant));
        
        // Act
        RestaurantDTO result = restaurantService.getRestaurantById("test-id-123");
        
        // Assert
        assertNotNull(result);
        assertEquals("test-id-123", result.getId());
        assertEquals("Test Restaurant", result.getName());
        assertEquals("Mumbai", result.getCity());
        
        // Verify MySQL was queried
        verify(restaurantRepository, times(1)).findById(anyString());
    }
    
    /**
     * Test: Get restaurant by ID - Not found
     */
    @Test
    void getRestaurantById_NotFound_ThrowsException() {
        // Arrange
        when(restaurantRepository.findById(anyString())).thenReturn(Optional.empty());
        
        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            restaurantService.getRestaurantById("non-existent-id");
        });
        
        assertTrue(exception.getMessage().contains("not found"));
        verify(restaurantRepository, times(1)).findById(anyString());
    }
    
    /**
     * Test: Update restaurant - Success
     * Note: Update publishes to Kafka, cache consumers handle DynamoDB sync
     */
    @Test
    void updateRestaurant_Success() {
        // Arrange
        RestaurantDTO updateDTO = RestaurantDTO.builder()
                .name("Updated Restaurant")
                .address("456 New St")
                .city("Mumbai")
                .contactEmail("updated@restaurant.com")
                .contactNumber("9876543210")
                .cuisineType("French")
                .build();
        
        when(restaurantRepository.findById(anyString())).thenReturn(Optional.of(testRestaurant));
        when(restaurantRepository.save(any(Restaurant.class))).thenReturn(testRestaurant);
        lenient().doNothing().when(kafkaProducerService).publishEvent(nullable(String.class), anyString(), any());
        
        // Act
        RestaurantDTO result = restaurantService.updateRestaurant("test-id-123", updateDTO);
        
        // Assert
        assertNotNull(result);
        verify(restaurantRepository, times(1)).findById(anyString());
        verify(restaurantRepository, times(1)).save(any(Restaurant.class));
        verify(kafkaProducerService, times(1)).publishEvent(nullable(String.class), anyString(), any());
    }
    
    /**
     * Test: Delete restaurant - Success (Soft Delete)
     * Note: Soft delete marks isDeleted=true and publishes Kafka event
     */
    @Test
    void deleteRestaurant_Success() {
        // Arrange
        when(restaurantRepository.findByIdIncludingDeleted(anyString())).thenReturn(Optional.of(testRestaurant));
        when(restaurantRepository.save(any(Restaurant.class))).thenReturn(testRestaurant);
        lenient().doNothing().when(kafkaProducerService).publishEvent(nullable(String.class), anyString(), any());
        
        // Act
        restaurantService.deleteRestaurant("test-id-123");
        
        // Assert
        verify(restaurantRepository, times(1)).findByIdIncludingDeleted(anyString());
        verify(restaurantRepository, times(1)).save(any(Restaurant.class));
        verify(kafkaProducerService, times(1)).publishEvent(nullable(String.class), anyString(), any());
    }
    
    /**
     * Test: Delete non-existent restaurant
     */
    @Test
    void deleteRestaurant_NotFound_ThrowsException() {
        // Arrange
        when(restaurantRepository.findByIdIncludingDeleted(anyString())).thenReturn(Optional.empty());
        
        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            restaurantService.deleteRestaurant("non-existent-id");
        });
        
        assertTrue(exception.getMessage().contains("not found"));
        verify(restaurantRepository, times(1)).findByIdIncludingDeleted(anyString());
        verify(restaurantRepository, never()).save(any());
    }
}
