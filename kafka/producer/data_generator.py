"""
NYC Taxi Trip Data Generator
Generates realistic taxi trip data based on actual patterns
Author: Vikas Pabba
"""

import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List
import json


class NYCTaxiDataGenerator:
    """Generate realistic NYC taxi trip data"""
    
    # NYC borough coordinates (approximate centers)
    LOCATIONS = {
        'Manhattan': {'lat_range': (40.7000, 40.8000), 'lon_range': (-74.0200, -73.9200)},
        'Brooklyn': {'lat_range': (40.5700, 40.7400), 'lon_range': (-74.0400, -73.8300)},
        'Queens': {'lat_range': (40.5400, 40.8000), 'lon_range': (-73.9600, -73.7000)},
        'Bronx': {'lat_range': (40.7850, 40.9150), 'lon_range': (-73.9330, -73.7650)},
        'Staten Island': {'lat_range': (40.4900, 40.6500), 'lon_range': (-74.2600, -74.0500)},
    }
    
    # Popular pickup zones
    POPULAR_ZONES = [
        'JFK Airport', 'LaGuardia Airport', 'Times Square', 'Central Park',
        'Penn Station', 'Grand Central', 'Brooklyn Bridge', 'Wall Street',
        'Greenwich Village', 'Harlem', 'Upper East Side', 'Upper West Side'
    ]
    
    # Payment types
    PAYMENT_TYPES = ['Credit card', 'Cash', 'No charge', 'Dispute']
    
    # Trip types
    TRIP_TYPES = ['Street-hail', 'Dispatch']
    
    # Vendor IDs
    VENDOR_IDS = ['Creative Mobile Technologies', 'VeriFone Inc.']
    
    def __init__(self, seed: int = None):
        """Initialize the data generator"""
        if seed:
            random.seed(seed)
    
    def generate_location(self, borough: str = None) -> Dict[str, float]:
        """Generate a random location within a borough"""
        if borough is None:
            borough = random.choice(list(self.LOCATIONS.keys()))
        
        location = self.LOCATIONS[borough]
        lat = random.uniform(*location['lat_range'])
        lon = random.uniform(*location['lon_range'])
        
        return {
            'latitude': round(lat, 6),
            'longitude': round(lon, 6),
            'borough': borough
        }
    
    def generate_fare(self, distance: float, duration: int) -> Dict[str, float]:
        """Generate fare breakdown based on distance and duration"""
        # NYC taxi fare structure (approximate)
        base_fare = 2.50
        per_mile = 2.50
        per_minute = 0.50
        
        # Calculate fare
        fare_amount = base_fare + (distance * per_mile) + (duration / 60 * per_minute)
        
        # Add randomness (traffic, demand surge, etc.)
        fare_amount *= random.uniform(0.9, 1.3)
        
        # Extras
        surcharge = random.choice([0, 0.50, 1.00])  # Rush hour, overnight
        mta_tax = 0.50
        tolls = random.choice([0, 0, 0, 5.76, 6.12])  # Some trips have tolls
        improvement_surcharge = 0.30
        
        # Tips (only for credit card payments)
        payment_type = random.choice(self.PAYMENT_TYPES)
        tip_amount = 0
        if payment_type == 'Credit card':
            tip_percentage = random.uniform(0.10, 0.25)
            tip_amount = fare_amount * tip_percentage
        
        total_amount = fare_amount + surcharge + mta_tax + tolls + improvement_surcharge + tip_amount
        
        return {
            'fare_amount': round(fare_amount, 2),
            'extra': round(surcharge, 2),
            'mta_tax': round(mta_tax, 2),
            'tip_amount': round(tip_amount, 2),
            'tolls_amount': round(tolls, 2),
            'improvement_surcharge': round(improvement_surcharge, 2),
            'total_amount': round(total_amount, 2),
            'payment_type': payment_type
        }
    
    def calculate_trip_details(self, pickup_location: Dict, dropoff_location: Dict) -> Dict[str, Any]:
        """Calculate trip distance and duration"""
        # Simplified distance calculation (Haversine would be more accurate)
        lat_diff = abs(pickup_location['latitude'] - dropoff_location['latitude'])
        lon_diff = abs(pickup_location['longitude'] - dropoff_location['longitude'])
        
        # Approximate distance in miles
        distance = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 69  # degrees to miles conversion
        distance = max(0.1, distance)  # Minimum distance
        
        # Duration based on distance with traffic variation
        avg_speed_mph = random.uniform(8, 25)  # NYC traffic varies
        duration_minutes = (distance / avg_speed_mph) * 60
        
        return {
            'trip_distance': round(distance, 2),
            'trip_duration_minutes': round(duration_minutes, 1),
            'trip_duration_seconds': int(duration_minutes * 60)
        }
    
    def generate_trip(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single taxi trip record"""
        
        if timestamp is None:
            timestamp = datetime.now()
        
        # Generate pickup and dropoff locations
        pickup_location = self.generate_location()
        dropoff_location = self.generate_location()
        
        # Calculate trip details
        trip_details = self.calculate_trip_details(pickup_location, dropoff_location)
        
        # Calculate pickup and dropoff times
        pickup_datetime = timestamp
        dropoff_datetime = pickup_datetime + timedelta(seconds=trip_details['trip_duration_seconds'])
        
        # Generate fare
        fare_details = self.generate_fare(
            trip_details['trip_distance'],
            trip_details['trip_duration_minutes']
        )
        
        # Passenger count (realistic distribution)
        passenger_count = random.choices(
            [1, 2, 3, 4, 5, 6],
            weights=[60, 25, 10, 3, 1, 1]
        )[0]
        
        # Build trip record
        trip_record = {
            'trip_id': str(uuid.uuid4()),
            'vendor_id': random.choice(self.VENDOR_IDS),
            'pickup_datetime': pickup_datetime.isoformat(),
            'dropoff_datetime': dropoff_datetime.isoformat(),
            'passenger_count': passenger_count,
            'trip_distance': trip_details['trip_distance'],
            'trip_duration_seconds': trip_details['trip_duration_seconds'],
            'pickup_longitude': pickup_location['longitude'],
            'pickup_latitude': pickup_location['latitude'],
            'pickup_borough': pickup_location['borough'],
            'dropoff_longitude': dropoff_location['longitude'],
            'dropoff_latitude': dropoff_location['latitude'],
            'dropoff_borough': dropoff_location['borough'],
            'fare_amount': fare_details['fare_amount'],
            'extra': fare_details['extra'],
            'mta_tax': fare_details['mta_tax'],
            'tip_amount': fare_details['tip_amount'],
            'tolls_amount': fare_details['tolls_amount'],
            'improvement_surcharge': fare_details['improvement_surcharge'],
            'total_amount': fare_details['total_amount'],
            'payment_type': fare_details['payment_type'],
            'trip_type': random.choice(self.TRIP_TYPES),
            'pickup_zone': random.choice(self.POPULAR_ZONES),
            'rate_code': random.choice(['Standard rate', 'JFK', 'Newark', 'Nassau/Westchester', 'Negotiated fare']),
            'store_and_fwd_flag': random.choice(['N', 'N', 'N', 'Y']),  # Mostly N
        }
        
        return trip_record
    
    def generate_batch(self, count: int, start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate a batch of trip records"""
        trips = []
        
        if start_time is None:
            start_time = datetime.now()
        
        for i in range(count):
            # Spread trips over time (simulate realistic arrival pattern)
            timestamp = start_time + timedelta(seconds=random.randint(0, 60))
            trip = self.generate_trip(timestamp)
            trips.append(trip)
        
        return trips


if __name__ == "__main__":
    # Test the generator
    generator = NYCTaxiDataGenerator(seed=42)
    
    print("Generating sample taxi trips...\n")
    
    # Generate 5 sample trips
    trips = generator.generate_batch(5)
    
    for i, trip in enumerate(trips, 1):
        print(f"Trip {i}:")
        print(json.dumps(trip, indent=2))
        print("\n" + "="*80 + "\n")