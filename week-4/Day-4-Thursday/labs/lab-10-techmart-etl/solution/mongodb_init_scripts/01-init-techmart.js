// MongoDB Initialization Script for TechMart ETL Pipeline
// This script runs when the container starts for the first time

// Switch to the techmart_customers database
db = db.getSiblingDB('techmart_customers');

// Create collections with indexes
db.createCollection('customer_profiles');
db.createCollection('customer_preferences');
db.createCollection('customer_segments');

// Create indexes for customer_profiles collection
db.customer_profiles.createIndex({ "customer_id": 1 }, { unique: true });
db.customer_profiles.createIndex({ "email": 1 }, { unique: true });
db.customer_profiles.createIndex({ "registration_date": -1 });
db.customer_profiles.createIndex({ "last_purchase_date": -1 });
db.customer_profiles.createIndex({ "customer_segment": 1 });

// Create indexes for customer_preferences collection
db.customer_preferences.createIndex({ "customer_id": 1 });
db.customer_preferences.createIndex({ "category": 1 });
db.customer_preferences.createIndex({ "preference_score": -1 });

// Create indexes for customer_segments collection
db.customer_segments.createIndex({ "customer_id": 1 });
db.customer_segments.createIndex({ "segment_type": 1 });
db.customer_segments.createIndex({ "segment_value": 1 });

// Insert sample document to verify setup
db.customer_profiles.insertOne({
    "customer_id": 999999,
    "email": "setup.test@techmart.com",
    "first_name": "Setup",
    "last_name": "Test",
    "registration_date": new Date(),
    "customer_segment": "test",
    "status": "setup_verification"
});

print("[OK] TechMart MongoDB initialization complete!");
print("[INFO] Collections created: customer_profiles, customer_preferences, customer_segments");
print("[INFO] Indexes created for optimal query performance");
print("[OK] Ready for ETL pipeline data loading!");
