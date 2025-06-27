Day 12: NoSQL Databases - MongoDB for Modern Data Engineering
Learning Objectives

Understanding NoSQL principles and document database design
Hands-on MongoDB implementation with aggregation pipelines
Working with Amazon Products Dataset for varied schema exploration

Project Structure
day12-mongodb-data-engineering/
├── data/
│   ├── raw/                 # Raw Amazon products dataset
│   └── processed/           # Processed and enriched data
├── scripts/                 # Python scripts for data processing
├── notebooks/              # Jupyter notebooks for exploration
├── docker-compose.yml      # MongoDB and Mongo Express setup
└── README.md              # This file
Technologies Used

MongoDB 7.0
Mongo Express (Web UI)
Python with PyMongo
Docker & Docker Compose
Amazon Products Dataset from Kaggle

Key Concepts Learned

NoSQL vs SQL: When to use document databases
Document Design Patterns: Embedding vs Referencing
MongoDB Aggregation Pipeline: Advanced analytics queries
Performance Optimization: Indexing strategies
Production Patterns: Backup, monitoring, and scaling

Setup Instructions

Install Docker Desktop for macOS
Clone this repository
Run docker-compose up -d to start MongoDB
Access Mongo Express at http://localhost:8081
Run Python scripts to import and analyze data

Progress Tracking

 MongoDB Installation and Setup
 Understanding Document Structure
 Data Import and Transformation
 Basic Queries and Aggregation
 Performance Optimization
 Production Best Practices

Next Steps

Day 13: Data Warehousing Concepts
Integration with data engineering stack (Airflow, Spark)
Real-time analytics with Change Streams
