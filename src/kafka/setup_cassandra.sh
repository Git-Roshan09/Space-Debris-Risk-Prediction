#!/bin/bash
# Setup script for Cassandra keyspace and table

echo "Creating Cassandra keyspace and table for risk scores..."

docker exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS space_debris 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE space_debris;

CREATE TABLE IF NOT EXISTS risk_scores (
    satellite_id text,
    epoch timestamp,
    position_x double,
    position_y double,
    position_z double,
    velocity_x double,
    velocity_y double,
    velocity_z double,
    risk_score double,
    risk_level text,
    altitude_km double,
    velocity_kms double,
    processing_time timestamp,
    PRIMARY KEY ((satellite_id), epoch)
) WITH CLUSTERING ORDER BY (epoch DESC);

CREATE INDEX IF NOT EXISTS risk_level_idx ON risk_scores (risk_level);
CREATE INDEX IF NOT EXISTS risk_score_idx ON risk_scores (risk_score);

DESCRIBE TABLE risk_scores;
"

echo "✓ Cassandra setup complete!"
