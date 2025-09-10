# ================================================================================================
# scripts/setup/setup_clickhouse.py - ClickHouse Schema Setup
# ================================================================================================

import asyncio
from clickhouse_driver import Client
import structlog
from typing import List, Dict, Any

from app.settings import settings

logger = structlog.get_logger(__name__)

class ClickHouseSetup:
    """Setup ClickHouse database and tables"""
    
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.client = Client(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password or None
        )
        self.database = database
    
    async def create_database(self) -> None:
        """Create database if not exists"""
        try:
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info("Database created/verified", database=self.database)
        except Exception as e:
            logger.error("Failed to create database", database=self.database, error=str(e))
            raise
    
    async def create_tables(self) -> None:
        """Create all required tables"""
        
        # Detection Results Table
        detection_results_sql = """
        CREATE TABLE IF NOT EXISTS detection_results (
            id UUID DEFAULT generateUUIDv4(),
            camera_id String,
            timestamp DateTime64(3) DEFAULT now64(),
            frame_id String,
            model_name String,
            model_version String,
            
            -- Detection Data
            class_name String,
            confidence Float32,
            bbox_x Float32,
            bbox_y Float32,
            bbox_width Float32,
            bbox_height Float32,
            
            -- Additional Metadata
            processing_time_ms UInt32,
            frame_width UInt16,
            frame_height UInt16,
            
            -- Optional Fields
            track_id Nullable(String),
            attributes Map(String, String),
            
            -- Indexes
            INDEX idx_camera_time (camera_id, timestamp) TYPE minmax GRANULARITY 1,
            INDEX idx_class (class_name) TYPE bloom_filter GRANULARITY 1
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (camera_id, timestamp, id)
        TTL timestamp + INTERVAL 30 DAY
        """
        
        # Camera Analytics Table
        camera_analytics_sql = """
        CREATE TABLE IF NOT EXISTS camera_analytics (
            id UUID DEFAULT generateUUIDv4(),
            camera_id String,
            timestamp DateTime64(3) DEFAULT now64(),
            
            -- Performance Metrics
            frames_processed UInt32,
            processing_fps Float32,
            avg_processing_time_ms Float32,
            queue_size UInt32,
            
            -- Detection Metrics
            total_detections UInt32,
            person_count UInt16,
            vehicle_count UInt16,
            face_count UInt16,
            
            -- System Metrics
            cpu_usage Float32,
            memory_usage_mb UInt32,
            gpu_usage Float32,
            gpu_memory_mb UInt32,
            
            -- Status
            status Enum8('active' = 1, 'inactive' = 2, 'error' = 3),
            error_message Nullable(String)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (camera_id, timestamp)
        TTL timestamp + INTERVAL 7 DAY
        """
        
        # Alert Events Table
        alert_events_sql = """
        CREATE TABLE IF NOT EXISTS alert_events (
            id UUID DEFAULT generateUUIDv4(),
            camera_id String,
            timestamp DateTime64(3) DEFAULT now64(),
            
            -- Alert Data
            alert_type Enum8(
                'person_detected' = 1, 
                'vehicle_detected' = 2, 
                'face_recognized' = 3,
                'motion_detected' = 4,
                'intrusion_detected' = 5,
                'custom_alert' = 6
            ),
            severity Enum8('low' = 1, 'medium' = 2, 'high' = 3, 'critical' = 4),
            
            -- Detection Reference
            detection_id Nullable(UUID),
            description String,
            
            -- Location Data
            zone_name Nullable(String),
            coordinates Array(Float32),
            
            -- Status
            status Enum8('active' = 1, 'acknowledged' = 2, 'resolved' = 3),
            acknowledged_by Nullable(String),
            acknowledged_at Nullable(DateTime64(3)),
            
            -- Metadata
            metadata Map(String, String),
            snapshot_url Nullable(String)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (camera_id, timestamp, severity)
        TTL timestamp + INTERVAL 90 DAY
        """
        
        # Model Performance Table
        model_performance_sql = """
        CREATE TABLE IF NOT EXISTS model_performance (
            id UUID DEFAULT generateUUIDv4(),
            timestamp DateTime64(3) DEFAULT now64(),
            
            -- Model Info
            model_name String,
            model_version String,
            model_type String,
            
            -- Performance Metrics
            inference_time_ms Float32,
            preprocessing_time_ms Float32,
            postprocessing_time_ms Float32,
            total_time_ms Float32,
            
            -- Resource Usage
            cpu_usage Float32,
            memory_usage_mb UInt32,
            gpu_usage Float32,
            gpu_memory_mb UInt32,
            
            -- Batch Info
            batch_size UInt8,
            input_resolution String,
            
            -- Quality Metrics
            confidence_avg Float32,
            detection_count UInt16,
            
            -- Worker Info
            worker_id String,
            device String
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (model_name, timestamp)
        TTL timestamp + INTERVAL 14 DAY
        """
        
        # System Health Table
        system_health_sql = """
        CREATE TABLE IF NOT EXISTS system_health (
            id UUID DEFAULT generateUUIDv4(),
            timestamp DateTime64(3) DEFAULT now64(),
            
            -- Service Info
            service_name String,
            service_version String,
            host_name String,
            
            -- Health Status
            status Enum8('healthy' = 1, 'degraded' = 2, 'unhealthy' = 3),
            uptime_seconds UInt32,
            
            -- System Resources
            cpu_usage Float32,
            memory_usage_mb UInt32,
            disk_usage_gb Float32,
            network_rx_mb Float32,
            network_tx_mb Float32,
            
            -- Service Specific
            active_connections UInt16,
            queue_sizes Map(String, UInt32),
            error_count UInt32,
            
            -- Additional Info
            metadata Map(String, String)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (service_name, timestamp)
        TTL timestamp + INTERVAL 7 DAY
        """
        
        tables = {
            "detection_results": detection_results_sql,
            "camera_analytics": camera_analytics_sql,
            "alert_events": alert_events_sql,
            "model_performance": model_performance_sql,
            "system_health": system_health_sql
        }
        
        for table_name, sql in tables.items():
            try:
                self.client.execute(sql)
                logger.info("Table created/verified", table=table_name)
            except Exception as e:
                logger.error("Failed to create table", table=table_name, error=str(e))
                raise
    
    async def create_materialized_views(self) -> None:
        """Create materialized views for analytics"""
        
        # Hourly Detection Summary
        hourly_summary_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS detection_hourly_summary
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(hour)
        ORDER BY (camera_id, class_name, hour)
        AS SELECT
            camera_id,
            class_name,
            toStartOfHour(timestamp) as hour,
            count() as detection_count,
            avg(confidence) as avg_confidence,
            min(confidence) as min_confidence,
            max(confidence) as max_confidence
        FROM detection_results
        GROUP BY camera_id, class_name, hour
        """
        
        # Daily Camera Stats
        daily_stats_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS camera_daily_stats
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (camera_id, date)
        AS SELECT
            camera_id,
            toDate(timestamp) as date,
            count() as total_detections,
            countIf(class_name = 'person') as person_detections,
            countIf(class_name IN ('car', 'truck', 'bus', 'motorcycle')) as vehicle_detections,
            uniq(frame_id) as frames_processed,
            avg(processing_time_ms) as avg_processing_time
        FROM detection_results
        GROUP BY camera_id, date
        """
        
        views = {
            "detection_hourly_summary": hourly_summary_sql,
            "camera_daily_stats": daily_stats_sql
        }
        
        for view_name, sql in views.items():
            try:
                self.client.execute(sql)
                logger.info("Materialized view created/verified", view=view_name)
            except Exception as e:
                logger.error("Failed to create view", view=view_name, error=str(e))
                raise
    
    def close(self) -> None:
        """Close client connection"""
        self.client.disconnect()

async def setup_clickhouse():
    """Main function to setup ClickHouse"""
    logger.info("Setting up ClickHouse", 
                host=settings.clickhouse_host, 
                port=settings.clickhouse_port,
                database=settings.clickhouse_database)
    
    setup = ClickHouseSetup(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_username,
        password=settings.clickhouse_password
    )
    
    try:
        await setup.create_database()
        await setup.create_tables()
        await setup.create_materialized_views()
        
        logger.info("ClickHouse setup completed successfully")
        
    except Exception as e:
        logger.error("ClickHouse setup failed", error=str(e))
        raise
    finally:
        setup.close()

if __name__ == "__main__":
    asyncio.run(setup_clickhouse())