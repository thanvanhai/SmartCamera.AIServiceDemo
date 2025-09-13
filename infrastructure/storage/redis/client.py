import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional, List, Union
from dataclasses import asdict

try:
    import aioredis
    from aioredis.exceptions import RedisError, ConnectionError as RedisConnectionError
except ImportError:
    # Mock for development without redis installed
    aioredis = None
    RedisError = Exception
    RedisConnectionError = Exception


class RedisClient:
    """Asynchronous Redis Client for AI Service"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Connection settings
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 6379)
        self.db = self.config.get('db', 0)
        self.password = self.config.get('password', None)
        self.username = self.config.get('username', None)
        
        # Connection pool settings
        self.max_connections = self.config.get('max_connections', 10)
        self.socket_timeout = self.config.get('socket_timeout', 30)
        self.socket_connect_timeout = self.config.get('socket_connect_timeout', 30)
        self.socket_keepalive = self.config.get('socket_keepalive', True)
        self.socket_keepalive_options = self.config.get('socket_keepalive_options', {})
        
        # Performance settings
        self.retry_on_timeout = self.config.get('retry_on_timeout', True)
        self.health_check_interval = self.config.get('health_check_interval', 30)
        self.decode_responses = self.config.get('decode_responses', True)
        
        # Client state
        self.redis = None
        self.is_connected = False
        self.connection_pool = None
        
        # Monitoring
        self.operations_count = 0
        self.errors_count = 0
        self.last_error_time = None
        self.start_time = None
        self.last_health_check = 0
        
        # Key prefixes for organization
        self.key_prefix = self.config.get('key_prefix', 'smartcamera:ai:')
    
    async def connect(self):
        """Connect to Redis server"""
        if self.is_connected:
            self.logger.warning("Redis client already connected")
            return
            
        if not aioredis:
            self.logger.error("aioredis not installed. Run: pip install aioredis")
            raise ImportError("aioredis package required")
        
        try:
            self.logger.info(f"🔗 Connecting to Redis: {self.host}:{self.port}/{self.db}")
            
            # Create connection URL
            if self.username and self.password:
                url = f"redis://{self.username}:{self.password}@{self.host}:{self.port}/{self.db}"
            elif self.password:
                url = f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
            else:
                url = f"redis://{self.host}:{self.port}/{self.db}"
            
            # Create Redis client with connection pool
            self.redis = aioredis.from_url(
                url,
                max_connections=self.max_connections,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                socket_keepalive=self.socket_keepalive,
                socket_keepalive_options=self.socket_keepalive_options,
                retry_on_timeout=self.retry_on_timeout,
                decode_responses=self.decode_responses,
                encoding='utf-8'
            )
            
            # Test connection
            await self.redis.ping()
            
            self.is_connected = True
            self.start_time = time.time()
            
            self.logger.info("✅ Redis client connected successfully")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Redis: {e}")
            self.is_connected = False
            raise
    
    async def disconnect(self):
        """Disconnect from Redis server"""
        if not self.is_connected or not self.redis:
            return
            
        try:
            self.logger.info("🔌 Disconnecting Redis client...")
            
            # Close connection pool
            await self.redis.close()
            
            self.is_connected = False
            self.redis = None
            
            self.logger.info("✅ Redis client disconnected")
            
        except Exception as e:
            self.logger.error(f"❌ Error disconnecting Redis client: {e}")
    
    def _get_key(self, key: str) -> str:
        """Get full key with prefix"""
        if key.startswith(self.key_prefix):
            return key
        return f"{self.key_prefix}{key}"
    
    async def set(self, key: str, value: Union[Dict, List, str, int, float], 
                  ttl: Optional[int] = None, nx: bool = False, xx: bool = False) -> bool:
        """
        Set a key-value pair in Redis
        
        Args:
            key: Redis key
            value: Value to store (will be JSON serialized if not string)
            ttl: Time to live in seconds
            nx: Only set if key doesn't exist
            xx: Only set if key exists
            
        Returns:
            True if operation successful
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected. Call connect() first.")
        
        try:
            full_key = self._get_key(key)
            
            # Serialize value
            if isinstance(value, (dict, list)):
                # Handle dataclass objects
                if hasattr(value, '__dataclass_fields__'):
                    value = asdict(value)
                serialized_value = json.dumps(value, ensure_ascii=False, default=str)
            else:
                serialized_value = str(value)
            
            # Set value with options
            result = await self.redis.set(
                full_key, 
                serialized_value,
                ex=ttl,
                nx=nx,
                xx=xx
            )
            
            self.operations_count += 1
            
            self.logger.debug(f"✅ SET {full_key} (TTL: {ttl}s)")
            return bool(result)
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error setting key {key}: {e}")
            raise
    
    async def get(self, key: str, default: Any = None) -> Any:
        """
        Get value from Redis
        
        Args:
            key: Redis key
            default: Default value if key doesn't exist
            
        Returns:
            Deserialized value or default
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_key = self._get_key(key)
            value = await self.redis.get(full_key)
            
            self.operations_count += 1
            
            if value is None:
                return default
            
            # Try to deserialize as JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                # Return as string if not valid JSON
                return value
                
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error getting key {key}: {e}")
            return default
    
    async def hset(self, name: str, mapping: Dict[str, Any]) -> int:
        """
        Set multiple hash fields
        
        Args:
            name: Hash name
            mapping: Dictionary of field-value pairs
            
        Returns:
            Number of fields added
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_name = self._get_key(name)
            
            # Serialize values
            serialized_mapping = {}
            for field, value in mapping.items():
                if isinstance(value, (dict, list)):
                    if hasattr(value, '__dataclass_fields__'):
                        value = asdict(value)
                    serialized_mapping[field] = json.dumps(value, ensure_ascii=False, default=str)
                else:
                    serialized_mapping[field] = str(value)
            
            result = await self.redis.hset(full_name, mapping=serialized_mapping)
            
            self.operations_count += 1
            self.logger.debug(f"✅ HSET {full_name} ({len(mapping)} fields)")
            
            return result
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error setting hash {name}: {e}")
            raise
    
    async def hget(self, name: str, key: str, default: Any = None) -> Any:
        """
        Get hash field value
        
        Args:
            name: Hash name
            key: Field name
            default: Default value if field doesn't exist
            
        Returns:
            Deserialized field value or default
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_name = self._get_key(name)
            value = await self.redis.hget(full_name, key)
            
            self.operations_count += 1
            
            if value is None:
                return default
            
            # Try to deserialize as JSON
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
                
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error getting hash field {name}.{key}: {e}")
            return default
    
    async def hgetall(self, name: str) -> Dict[str, Any]:
        """
        Get all hash fields and values
        
        Args:
            name: Hash name
            
        Returns:
            Dictionary of all field-value pairs
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_name = self._get_key(name)
            hash_data = await self.redis.hgetall(full_name)
            
            self.operations_count += 1
            
            # Deserialize values
            result = {}
            for field, value in hash_data.items():
                try:
                    result[field] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    result[field] = value
            
            return result
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error getting hash {name}: {e}")
            return {}
    
    async def delete(self, *keys: str) -> int:
        """
        Delete one or more keys
        
        Args:
            keys: Keys to delete
            
        Returns:
            Number of keys deleted
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_keys = [self._get_key(key) for key in keys]
            result = await self.redis.delete(*full_keys)
            
            self.operations_count += 1
            self.logger.debug(f"✅ DELETE {len(keys)} keys")
            
            return result
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error deleting keys {keys}: {e}")
            raise
    
    async def exists(self, *keys: str) -> int:
        """
        Check if keys exist
        
        Args:
            keys: Keys to check
            
        Returns:
            Number of existing keys
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_keys = [self._get_key(key) for key in keys]
            result = await self.redis.exists(*full_keys)
            
            self.operations_count += 1
            return result
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error checking existence of keys {keys}: {e}")
            raise
    
    async def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for key
        
        Args:
            key: Redis key
            ttl: Time to live in seconds
            
        Returns:
            True if timeout was set
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_key = self._get_key(key)
            result = await self.redis.expire(full_key, ttl)
            
            self.operations_count += 1
            return bool(result)
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error setting expiration for key {key}: {e}")
            raise
    
    async def ttl(self, key: str) -> int:
        """
        Get remaining time to live for key
        
        Args:
            key: Redis key
            
        Returns:
            TTL in seconds (-1 if no expiration, -2 if key doesn't exist)
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_key = self._get_key(key)
            result = await self.redis.ttl(full_key)
            
            self.operations_count += 1
            return result
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error getting TTL for key {key}: {e}")
            raise
    
    async def keys(self, pattern: str = '*') -> List[str]:
        """
        Get keys matching pattern
        
        Args:
            pattern: Key pattern (e.g., 'user:*')
            
        Returns:
            List of matching keys (without prefix)
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            full_pattern = self._get_key(pattern)
            keys = await self.redis.keys(full_pattern)
            
            self.operations_count += 1
            
            # Remove prefix from returned keys
            return [key.replace(self.key_prefix, '', 1) for key in keys]
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error getting keys with pattern {pattern}: {e}")
            raise
    
    async def flushdb(self) -> bool:
        """
        Clear current database
        
        Returns:
            True if successful
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            result = await self.redis.flushdb()
            
            self.operations_count += 1
            self.logger.warning("🗑️ Database flushed")
            
            return bool(result)
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error flushing database: {e}")
            raise
    
    async def info(self, section: str = 'all') -> Dict[str, Any]:
        """
        Get Redis server information
        
        Args:
            section: Info section to retrieve
            
        Returns:
            Server information dictionary
        """
        if not self.is_connected or not self.redis:
            raise RuntimeError("Redis not connected")
        
        try:
            info = await self.redis.info(section)
            self.operations_count += 1
            return info
            
        except Exception as e:
            self.errors_count += 1
            self.last_error_time = time.time()
            self.logger.error(f"❌ Error getting Redis info: {e}")
            raise
    
    async def health_check(self) -> bool:
        """Check if Redis connection is healthy"""
        try:
            if not self.is_connected or not self.redis:
                return False
            
            # Only do actual ping if enough time has passed
            now = time.time()
            if now - self.last_health_check < self.health_check_interval:
                return True
            
            # Ping Redis server
            await self.redis.ping()
            self.last_health_check = now
            
            return True
            
        except Exception as e:
            self.logger.debug(f"Redis health check failed: {e}")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get Redis client performance metrics"""
        uptime = time.time() - self.start_time if self.start_time else 0
        
        return {
            'is_connected': self.is_connected,
            'operations_count': self.operations_count,
            'errors_count': self.errors_count,
            'last_error_time': self.last_error_time,
            'uptime_seconds': uptime,
            'operations_per_second': self.operations_count / uptime if uptime > 0 else 0,
            'error_rate': self.errors_count / self.operations_count if self.operations_count > 0 else 0,
            'config': {
                'host': self.host,
                'port': self.port,
                'db': self.db,
                'key_prefix': self.key_prefix,
                'max_connections': self.max_connections
            }
        }
    
    # Context manager support
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
    
    def __repr__(self):
        return f"RedisClient(connected={self.is_connected}, {self.host}:{self.port}/{self.db})"


# Example usage and testing
async def test_redis_client():
    """Test function for development"""
    import os
    
    config = {
        'host': os.getenv('REDIS_HOST', 'localhost'),
        'port': int(os.getenv('REDIS_PORT', 6379)),
        'db': int(os.getenv('REDIS_DB', 0)),
        'password': os.getenv('REDIS_PASSWORD'),
        'key_prefix': 'test:smartcamera:',
        'max_connections': 5
    }
    
    # Test with context manager
    async with RedisClient(config) as redis_client:
        
        # Test basic operations
        print("🧪 Testing basic operations...")
        
        # Set/Get string
        await redis_client.set('test_key', 'test_value', ttl=60)
        value = await redis_client.get('test_key')
        print(f"✅ String: {value}")
        
        # Set/Get JSON object
        test_data = {
            'camera_id': 'cam_001',
            'detections': [{'class': 'person', 'confidence': 0.95}],
            'timestamp': time.time()
        }
        
        await redis_client.set('test_json', test_data, ttl=120)
        json_value = await redis_client.get('test_json')
        print(f"✅ JSON: {json_value}")
        
        # Test hash operations
        print("\n🧪 Testing hash operations...")
        
        hash_data = {
            'worker_id': 'worker_001',
            'fps': 15.2,
            'status': 'healthy',
            'metadata': {'gpu': True, 'model': 'yolov8'}
        }
        
        await redis_client.hset('worker_stats', hash_data)
        
        fps = await redis_client.hget('worker_stats', 'fps')
        print(f"✅ Hash field: {fps}")
        
        all_stats = await redis_client.hgetall('worker_stats')
        print(f"✅ All hash: {all_stats}")
        
        # Test key operations
        print("\n🧪 Testing key operations...")
        
        exists_count = await redis_client.exists('test_key', 'test_json')
        print(f"✅ Exists: {exists_count}")
        
        ttl_value = await redis_client.ttl('test_key')
        print(f"✅ TTL: {ttl_value}s")
        
        keys = await redis_client.keys('test_*')
        print(f"✅ Keys: {keys}")
        
        # Test server info
        print("\n🧪 Testing server info...")
        
        health = await redis_client.health_check()
        print(f"✅ Health: {health}")
        
        metrics = redis_client.get_metrics()
        print(f"📊 Metrics: {metrics}")
        
        server_info = await redis_client.info('server')
        print(f"🖥️ Redis version: {server_info.get('redis_version', 'unknown')}")
        
        # Cleanup
        await redis_client.delete('test_key', 'test_json', 'worker_stats')
        print("🗑️ Cleanup completed")


if __name__ == "__main__":
    # Run test
    asyncio.run(test_redis_client())