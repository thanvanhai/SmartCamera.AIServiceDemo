# ================================================================================================
# scripts/setup/download_models.py - AI Models Download
# ================================================================================================

import asyncio
import aiohttp
from pathlib import Path
import hashlib
import structlog
from typing import Dict, List, Tuple, Optional

from shared.config.model_configs import ModelConfigs
from app.settings import settings

logger = structlog.get_logger(__name__)

class ModelDownloader:
    """Download and verify AI models"""
    
    MODEL_URLS = {
        # YOLO Models
        "yolov8n.pt": "https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8n.pt",
        "yolov8s.pt": "https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8s.pt",
        "yolov8m.pt": "https://github.com/ultralytics/assets/releases/download/v0.0.0/yolov8m.pt",
        
        # Face Recognition (Example URLs - replace with actual)
        "facenet_model.pb": "https://example.com/models/facenet_model.pb",
        
        # Person Detection
        "person_detector.tflite": "https://example.com/models/person_detector.tflite",
        
        # Vehicle Detection
        "vehicle_detector.onnx": "https://example.com/models/vehicle_detector.onnx",
    }
    
    MODEL_HASHES = {
        # Add SHA256 hashes for verification
        "yolov8n.pt": "6d7b5a7d98d5f3b2c1a8e9f0d4c6b3a5e7f9d1c3b5a7e9f1d3c5b7a9e1f3d5c7",
        # Add other model hashes...
    }
    
    def __init__(self, models_dir: Path):
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
    
    async def download_file(
        self, 
        session: aiohttp.ClientSession, 
        url: str, 
        filepath: Path,
        expected_hash: Optional[str] = None
    ) -> bool:
        """Download a file with progress tracking"""
        
        try:
            logger.info("Downloading model", url=url, filepath=str(filepath))
            
            async with session.get(url) as response:
                if response.status != 200:
                    logger.error("Download failed", url=url, status=response.status)
                    return False
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                # Create subdirectories if needed
                filepath.parent.mkdir(parents=True, exist_ok=True)
                
                with open(filepath, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            if downloaded % (1024 * 1024) == 0:  # Log every MB
                                logger.info("Download progress", 
                                          file=filepath.name, 
                                          progress=f"{progress:.1f}%",
                                          downloaded_mb=f"{downloaded / 1024 / 1024:.1f}")
            
            # Verify file hash if provided
            if expected_hash:
                if not self.verify_file_hash(filepath, expected_hash):
                    logger.error("Hash verification failed", filepath=str(filepath))
                    filepath.unlink()  # Delete corrupted file
                    return False
            
            logger.info("Download completed", filepath=str(filepath), size_mb=f"{downloaded / 1024 / 1024:.1f}")
            return True
            
        except Exception as e:
            logger.error("Download error", url=url, error=str(e))
            if filepath.exists():
                filepath.unlink()  # Cleanup partial download
            return False
    
    def verify_file_hash(self, filepath: Path, expected_hash: str) -> bool:
        """Verify file SHA256 hash"""
        try:
            sha256_hash = hashlib.sha256()
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            
            actual_hash = sha256_hash.hexdigest()
            return actual_hash == expected_hash
            
        except Exception as e:
            logger.error("Hash verification error", filepath=str(filepath), error=str(e))
            return False
    
    async def download_models(self, force_download: bool = False) -> Dict[str, bool]:
        """Download all required models"""
        
        results = {}
        
        # Get enabled models
        enabled_models = ModelConfigs.get_enabled_models()
        
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3600)  # 1 hour timeout
        ) as session:
            
            for model_name, config in enabled_models.items():
                model_filename = config.path.name
                local_path = self.models_dir / config.path
                
                # Skip if file exists and not forcing download
                if local_path.exists() and not force_download:
                    logger.info("Model already exists", model=model_filename, path=str(local_path))
                    results[model_name] = True
                    continue
                
                # Check if we have download URL
                if model_filename not in self.MODEL_URLS:
                    logger.warning("No download URL for model", model=model_filename)
                    results[model_name] = False
                    continue
                
                url = self.MODEL_URLS[model_filename]
                expected_hash = self.MODEL_HASHES.get(model_filename)
                
                success = await self.download_file(session, url, local_path, expected_hash)
                results[model_name] = success
        
        return results
    
    def list_available_models(self) -> List[Tuple[str, Path, bool]]:
        """List available models and their status"""
        models = []
        enabled_models = ModelConfigs.get_enabled_models()
        
        for model_name, config in enabled_models.items():
            local_path = self.models_dir / config.path
            exists = local_path.exists()
            models.append((model_name, local_path, exists))
        
        return models

async def download_models(force: bool = False):
    """Main function to download models"""
    logger.info("Starting model download", models_dir=settings.ai_models_path, force=force)
    
    downloader = ModelDownloader(settings.ai_models_path)
    
    # List current models
    available_models = downloader.list_available_models()
    logger.info("Available models", count=len(available_models))
    
    for model_name, path, exists in available_models:
        status = "EXISTS" if exists else "MISSING"
        logger.info("Model status", model=model_name, path=str(path), status=status)
    
    # Download models
    results = await downloader.download_models(force_download=force)
    
    # Summary
    successful = sum(1 for success in results.values() if success)
    total = len(results)
    
    logger.info("Download summary", 
                successful=successful, 
                total=total, 
                failed=total - successful)
    
    for model_name, success in results.items():
        status = "SUCCESS" if success else "FAILED"
        logger.info("Model download result", model=model_name, status=status)
    
    if successful == total:
        logger.info("All models downloaded successfully")
        return True
    else:
        logger.error("Some models failed to download")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Download AI models")
    parser.add_argument("--force", action="store_true", help="Force re-download existing models")
    args = parser.parse_args()
    
    success = asyncio.run(download_models(force=args.force))
    exit(0 if success else 1)
