# ================================================================================================
# setup.py - Package Setup
# ================================================================================================

from setuptools import setup, find_packages

setup(
    name="smartcamera-ai-service",
    version="1.0.0",
    description="SmartCamera AI Service for real-time video analysis",
    author="SmartCamera Team",
    author_email="team@smartcamera.com",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "fastapi>=0.104.1",
        "uvicorn[standard]>=0.24.0",
        "pydantic>=2.5.0",
        "pydantic-settings>=2.1.0",
        "torch>=2.1.0",
        "torchvision>=0.16.0",
        "ultralytics>=8.0.200",
        "opencv-python>=4.8.1.78",
        "numpy>=1.24.3",
        "Pillow>=10.0.1",
        "kafka-python>=2.0.2",
        "pika>=1.3.2",
        "redis>=5.0.1",
        "clickhouse-driver>=0.2.6",
        "minio>=7.2.0",
        "sqlalchemy>=2.0.23",
        "httpx>=0.25.2",
        "aiohttp>=3.9.0",
        "prometheus-client>=0.19.0",
        "structlog>=23.2.0",
        "rich>=13.7.0",
        "python-multipart>=0.0.6",
        "python-jose[cryptography]>=3.3.0",
        "python-dotenv>=1.0.0"
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "black>=23.7.0",
            "flake8>=6.0.0",
            "mypy>=1.5.0"
        ],
        "gpu": [
            "torch[cuda]>=2.1.0"
        ]
    },
    entry_points={
        "console_scripts": [
            "smartcamera-ingest=services.ingest.main:main",
            "smartcamera-worker=services.ai_worker.main:main", 
            "smartcamera-results=services.results.main:main",
            "smartcamera-api=services.api.main:main"
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11"
    ]
)