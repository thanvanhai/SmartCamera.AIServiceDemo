import asyncio
import json
import logging
import time
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
import hashlib
import cv2
import numpy as np
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

# --- Cấu hình logging cơ bản ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KafkaPublisher:
    """
    Một class publisher hiệu suất cao cho Kafka, được thiết kế để xử lý frame từ camera.
    - Lưu frame ảnh ra đĩa local (Claim-Check pattern).
    - Gửi metadata của frame (đường dẫn, kích thước) tới Kafka.
    - Tự động dọn dẹp file cũ dựa trên tuổi và tổng dung lượng lưu trữ.
    - Được tối ưu cho môi trường production với các tính năng: idempotence, acks='all',
      giới hạn I/O đồng thời và xử lý lỗi retry.
    """
    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str = "smartcamera",
        num_partitions: int = 10,
        replication_factor: int = 1,
        frame_storage_dir: str = "./frames",
        max_storage_days: int = 7,
        max_storage_gb: float = 20.0,
        cleanup_interval: int = 3600, # 1 giờ
        use_date_folders: bool = True,
        max_retry: int = 3,
        max_frame_concurrent: int = 16, # Giới hạn số frame ghi xuống đĩa đồng thời
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.frame_storage_dir = Path(frame_storage_dir)
        self.max_storage_days = max_storage_days
        self.max_storage_gb = max_storage_gb
        self.cleanup_interval = cleanup_interval
        self.use_date_folders = use_date_folders
        self.max_retry = max_retry

        # Đảm bảo thư mục lưu trữ tồn tại
        self.frame_storage_dir.mkdir(parents=True, exist_ok=True)

        self.producer: Optional[AIOKafkaProducer] = None
        self.admin_client: Optional[AIOKafkaAdminClient] = None
        self.topic_created = False

        # Thread pool để chạy các tác vụ blocking I/O (ghi file) mà không làm nghẽn event loop
        self.executor = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 4) * 2))
        logger.info(f"ThreadPoolExecutor được khởi tạo với {self.executor._max_workers} workers.")

        # Semaphore giới hạn số lượng tác vụ ghi file đồng thời để tránh quá tải đĩa
        self.frame_semaphore = asyncio.Semaphore(max_frame_concurrent)

        self.running = False
        self.cleanup_task: Optional[asyncio.Task] = None

        # Sử dụng asyncio.Lock để đảm bảo an toàn luồng khi cập nhật stats
        self.stats_lock = asyncio.Lock()
        self.stats = defaultdict(lambda: {"frames": 0, "events": 0})

        # Cache để lưu kết quả phân vùng cho từng camera_id, tránh tính toán lại hash
        self.partition_cache = {}

    # ------------------- Các hàm tiện ích (Utilities) -------------------

    def _get_storage_path(self, camera_id: str, timestamp: int) -> Path:
        """Tạo và trả về đường dẫn lưu trữ frame dựa trên cấu hình."""
        if self.use_date_folders:
            # Sắp xếp frame vào các thư mục YYYY/MM/DD để dễ quản lý
            date_str = datetime.fromtimestamp(timestamp / 1000).strftime("%Y/%m/%d")
            folder = self.frame_storage_dir / date_str / camera_id
        else:
            folder = self.frame_storage_dir / camera_id
        folder.mkdir(parents=True, exist_ok=True)
        return folder

    def _save_frame_sync(self, frame: np.ndarray, path: Path) -> Optional[int]:
        """
        Hàm đồng bộ (blocking) để ghi frame ra file.
        Trả về kích thước file nếu thành công, None nếu thất bại.
        """
        try:
            # Ghi ảnh JPEG với chất lượng 70 và tối ưu hóa để giảm dung lượng
            cv2.imwrite(str(path), frame, [cv2.IMWRITE_JPEG_QUALITY, 70, cv2.IMWRITE_JPEG_OPTIMIZE, 1])
            # Trả về kích thước file ngay sau khi ghi thành công
            return path.stat().st_size
        except Exception as e:
            logger.error(f"Thất bại khi lưu frame {path}: {e}")
            return None

    async def _save_frame_async(self, frame: np.ndarray, path: Path) -> Optional[int]:
        """
        Hàm bất đồng bộ (non-blocking) để ghi frame.
        Sử dụng semaphore để giới hạn số lượng ghi đồng thời và executor để chạy hàm sync.
        """
        async with self.frame_semaphore:
            loop = asyncio.get_event_loop()
            # Chạy hàm blocking _save_frame_sync trong một thread riêng
            file_size = await loop.run_in_executor(self.executor, self._save_frame_sync, frame, path)
            return file_size

    def _camera_partitioner(self, key_bytes, all_partitions, available_partitions):
        """
        Partitioner tùy chỉnh: đảm bảo tất cả message từ cùng một camera_id
        sẽ đi vào cùng một partition. Điều này giữ đúng thứ tự message cho mỗi camera.
        """
        if key_bytes is None:
            return available_partitions[0] if available_partitions else all_partitions[0]

        key_str = key_bytes.decode('utf-8')
        # Kiểm tra cache trước khi tính toán hash
        if key_str not in self.partition_cache:
            h = int(hashlib.md5(key_bytes).hexdigest(), 16)
            partition = all_partitions[h % len(all_partitions)]
            self.partition_cache[key_str] = partition

        return self.partition_cache[key_str]

    # ------------------- Quản lý kết nối Kafka -------------------

    async def connect(self):
        """Khởi tạo kết nối tới Kafka, tạo topic và bắt đầu các tác vụ nền."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            compression_type="gzip", # Nén message để tiết kiệm băng thông và dung lượng
            linger_ms=50,            # Chờ 50ms để gom message thành batch
            max_batch_size=32768,    # Đúng tham số
            partitioner=self._camera_partitioner,
            # Cài đặt quan trọng cho production:
            acks='all',              # Đảm bảo message đã được replicate đầy đủ
            enable_idempotence=True, # Đảm bảo message được gửi đúng một lần (exactly-once)
        )
        await self.producer.start()

        self.admin_client = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await self.admin_client.start()

        await self.ensure_topic_exists()
        self.running = True
        self.cleanup_task = asyncio.create_task(self._periodic_cleanup())
        # Chạy dọn dẹp một lần ngay khi khởi động
        await self._cleanup_old_files()

    async def ensure_topic_exists(self):
        """Đảm bảo topic Kafka tồn tại với đúng cấu hình."""
        if self.topic_created:
            return
        try:
            topic = NewTopic(
                name=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.replication_factor,
                topic_configs={
                    "cleanup.policy": "delete",
                    "retention.ms": str(self.max_storage_days * 24 * 3600 * 1000),
                    "compression.type": "gzip",
                }
            )
            await self.admin_client.create_topics([topic])
            logger.info(f"✅ Đã tạo Kafka topic '{self.topic_name}'")
        except TopicAlreadyExistsError:
            logger.info(f"ℹ️ Topic '{self.topic_name}' đã tồn tại.")
        except Exception as e:
            logger.warning(f"⚠️ Không thể tạo topic: {e}")
        self.topic_created = True

    async def _send_with_retry(self, message: dict, key: str) -> bool:
        """Gửi message tới Kafka với cơ chế thử lại (retry)."""
        for attempt in range(self.max_retry):
            try:
                # send_and_wait sẽ chờ xác nhận từ broker
                await self.producer.send_and_wait(self.topic_name, value=message, key=key.encode('utf-8'))
                return True
            except Exception as e:
                logger.warning(f"Gửi message lần {attempt + 1} thất bại: {e}")
                await asyncio.sleep(0.5 * (attempt + 1)) # Chờ lâu hơn sau mỗi lần thất bại
        logger.error(f"Gửi message cho key '{key}' thất bại sau {self.max_retry} lần thử.")
        return False

    # ------------------- Các hàm xuất bản (Publishing) -------------------

    async def publish_frame(self, camera_id: str, frame: np.ndarray, metadata: Optional[dict] = None) -> bool:
        """
        Lưu frame ra đĩa và publish metadata của nó lên Kafka.
        Đây là việc triển khai Claim-Check Pattern.
        """
        timestamp = int(time.time() * 1000)
        folder = self._get_storage_path(camera_id, timestamp)
        filename = f"{timestamp}.jpg"
        path = folder / filename

        # Bước 1: Lưu file (payload lớn) ra hệ thống lưu trữ ngoài
        file_size = await self._save_frame_async(frame, path)
        if file_size is None:
            logger.error(f"Không thể lưu frame cho camera {camera_id}")
            return False

        # Bước 2: Tạo message (claim check) chứa thông tin tham chiếu tới payload
        relative_path = path.relative_to(self.frame_storage_dir)
        message = {
            "camera_id": camera_id,
            "timestamp": timestamp,
            "frame_path": str(relative_path),
            "frame_size": file_size,
            "format": "jpeg",
            "message_type": "frame",
        }
        if metadata:
            message["metadata"] = metadata

        # Bước 3: Gửi "claim check" (message nhỏ) tới Kafka
        sent = await self._send_with_retry(message, camera_id)
        if sent:
            async with self.stats_lock: # Bảo vệ việc ghi vào self.stats
                self.stats[camera_id]["frames"] += 1
        return sent

    async def publish_event(self, camera_id: str, event_type: str, event_data: dict) -> bool:
        """Publish một message sự kiện chung lên Kafka."""
        timestamp = int(time.time() * 1000)
        message = {
            "camera_id": camera_id,
            "timestamp": timestamp,
            "message_type": event_type,
            "data": event_data
        }
        sent = await self._send_with_retry(message, camera_id)
        if sent:
            async with self.stats_lock: # Bảo vệ việc ghi vào self.stats
                self.stats[camera_id]["events"] += 1
        return sent

    async def publish_status(self, camera_id: str, status: str, details: dict = None) -> bool:
        """Publish một message trạng thái đặc biệt, bao gồm cả thống kê hiện tại."""
        async with self.stats_lock:
            # Tạo bản sao của stats để tránh thay đổi ngoài ý muốn
            current_stats = self.stats.get(camera_id, {}).copy()

        status_data = {
            "status": status,
            "details": details or {},
            "stats": current_stats
        }
        return await self.publish_event(camera_id, "status", status_data)

    # ------------------- Logic Dọn Dẹp (Cleanup) -------------------

    async def _cleanup_old_files(self):
        """
        Dọn dẹp các file frame cũ dựa trên tuổi và tổng dung lượng.
        Hàm này được tối ưu để chỉ duyệt hệ thống file một lần.
        """
        logger.info("🧹 Bắt đầu quá trình dọn dẹp file...")
        try:
            cutoff_time = time.time() - self.max_storage_days * 24 * 3600
            max_bytes = self.max_storage_gb * 1024 * 1024 * 1024

            files_to_check = []
            total_size_bytes = 0
            deleted_by_age = 0
            
            # Bước 1: Duyệt cây thư mục MỘT LẦN DUY NHẤT
            for f in self.frame_storage_dir.rglob("*.jpg"):
                if not f.is_file():
                    continue
                try:
                    stat = f.stat()
                    # Xóa ngay những file quá tuổi
                    if stat.st_mtime < cutoff_time:
                        f.unlink()
                        deleted_by_age += 1
                    else:
                        # Thu thập thông tin các file còn lại để kiểm tra dung lượng
                        files_to_check.append({'path': f, 'mtime': stat.st_mtime, 'size': stat.st_size})
                        total_size_bytes += stat.st_size
                except FileNotFoundError:
                    continue # File đã bị xóa bởi một tiến trình khác
                except Exception as e:
                    logger.warning(f"Không thể xử lý file {f}: {e}")

            if deleted_by_age > 0:
                logger.info(f"   - Đã xóa {deleted_by_age} file quá {self.max_storage_days} ngày tuổi.")

            # Bước 2: Nếu dung lượng vẫn vượt mức, xóa các file cũ nhất
            deleted_by_size = 0
            if total_size_bytes > max_bytes:
                logger.info(f"   - Dung lượng vượt mức cho phép ({total_size_bytes / (1024**3):.2f}GB > {self.max_storage_gb:.2f}GB). Bắt đầu xóa file cũ nhất.")
                # Sắp xếp các file còn lại theo thời gian sửa đổi (cũ nhất trước)
                files_to_check.sort(key=lambda x: x['mtime'])
                
                # Mục tiêu dung lượng là 85% mức tối đa để có khoảng trống
                target_size_bytes = max_bytes * 0.85
                
                for file_info in files_to_check:
                    if total_size_bytes <= target_size_bytes:
                        break
                    try:
                        file_info['path'].unlink()
                        total_size_bytes -= file_info['size']
                        deleted_by_size += 1
                    except FileNotFoundError:
                        continue
                    except Exception as e:
                        logger.warning(f"Không thể xóa file {file_info['path']}: {e}")
            
            total_deleted = deleted_by_age + deleted_by_size
            final_storage_gb = total_size_bytes / (1024**3)
            logger.info(f"✅ Dọn dẹp hoàn tất. Tổng cộng đã xóa {total_deleted} file. Dung lượng hiện tại: {final_storage_gb:.2f}GB")

        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng trong quá trình dọn dẹp: {e}", exc_info=True)

    async def _periodic_cleanup(self):
        """Chạy tác vụ dọn dẹp theo định kỳ."""
        while self.running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_old_files()
            except asyncio.CancelledError:
                logger.info("Tác vụ dọn dẹp định kỳ đã bị hủy.")
                break
            except Exception as e:
                logger.error(f"Lỗi trong vòng lặp dọn dẹp: {e}", exc_info=True)

    async def close(self):
        """Dọn dẹp tài nguyên và đóng kết nối một cách an toàn."""
        logger.info("Bắt đầu tắt Kafka publisher...")
        self.running = False
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass # Đây là điều mong đợi

        if self.producer:
            await self.producer.flush() # Đảm bảo mọi message trong buffer được gửi đi
            await self.producer.stop()
            logger.info("Kafka producer đã dừng.")
        
        if self.admin_client:
            await self.admin_client.close()
            logger.info("Kafka admin client đã đóng.")
        
        if self.executor:
            self.executor.shutdown(wait=True)
            logger.info("Thread pool executor đã dừng.")
