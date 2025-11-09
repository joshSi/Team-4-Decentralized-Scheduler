"""
Model loader for downloading and loading PyTorch models from GCS with HF fallback.
"""

import os
import shutil
import logging
import torch
import threading
from pathlib import Path
from typing import Optional, Dict, Any, List
from google.cloud import storage

logger = logging.getLogger(__name__)


class ModelLoader:
    def __init__(self, gcs_bucket: str = "remote_model", cache_dir: str = "/tmp/model_cache", device: str = "cpu"):
        self.gcs_bucket = gcs_bucket
        self.cache_dir = Path(cache_dir)
        self.device = device
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        try:
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(gcs_bucket)
            logger.info(f"Connected to GCS bucket: {gcs_bucket}")
        except Exception as e:
            logger.warning(f"Failed to connect to GCS: {e}. Will use local/HF only.")
            self.storage_client = None
            self.bucket = None

        self.models_loaded: Dict[str, Any] = {}
        self._download_progress_lock = threading.Lock()

    def _download_with_timeout(self, blob, local_file: Path, timeout_seconds: int = 300) -> bool:
        try:
            download_success = [False]
            download_error = [None]

            def download_worker():
                try:
                    blob.download_to_filename(str(local_file))
                    download_success[0] = True
                except Exception as e:
                    download_error[0] = e

            t = threading.Thread(target=download_worker, daemon=True)
            t.start()
            t.join(timeout=timeout_seconds)

            if t.is_alive():
                logger.error(f"Download timeout after {timeout_seconds}s for {blob.name}")
                return False

            if not download_success[0]:
                logger.error(f"Download failed for {blob.name}: {download_error[0]}")
                return False

            return True
        except Exception as e:
            logger.error(f"Download timeout wrapper failed for {blob.name}: {e}")
            return False

    def _download_from_gcs(self, model_id: str) -> Path:
        local_model_path = self.cache_dir / model_id
        if local_model_path.exists():
            logger.info(f"Model {model_id} already cached at {local_model_path}")
            return local_model_path

        if not self.bucket:
            raise RuntimeError(f"Cannot download {model_id}: GCS client not initialized")

        gcs_prefix = f"models/facebook/{model_id}/"
        logger.info(f"Downloading model from gs://{self.gcs_bucket}/{gcs_prefix}")

        temp_dir = local_model_path.with_suffix('.tmp')
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            blobs = list(self.bucket.list_blobs(prefix=gcs_prefix))
            if not blobs:
                raise FileNotFoundError(f"No files found for model {model_id} in GCS bucket")

            for i, blob in enumerate(blobs):
                rel = blob.name[len(gcs_prefix):]
                if not rel:
                    continue
                local_file = temp_dir / rel
                local_file.parent.mkdir(parents=True, exist_ok=True)

                # Large files get longer timeout
                timeout = 300 if blob.size and blob.size > 100 * 1024 * 1024 else 30
                if not self._download_with_timeout(blob, local_file, timeout_seconds=timeout):
                    raise RuntimeError(f"Download failed for {blob.name}")

            temp_dir.rename(local_model_path)
            logger.info(f"Successfully downloaded {model_id} to {local_model_path}")
            return local_model_path

        except Exception as e:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise RuntimeError(f"Failed to download model {model_id}: {e}")

    def load_model(self, model_id: str) -> bool:
        if model_id in self.models_loaded:
            logger.info(f"Model {model_id} already loaded")
            return True
        try:
            try:
                local_model_path = self._download_from_gcs(model_id)
            except Exception as gcs_error:
                logger.warning(f"GCS download failed for {model_id}: {gcs_error}")
                logger.info(f"Falling back to HuggingFace for {model_id}")
                local_model_path = f"facebook/{model_id}"

            logger.info(f"Loading model {model_id} from {local_model_path}")
            from transformers import AutoModelForCausalLM, AutoTokenizer
            tokenizer = AutoTokenizer.from_pretrained(str(local_model_path))
            model = AutoModelForCausalLM.from_pretrained(
                str(local_model_path),
                dtype=torch.float16 if self.device.startswith('cuda') else torch.float32,
                device_map=self.device if self.device.startswith('cuda') else None,
                low_cpu_mem_usage=True
            )
            if not self.device.startswith('cuda'):
                model = model.to(self.device)

            self.models_loaded[model_id] = {
                'model': model,
                'tokenizer': tokenizer,
                'path': local_model_path
            }
            logger.info(f"Successfully loaded model {model_id} on device {self.device}")
            return True
        except Exception as e:
            logger.error(f"Failed to load model {model_id}: {e}", exc_info=True)
            return False

    def unload_model(self, model_id: str) -> bool:
        if model_id not in self.models_loaded:
            logger.warning(f"Model {model_id} not loaded, cannot unload")
            return False
        try:
            info = self.models_loaded.pop(model_id)
            del info['model']
            del info['tokenizer']
            if self.device.startswith('cuda'):
                torch.cuda.empty_cache()
            logger.info(f"Successfully unloaded model {model_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to unload model {model_id}: {e}")
            return False

    def get_loaded_models(self) -> List[str]:
        return list(self.models_loaded.keys())

    def is_model_loaded(self, model_id: str) -> bool:
        return model_id in self.models_loaded

    def get_model_memory_usage(self, model_id: str) -> Optional[int]:
        if model_id not in self.models_loaded:
            return None
        try:
            model = self.models_loaded[model_id]['model']
            total_bytes = 0
            for p in model.parameters():
                total_bytes += p.nelement() * p.element_size()
            for b in model.buffers():
                total_bytes += b.nelement() * b.element_size()
            return total_bytes
        except Exception as e:
            logger.error(f"Failed to calc memory usage for {model_id}: {e}")
            return None

    def clear_cache(self, keep_loaded: bool = True):
        if keep_loaded:
            for item in self.cache_dir.iterdir():
                if item.is_dir() and item.name not in self.models_loaded:
                    logger.info(f"Removing cached model: {item.name}")
                    shutil.rmtree(item)
        else:
            logger.info(f"Clearing entire model cache: {self.cache_dir}")
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_available_models(self) -> list[str]:
        if not self.bucket:
            logger.warning("GCS client not initialized, cannot list models")
            return []
        try:
            blobs = self.bucket.list_blobs(delimiter='/')
            list(blobs)
            prefixes = blobs.prefixes if hasattr(blobs, 'prefixes') else []
            return [p.rstrip('/') for p in prefixes]
        except Exception as e:
            logger.error(f"Failed to list models from GCS: {e}")
            return []
