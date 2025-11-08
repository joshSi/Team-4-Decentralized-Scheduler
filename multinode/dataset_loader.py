"""
Dataset loader for GSM8K and ShareGPT datasets.
"""

import json
import random
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')


class DatasetLoader:
    def __init__(self, max_tokens: int = 2048, samples_per_dataset: int = 4000, seed: Optional[int] = 42):
        self.max_tokens = max_tokens
        self.samples_per_dataset = samples_per_dataset
        self.logger = logging.getLogger("DatasetLoader")
        if seed is not None:
            random.seed(seed)
        self.gsm8k_data: List[Dict[str, Any]] = []
        self.sharegpt_data: List[Dict[str, Any]] = []
        self.mixed_workload: List[Dict[str, Any]] = []

    def _estimate_tokens(self, text: str) -> int:
        return len(text) // 4

    def _truncate_text(self, text: str, max_tokens: int) -> str:
        estimated_tokens = self._estimate_tokens(text)
        if estimated_tokens <= max_tokens:
            return text
        max_chars = max_tokens * 4
        return text[:max_chars]

    def load_gsm8k(self, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
        if file_path and Path(file_path).exists():
            self.logger.info(f"Loading GSM8K from {file_path}")
            with open(file_path, 'r') as f:
                data = [json.loads(line) for line in f]
        else:
            self.logger.warning("GSM8K file not found, generating dummy data")
            data = []
            for i in range(10000):
                question = f"Problem {i}: If John has {i} apples and gives {i//2} to Mary, how many does he have left?"
                answer = f"{i - i//2}"
                data.append({"question": question, "answer": answer})

        if len(data) > self.samples_per_dataset:
            data = random.sample(data, self.samples_per_dataset)

        for sample in data:
            sample['question'] = self._truncate_text(sample['question'], self.max_tokens)

        self.gsm8k_data = data
        self.logger.info(f"Loaded {len(self.gsm8k_data)} GSM8K samples")
        return self.gsm8k_data

    def load_sharegpt(self, file_path: Optional[str] = None) -> List[Dict[str, Any]]:
        if file_path and Path(file_path).exists():
            self.logger.info(f"Loading ShareGPT from {file_path}")
            with open(file_path, 'r') as f:
                data = json.load(f)
        else:
            self.logger.warning("ShareGPT file not found, generating dummy data")
            data = []
            for i in range(10000):
                conversations = [
                    {"from": "human", "value": f"Hello, can you help me with task {i}?"},
                    {"from": "gpt", "value": f"Of course! I'd be happy to help you with task {i}."},
                    {"from": "human", "value": f"Great! Here are the details..."},
                ]
                data.append({"id": f"conv_{i}", "conversations": conversations})

        if len(data) > self.samples_per_dataset:
            data = random.sample(data, self.samples_per_dataset)

        for sample in data:
            if 'conversations' in sample:
                full_text = " ".join([turn.get('value', '') for turn in sample['conversations']])
                truncated = self._truncate_text(full_text, self.max_tokens)
                sample['truncated_text'] = truncated

        self.sharegpt_data = data
        self.logger.info(f"Loaded {len(self.sharegpt_data)} ShareGPT samples")
        return self.sharegpt_data

    def create_mixed_workload(self) -> List[Dict[str, Any]]:
        if not self.gsm8k_data:
            self.logger.warning("GSM8K data not loaded, loading with defaults")
            self.load_gsm8k()

        if not self.sharegpt_data:
            self.logger.warning("ShareGPT data not loaded, loading with defaults")
            self.load_sharegpt()

        mixed = []
        for sample in self.gsm8k_data:
            mixed.append({'dataset': 'gsm8k', 'data': sample, 'text': sample['question']})

        for sample in self.sharegpt_data:
            mixed.append({'dataset': 'sharegpt', 'data': sample, 'text': sample.get('truncated_text', '')})

        random.shuffle(mixed)
        self.mixed_workload = mixed
        self.logger.info(f"Created mixed workload with {len(self.mixed_workload)} samples")
        self.logger.info(f"  - GSM8K: {len(self.gsm8k_data)} samples")
        self.logger.info(f"  - ShareGPT: {len(self.sharegpt_data)} samples")
        return self.mixed_workload

    def get_sample(self, index: Optional[int] = None) -> Dict[str, Any]:
        if not self.mixed_workload:
            self.create_mixed_workload()
        if index is None:
            return random.choice(self.mixed_workload)
        return self.mixed_workload[index % len(self.mixed_workload)]

    def get_dataset_stats(self) -> Dict[str, Any]:
        stats = {
            'gsm8k_count': len(self.gsm8k_data),
            'sharegpt_count': len(self.sharegpt_data),
            'mixed_count': len(self.mixed_workload),
            'max_tokens': self.max_tokens,
            'samples_per_dataset': self.samples_per_dataset
        }
        if self.gsm8k_data:
            avg_len = sum(len(s['question']) for s in self.gsm8k_data) / len(self.gsm8k_data)
            stats['gsm8k_avg_length'] = avg_len
        if self.sharegpt_data:
            avg_len = sum(len(s.get('truncated_text', '')) for s in self.sharegpt_data) / len(self.sharegpt_data)
            stats['sharegpt_avg_length'] = avg_len
        return stats


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Dataset Loader Test")
    parser.add_argument("--gsm8k", type=str, help="Path to GSM8K dataset")
    parser.add_argument("--sharegpt", type=str, help="Path to ShareGPT dataset")
    parser.add_argument("--max-tokens", type=int, default=2048, help="Max tokens")
    parser.add_argument("--samples", type=int, default=4000, help="Samples per dataset")
    args = parser.parse_args()

    loader = DatasetLoader(max_tokens=args.max_tokens, samples_per_dataset=args.samples)
    loader.load_gsm8k(args.gsm8k)
    loader.load_sharegpt(args.sharegpt)
    loader.create_mixed_workload()

    stats = loader.get_dataset_stats()
    print("\n=== Dataset Statistics ===")
    for k, v in stats.items():
        print(f"{k}: {v}")

    print("\n=== Sample Data ===")
    for i in range(3):
        sample = loader.get_sample(i)
        print(f"\nSample {i} ({sample['dataset']}):")
        print(f"  Text: {sample['text'][:100]}...")


if __name__ == "__main__":
    main()
