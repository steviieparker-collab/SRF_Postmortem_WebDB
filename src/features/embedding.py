"""CPU-only text embeddings for report search.

Model: ``Xenova/multilingual-e5-small`` (int8 ONNX, 384-dim).  E5 expects
``"query: "`` for questions and ``"passage: "`` for indexed text.  Pooling is
mean-over-tokens with the attention mask, followed by L2 normalisation.

The model is loaded lazily so the MCP server starts without paying for
onnxruntime unless a report search is actually called.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEXT_MODEL_KEY = "text_e5small_v1"
TEXT_MODEL_NAME = "Xenova/multilingual-e5-small"
TEXT_MODEL_VERSION = "1.0.0"
TEXT_DIM = 384
MAX_TOKENS = 512


def default_model_dir() -> Path:
    return Path(
        os.getenv(
            "TEXT_MODEL_DIR",
            str(PROJECT_ROOT / "data" / "models" / "multilingual-e5-small"),
        )
    )


class E5Embedder:
    def __init__(self, model_dir: Optional[Path] = None) -> None:
        import onnxruntime as ort
        from tokenizers import Tokenizer

        model_dir = Path(model_dir) if model_dir else default_model_dir()
        candidates = [
            model_dir / "onnx" / "model_int8.onnx",
            model_dir / "onnx" / "model_quantized.onnx",
            model_dir / "model_int8.onnx",
            model_dir / "model_quantized.onnx",
        ]
        model_file = next((p for p in candidates if p.is_file()), None)
        tokenizer_file = model_dir / "tokenizer.json"
        if model_file is None:
            raise FileNotFoundError(f"no ONNX model in {model_dir}")
        if not tokenizer_file.is_file():
            raise FileNotFoundError(f"no tokenizer.json in {model_dir}")

        self.tokenizer = Tokenizer.from_file(str(tokenizer_file))
        self.tokenizer.enable_truncation(max_length=MAX_TOKENS)
        self.session = ort.InferenceSession(
            str(model_file), providers=["CPUExecutionProvider"]
        )
        self.model_file = str(model_file)

    def encode(self, texts: list[str], prefix: str) -> np.ndarray:
        vectors = np.empty((len(texts), TEXT_DIM), dtype=np.float32)
        for i, text in enumerate(texts):
            enc = self.tokenizer.encode(prefix + text)
            ids = np.array([enc.ids], dtype=np.int64)
            mask = np.array([enc.attention_mask], dtype=np.int64)
            out = self.session.run(
                None,
                {
                    "input_ids": ids,
                    "attention_mask": mask,
                    "token_type_ids": np.zeros_like(ids),
                },
            )[0]
            m = mask[..., None].astype(np.float32)
            pooled = (out * m).sum(1) / np.clip(m.sum(1), 1e-9, None)
            vec = pooled[0]
            norm = float(np.linalg.norm(vec))
            vectors[i] = vec / norm if norm > 0 else vec
        return vectors

    def encode_one(self, text: str, prefix: str) -> np.ndarray:
        return self.encode([text], prefix)[0]
