"""End-to-end plugin test against a local moto S3 + fake Gateway.

Setup (managed by run_test.sh):
  * moto_server on :29999 — real S3 API (the "worker")
  * fake_gateway.py on :29998 — returns 307 -> :29999 (the "Gateway")

Test:
  1. Seed one .safetensors file into moto (known tensor contents)
  2. Point runai-model-streamer plugin at the fake Gateway (AWS_ENDPOINT_URL)
  3. Read via alluxio://bucket/model/ URI
  4. Verify:
       - plugin emits "Alluxio ... -> http://localhost:29999" probe log
       - tensors come back bit-identical to what we seeded
"""
from __future__ import annotations

import io
import os
import sys
import boto3
import torch
from botocore.config import Config
from safetensors.torch import save as safetensors_save

BUCKET = "test-bucket"
KEY = "model/weights.safetensors"
WORKER_URL = "http://localhost:29999"
GATEWAY_URL = "http://localhost:29998"


def seed_worker() -> dict[str, torch.Tensor]:
    """Upload one safetensors file to moto with known contents."""
    s3 = boto3.client(
        "s3",
        endpoint_url=WORKER_URL,
        aws_access_key_id="placeholder",
        aws_secret_access_key="placeholder",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    s3.create_bucket(Bucket=BUCKET)

    tensors = {
        "layer0.weight": torch.arange(512, dtype=torch.float32).reshape(16, 32),
        "layer0.bias": torch.linspace(-1.0, 1.0, 32, dtype=torch.float32),
        "layer1.weight": torch.arange(1024, dtype=torch.float32).reshape(32, 32),
    }
    payload = safetensors_save(tensors)
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=payload)
    print(f"[test] seeded {len(payload)} bytes to s3://{BUCKET}/{KEY}")
    return tensors


def read_via_plugin() -> dict[str, torch.Tensor]:
    """Read the seeded file through the runai-model-streamer alluxio:// plugin."""
    # These MUST be set before importing runai_model_streamer so CRT config
    # reads them correctly.
    os.environ.setdefault("AWS_ENDPOINT_URL", GATEWAY_URL)
    os.environ["RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING"] = "0"
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    os.environ["AWS_ACCESS_KEY_ID"] = "placeholder"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "placeholder"

    from runai_model_streamer import SafetensorsStreamer
    from runai_model_streamer.s3_utils.s3_utils import S3Credentials

    creds = S3Credentials(
        access_key_id="placeholder",
        secret_access_key="placeholder",
        endpoint=GATEWAY_URL,
        region_name="us-east-1",
    )

    uri = f"alluxio://{BUCKET}/{KEY}"
    result: dict[str, torch.Tensor] = {}
    with SafetensorsStreamer() as streamer:
        streamer.stream_files([uri], s3_credentials=creds)
        for name, tensor in streamer.get_tensors():
            result[name] = tensor.clone()
    return result


def main():
    expected = seed_worker()
    got = read_via_plugin()

    # --- Validate ---
    if set(got.keys()) != set(expected.keys()):
        print(
            f"[test] FAIL: tensor names mismatch.\n  expected={sorted(expected)}\n  got={sorted(got)}",
            file=sys.stderr,
        )
        sys.exit(1)

    for name, want in expected.items():
        have = got[name]
        if not torch.equal(have.to(want.dtype).reshape(want.shape), want):
            print(f"[test] FAIL: tensor '{name}' contents differ", file=sys.stderr)
            print(f"  expected[0:4]={want.flatten()[:4].tolist()}", file=sys.stderr)
            print(f"  got     [0:4]={have.flatten()[:4].tolist()}", file=sys.stderr)
            sys.exit(1)

    print("[test] OK: all tensors match; alluxio:// plugin works end-to-end")


if __name__ == "__main__":
    main()
