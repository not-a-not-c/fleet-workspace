#!/usr/bin/env python3
"""
inference-client.py — Fleet inference client for macbook-prime.

Publishes prompts to fleet/inference/request over MQTT and listens
for results on fleet/inference/result. The inference-server.py on the
Jetson forwards prompts to Ollama and returns responses.

Usage:
    python3 inference-client.py "What is the capital of France?"
    python3 inference-client.py --model tinyllama "Explain gravity"
    python3 inference-client.py --timeout 60 --json "Write a haiku"

Can also be imported:
    from inference_client import infer
    result = infer("Hello world", model="tinyllama")
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time

BROKER = os.environ.get("MQTT_BROKER", "Johns-MacBook-Pro-5937.local")
DEFAULT_MODEL = os.environ.get("INFERENCE_MODEL", "tinyllama")
DEFAULT_TIMEOUT = int(os.environ.get("INFERENCE_TIMEOUT", "120"))

REQUEST_TOPIC = "fleet/inference/request"
RESULT_TOPIC = "fleet/inference/result"


def infer(prompt: str, model: str = DEFAULT_MODEL, timeout: int = DEFAULT_TIMEOUT,
          broker: str = BROKER) -> dict:
    """
    Send a prompt to the fleet inference server and wait for a response.

    Returns a dict with keys: response, model, latency_ms, request_id
    Raises TimeoutError if no response within timeout seconds.
    Raises RuntimeError on other errors.
    """
    request_id = f"inf-{int(time.time())}-{os.getpid()}-{id(prompt) % 10000}"
    ts = int(time.time())

    request_msg = json.dumps({
        "id": request_id,
        "from": "macbook-prime",
        "model": model,
        "prompt": prompt,
        "ts": ts,
    })

    result_holder = {"result": None, "error": None}
    stop_event = threading.Event()

    def subscriber():
        """Listen for the matching result message."""
        try:
            proc = subprocess.Popen(
                ["mosquitto_sub", "-h", broker, "-t", RESULT_TOPIC,
                 "-W", str(timeout), "-F", "%p"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            for line in proc.stdout:
                if stop_event.is_set():
                    proc.terminate()
                    return
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                    if msg.get("ref") == request_id:
                        result_holder["result"] = msg
                        stop_event.set()
                        proc.terminate()
                        return
                except json.JSONDecodeError:
                    continue
            proc.wait()
        except Exception as e:
            result_holder["error"] = str(e)
            stop_event.set()

    # Start subscriber BEFORE publishing to avoid race condition
    sub_thread = threading.Thread(target=subscriber, daemon=True)
    sub_thread.start()
    time.sleep(0.3)  # Let subscriber connect

    # Publish the request
    try:
        subprocess.run(
            ["mosquitto_pub", "-h", broker, "-t", REQUEST_TOPIC, "-m", request_msg],
            check=True, capture_output=True, text=True
        )
    except subprocess.CalledProcessError as e:
        stop_event.set()
        raise RuntimeError(f"Failed to publish request: {e.stderr}") from e

    # Wait for result
    stop_event.wait(timeout=timeout)

    if result_holder["error"]:
        raise RuntimeError(f"Subscriber error: {result_holder['error']}")

    if result_holder["result"] is None:
        raise TimeoutError(
            f"No response after {timeout}s. Is inference-server.py running on jetson?"
        )

    r = result_holder["result"]
    return {
        "response": r.get("response", r.get("body", "")),
        "model": r.get("model", model),
        "latency_ms": r.get("latency_ms", -1),
        "request_id": request_id,
        "raw": r,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Send inference requests to the fleet Ollama server"
    )
    parser.add_argument("prompt", help="The prompt to send")
    parser.add_argument("--model", default=DEFAULT_MODEL,
                        help=f"Model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"Timeout in seconds (default: {DEFAULT_TIMEOUT})")
    parser.add_argument("--broker", default=BROKER,
                        help=f"MQTT broker hostname (default: {BROKER})")
    parser.add_argument("--json", action="store_true",
                        help="Output raw JSON result")

    args = parser.parse_args()

    print(f"Sending to fleet inference (model={args.model})...", file=sys.stderr)

    try:
        result = infer(args.prompt, model=args.model, timeout=args.timeout,
                       broker=args.broker)

        if args.json:
            print(json.dumps(result["raw"], indent=2))
        else:
            print(f"\n--- Response (model={result['model']}, "
                  f"latency={result['latency_ms']}ms) ---", file=sys.stderr)
            print(result["response"])

    except TimeoutError as e:
        print(f"TIMEOUT: {e}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
