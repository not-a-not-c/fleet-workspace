#!/usr/bin/env python3
"""
Distributed Inference Server for Jetson Orin Nano
Listens on fleet/inference/request, runs prompts through Ollama API, publishes to fleet/inference/result
"""

import json
import time
import requests
import paho.mqtt.client as mqtt

BROKER = "Johns-MacBook-Pro-5937.local"
REQUEST_TOPIC = "fleet/inference/request"
RESULT_TOPIC = "fleet/inference/result"
STATUS_TOPIC = "fleet/status/jetson-inference"
MODEL = "tinyllama"
OLLAMA_API = "http://127.0.0.1:11434/api/generate"

def run_inference(prompt, model=MODEL):
    """Run inference via Ollama API and return the result"""
    try:
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False
        }

        response = requests.post(
            OLLAMA_API,
            json=payload,
            timeout=90
        )

        if response.status_code != 200:
            return {
                "success": False,
                "output": None,
                "error": f"Ollama API returned status {response.status_code}"
            }

        data = response.json()
        return {
            "success": True,
            "output": data.get("response", "").strip(),
            "error": None
        }

    except requests.exceptions.Timeout:
        return {
            "success": False,
            "output": None,
            "error": "Inference timeout (90s)"
        }
    except requests.exceptions.ConnectionError:
        return {
            "success": False,
            "output": None,
            "error": "Cannot connect to Ollama API at localhost:11434"
        }
    except Exception as e:
        return {
            "success": False,
            "output": None,
            "error": f"API error: {str(e)}"
        }

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe(REQUEST_TOPIC)
    print(f"Subscribed to {REQUEST_TOPIC}")

    # Publish online status
    status = {
        "service": "inference-server",
        "status": "online",
        "model": MODEL,
        "timestamp": int(time.time())
    }
    client.publish(STATUS_TOPIC, json.dumps(status), retain=True)

def on_message(client, userdata, msg):
    print(f"\n[{time.strftime('%H:%M:%S')}] Received inference request")

    try:
        request = json.loads(msg.payload.decode())
        request_id = request.get("id", "unknown")
        prompt = request.get("prompt", "")
        requester = request.get("from", "unknown")
        model = request.get("model", MODEL)  # Allow model override

        print(f"  Request ID: {request_id}")
        print(f"  From: {requester}")
        print(f"  Model: {model}")
        print(f"  Prompt: {prompt[:80]}..." if len(prompt) > 80 else f"  Prompt: {prompt}")

        # Validate prompt
        if not prompt or not prompt.strip():
            response = {
                "id": f"inf-{int(time.time())}-error",
                "from": "jetson-coordinator",
                "ref": request_id,
                "model": model,
                "response": "ERROR: Empty prompt provided",
                "ts": int(time.time()),
                "latency_ms": 0,
                "error": True
            }
            client.publish(RESULT_TOPIC, json.dumps(response))
            print(f"  ✗ Empty prompt rejected")
            return

        # Run inference
        print(f"  Running inference with {model}...")
        start_time = time.time()
        result = run_inference(prompt, model)
        elapsed = time.time() - start_time

        # Publish result in the correct format
        latency_ms = int(elapsed * 1000)

        if result["success"]:
            response = {
                "id": f"inf-{int(time.time())}-result",
                "from": "jetson-coordinator",
                "ref": request_id,
                "model": model,
                "response": result["output"],
                "ts": int(time.time()),
                "latency_ms": latency_ms
            }
            print(f"  ✓ Inference complete in {elapsed:.2f}s ({latency_ms}ms)")
            output_preview = result['output'][:100] + "..." if len(result['output']) > 100 else result['output']
            print(f"  Output: {output_preview}")
        else:
            # Error response format
            response = {
                "id": f"inf-{int(time.time())}-error",
                "from": "jetson-coordinator",
                "ref": request_id,
                "model": model,
                "response": f"ERROR: {result['error']}",
                "ts": int(time.time()),
                "latency_ms": latency_ms,
                "error": True
            }
            print(f"  ✗ Inference failed: {result['error']}")

        client.publish(RESULT_TOPIC, json.dumps(response))

    except json.JSONDecodeError as e:
        print(f"  ✗ Invalid JSON: {e}")
    except Exception as e:
        print(f"  ✗ Error processing request: {e}")

def check_ollama():
    """Verify Ollama is accessible"""
    try:
        response = requests.get("http://127.0.0.1:11434/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get("models", [])
            print(f"✓ Ollama is running")
            print(f"  Available models: {', '.join([m['name'] for m in models])}")
            return True
        return False
    except Exception as e:
        print(f"✗ Ollama check failed: {e}")
        return False

def main():
    print("=" * 60)
    print("Jetson Distributed Inference Server")
    print("=" * 60)
    print(f"Default Model: {MODEL}")
    print(f"Ollama API: {OLLAMA_API}")
    print(f"MQTT Broker: {BROKER}")
    print(f"Listening on: {REQUEST_TOPIC}")
    print(f"Publishing to: {RESULT_TOPIC}")
    print(f"Status topic: {STATUS_TOPIC}")
    print("=" * 60)
    print()

    # Check Ollama first
    print("Checking Ollama availability...")
    if not check_ollama():
        print("WARNING: Ollama may not be ready, but continuing...")
    print()

    # Create MQTT client (using callback API v2)
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="jetson-inference-server",
        protocol=mqtt.MQTTv5
    )

    client.on_connect = on_connect
    client.on_message = on_message

    # Set last will (offline status)
    offline_status = {
        "service": "inference-server",
        "status": "offline",
        "timestamp": int(time.time())
    }
    client.will_set(STATUS_TOPIC, json.dumps(offline_status), retain=True)

    # Connect and loop
    print("Connecting to MQTT broker...")
    client.connect(BROKER, 1883, 60)
    print("Ready for inference requests!\n")

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n\nShutting down inference server...")

        # Publish offline status
        offline_status = {
            "service": "inference-server",
            "status": "offline",
            "timestamp": int(time.time())
        }
        client.publish(STATUS_TOPIC, json.dumps(offline_status), retain=True)

        client.disconnect()

if __name__ == "__main__":
    main()
