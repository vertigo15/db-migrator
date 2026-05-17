"""Smoke test for the prompt-merger LLM connection.

Run inside the prompt-merger container:
    python test_llm_connection.py
"""
import json
import sys

from llm_client import LLMClient


def main() -> int:
    client = None
    try:
        client = LLMClient()
        content = client.chat(
            system="You are a connectivity smoke test. Reply with exactly: OK",
            user="Return OK",
        )
        print(json.dumps({
            "status": "ok",
            "provider": client.provider,
            "base_url": client.effective_url,
            "model": client.model,
            "output_preview": content[:200],
            "output_chars": len(content),
        }, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({
            "status": "error",
            "provider": getattr(client, "provider", None),
            "base_url": getattr(client, "effective_url", None),
            "model": getattr(client, "model", None),
            "exception_type": type(exc).__name__,
            "exception_message": str(exc)[:1000],
        }, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())
