import os
import sys
import json
import platform

from datetime import datetime

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def main() -> int:
    # Load env vars from .env if available
    if load_dotenv:
        try:
            load_dotenv()
        except Exception as ex:
            eprint(f"Warning: failed to load .env: {ex}")

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        eprint("OPENAI_API_KEY is not set. Add it to .env or environment.")
        print(json.dumps({
            "ok": False,
            "error": "missing_api_key",
            "hint": "Create a .env file with OPENAI_API_KEY=sk-...",
        }))
        return 2

    # Avoid printing full secret
    redacted = api_key[:6] + "…" if len(api_key) > 6 else "(short)"

    # Prepare client and make a simple test call
    try:
        from openai import OpenAI
        # Explicitly pass api_key to avoid relying on ambient state
        client = OpenAI(api_key=api_key)

        # Minimal Responses API call
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input="Reply with a short greeting saying the setup works.",
        )

        # Prefer the convenience property when present
        text = getattr(resp, "output_text", None)
        if not text:
            # Fallback: attempt to pull from the first content part
            try:
                # Newer SDKs: resp.output[0].content[0].text.value
                parts = []
                for item in getattr(resp, "output", []) or []:
                    for c in getattr(item, "content", []) or []:
                        t = getattr(getattr(c, "text", None), "value", None)
                        if t:
                            parts.append(t)
                text = "\n".join(parts)
            except Exception:
                text = None

        if not text:
            text = str(resp)

        # Print a compact JSON so callers can parse if desired
        print(json.dumps({
            "ok": True,
            "model": "gpt-4.1-mini",
            "message": text.strip(),
            "at": datetime.utcnow().isoformat() + "Z",
            "python": platform.python_version(),
        }, ensure_ascii=False))
        return 0

    except Exception as ex:
        # Collect diagnostics for quick triage
        info = {
            "ok": False,
            "error": type(ex).__name__,
            "message": str(ex),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "api_key_prefix": redacted,
        }

        # Attempt to extract HTTP-ish details if present
        for attr in ("status_code", "code", "response"):
            val = getattr(ex, attr, None)
            if val is not None:
                try:
                    info[attr] = int(val) if isinstance(val, (int,)) else str(val)
                except Exception:
                    info[attr] = str(val)

        print(json.dumps(info, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    sys.exit(main())

