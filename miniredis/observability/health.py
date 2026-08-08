def health(ready: bool) -> dict[str, object]:
    return {"status": "ok", "ready": ready}
