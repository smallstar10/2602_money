from __future__ import annotations

import requests


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    def send(self, text: str) -> None:
        if not self.token or not self.chat_id:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            requests.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True},
                timeout=10,
            ).raise_for_status()
        except Exception:
            # Notification failure must not crash the batch job.
            return

    def get_updates(self, offset: int | None = None, limit: int = 30, timeout: int = 5) -> list[dict]:
        if not self.token:
            return []
        url = f"https://api.telegram.org/bot{self.token}/getUpdates"
        params: dict[str, object] = {"limit": max(1, min(limit, 100)), "timeout": max(0, min(timeout, 25))}
        if offset is not None:
            params["offset"] = int(offset)
        try:
            resp = requests.get(url, params=params, timeout=timeout + 5)
            resp.raise_for_status()
            obj = resp.json()
            if not isinstance(obj, dict) or not obj.get("ok"):
                return []
            result = obj.get("result")
            if not isinstance(result, list):
                return []
            return [x for x in result if isinstance(x, dict)]
        except Exception:
            return []
