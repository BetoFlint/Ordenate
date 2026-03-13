from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken


def _get_fernet_key() -> bytes:
    """Obtiene una clave Fernet desde entorno o derivada de SESSION_SECRET."""
    raw_key = os.environ.get("DATA_ENCRYPTION_KEY")
    if raw_key:
        candidate = raw_key.encode("utf-8")
        try:
            Fernet(candidate)
            return candidate
        except Exception:
            digest = hashlib.sha256(raw_key.encode("utf-8")).digest()
            return base64.urlsafe_b64encode(digest)

    # Fallback deterministico para entornos donde aun no se define DATA_ENCRYPTION_KEY.
    # Recomendado: definir DATA_ENCRYPTION_KEY como clave Fernet en produccion.
    seed = os.environ.get("SESSION_SECRET", "ordenate-secret-key-change-in-prod")
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


def _get_cipher() -> Fernet:
    return Fernet(_get_fernet_key())


def encrypt_amount(value: float | int | None) -> str | None:
    if value is None:
        return None
    amount = float(value)
    token = _get_cipher().encrypt(str(amount).encode("utf-8"))
    return token.decode("utf-8")


def decrypt_amount(token: str | None) -> float | None:
    if not token:
        return None
    try:
        plain = _get_cipher().decrypt(token.encode("utf-8"))
    except (InvalidToken, ValueError, TypeError):
        return None
    try:
        return float(plain.decode("utf-8"))
    except Exception:
        return None
