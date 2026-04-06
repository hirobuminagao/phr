# -*- coding: utf-8 -*-
"""
pseudo_id.py
個人キー(person_key)を不可逆に擬似ID化するためのユーティリティ。
- HMAC-SHA256 + エンコード（Base32/62/URL-safe Base64）の短縮表現
- 入力: person_key, salt（必須） / 出力: 固定長の英数字（既定: Base32 長さ16）

注意:
- HMACベースのため復号はできません（不可逆）。
- 同じ salt + person_key なら常に同じIDになります（決定的）。
- salt は絶対に公開しないでください。
"""
import base64, hashlib, hmac
from typing import Literal

Alphabet = Literal['base32', 'base62', 'urlsafe']

_BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

def _hmac_digest(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()

def _encode_base32_nopad(b: bytes) -> str:
    return base64.b32encode(b).decode('ascii').rstrip('=')

def _encode_base62(b: bytes, length: int) -> str:
    num = int.from_bytes(b, 'big')
    chars = []
    while num > 0:
        num, rem = divmod(num, 62)
        chars.append(_BASE62[rem])
    s = ''.join(reversed(chars)) or '0'
    if len(s) < length:
        s = (_BASE62[0] * (length - len(s))) + s
    return s[:length]

def _encode_urlsafe(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode('ascii').rstrip('=')

def make_pseudo_id(person_key: str, *, salt: str, length: int = 16, alphabet: Alphabet = 'base32') -> str:
    if not salt:
        raise ValueError("salt が未設定です。")
    msg = person_key.encode('utf-8')
    key = salt.encode('utf-8')
    digest = _hmac_digest(key, msg)
    if alphabet == 'base32':
        enc = _encode_base32_nopad(digest)
    elif alphabet == 'base62':
        enc = _encode_base62(digest, length)
    elif alphabet == 'urlsafe':
        enc = _encode_urlsafe(digest)
    else:
        raise ValueError("alphabet は 'base32' | 'base62' | 'urlsafe' を指定してください。")
    if len(enc) < length:
        enc = (enc + enc)[0:length]
    return enc[:length]
