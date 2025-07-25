import datetime
from typing import Optional

import jwt

def get_jwt_token(
    payload: dict,
    key: str,
    ttl: Optional[datetime.timedelta] = datetime.timedelta(hours=1)
) -> str:
    payload['exp'] = (datetime.datetime.now(datetime.timezone.utc) + ttl).timestamp()
    return jwt.encode(payload, key=key, algorithm='HS256')


def decode_token(token: str, key: str) -> dict:
    return jwt.decode(token, key=key, algorithms=['HS256'])
