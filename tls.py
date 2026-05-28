"""
tls.py — TLS/SSL context construction for the load balancer listener.
"""

import ssl


class TLSContextBuilder:
    """
    Builds a server-side SSLContext from a certificate and private key.
    Enforces a minimum of TLS 1.2.
    """

    def __init__(self, cert_path: str, key_path: str) -> None:
        self._cert = cert_path
        self._key = key_path

    def build(self) -> ssl.SSLContext:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(self._cert, self._key)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        return ctx