import base64
import os
import secrets

from cassandra.auth import AuthProvider
from cassandra.auth import Authenticator
from metatron import sign

class MetatronAuthProvider(AuthProvider):
    def new_authenticator(self, host):
        return MetatronAuthenticator()


class MetatronAuthenticator(Authenticator):
    def evaluate_challenge(self, challenge):
        sig_data = dict()
        sig_data['serverRandom'] = base64.b64encode(challenge)
        client_random = secrets.token_bytes(64)
        sig_data['clientRandom'] = base64.b64encode(client_random)
        sig_string = sign.urlencode(sig_data)

        metadata_signature = sign.sign_metadata(sig_string)

        result_dict = dict()
        result_dict["sigData"] = sig_string
        result_dict["signature"] = metadata_signature
        result_string = sign.urlencode(result_dict)

        return result_string.encode()