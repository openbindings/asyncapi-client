#!/usr/bin/env python3
"""Generate disposable public test identities; no production credentials.

Tests use the committed PEM files, so Python/cryptography is not a CI dependency.
Expected names, dates and trust relationships are independent of the client.
"""
from pathlib import Path
from datetime import datetime, timezone
from ipaddress import ip_address
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
import json

out = Path(__file__).resolve().parents[2] / 'rust/native/tests/fixtures/tls'
out.mkdir(exist_ok=True)
if any(out.glob('*.pem')):
    raise SystemExit('Refusing to overwrite existing fixture identities')
start = datetime(2020, 1, 1, tzinfo=timezone.utc)
end = datetime(2120, 1, 1, tzinfo=timezone.utc)
def name(value): return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])
def key(): return ec.generate_private_key(ec.SECP256R1())
def write(label, cert, private_key=None):
    (out / (label + '.pem')).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    if private_key:
        (out / (label + '.key.pem')).write_bytes(private_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
def ca(label):
    private = key()
    cert = (x509.CertificateBuilder().subject_name(name(label)).issuer_name(name(label)).public_key(private.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(start).not_valid_after(end)
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, None, None), critical=True)
            .sign(private, hashes.SHA256()))
    write(label, cert)
    return cert, private
root, root_key = ca('fixture-ca')
other, other_key = ca('other-ca')
for label, dns, expiry, client in [('server','localhost',end,False), ('wrong-name','wrong.example',end,False), ('expired','localhost',datetime(2021,1,1,tzinfo=timezone.utc),False), ('client','fixture-client',end,True)]:
    private = key()
    names = [x509.DNSName(dns)]
    if dns == 'localhost': names.append(x509.IPAddress(ip_address('127.0.0.1')))
    cert = (x509.CertificateBuilder().subject_name(name(dns)).issuer_name(root.subject).public_key(private.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(start).not_valid_after(expiry)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None),critical=True)
            .add_extension(x509.SubjectAlternativeName(names),critical=False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH if client else ExtendedKeyUsageOID.SERVER_AUTH]),critical=False)
            .add_extension(x509.KeyUsage(True,False,False,False,False,False,False,None,None),critical=True)
            .sign(root_key,hashes.SHA256()))
    write(label,cert,private)
(out/'identities.json').write_text(json.dumps({'purpose':'Disposable local test credentials only; private keys intentionally public.','trust':'server, wrong-name, expired and client signed by fixture-ca; other-ca is unrelated','server_names':['localhost','127.0.0.1'],'wrong_name':'wrong.example','expired_after':'2021-01-01T00:00:00Z','normal_validity':['2020-01-01T00:00:00Z','2120-01-01T00:00:00Z'],'client_identity':'fixture-client'},indent=2)+'\n')
