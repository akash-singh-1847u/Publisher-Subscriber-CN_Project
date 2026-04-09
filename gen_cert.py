from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import ipaddress, datetime

key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "10.1.1.5")])

cert = (
    x509.CertificateBuilder()
    .subject_name(subject)
    .issuer_name(issuer)
    .public_key(key.public_key())
    .serial_number(x509.random_serial_number())
    .not_valid_before(datetime.datetime.utcnow())
    .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
    .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.IPv4Address("10.1.1.5"))]), critical=False)
    .sign(key, hashes.SHA256())
)

open("server.key", "wb").write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
open("server.crt", "wb").write(cert.public_bytes(serialization.Encoding.PEM))

print("Done! server.key and server.crt generated.")