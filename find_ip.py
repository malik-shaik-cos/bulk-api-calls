import socket
import requests

local_ip = socket.gethostbyname(socket.gethostname())

print(f"Local IP: {local_ip}")


public_ip = requests.get("https://api.ipify.org").text
print(f"Local IP: {public_ip}")
