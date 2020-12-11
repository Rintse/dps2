#!/usr/bin/python3

import http.server
import socketserver
import os

PORT = 5090

os.chdir("DPS2/webserver")

Handler = http.server.SimpleHTTPRequestHandler
httpd = socketserver.TCPServer(("", PORT), Handler)
print("serving at port", PORT)

while(True):
        try:
                httpd.serve_forever()
        except Exception:
                httpd.server_close()
                exit()
