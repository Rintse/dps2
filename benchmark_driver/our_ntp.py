# Quick wrapper to query can NTP client if it is available, else use time.time()
from time import time
import ntplib

def getLocalTime(ntp = None):
    if ntp != None:
        client, address = ntp
        response = client.request(address)
        return response.recv_time

    return time()
