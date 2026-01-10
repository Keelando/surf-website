#!/usr/bin/env python3
"""
Grab stills from Hollyburn Sailing Club Yawcam webcam.
Handles the session handshake that Yawcam requires.

NOTE: This code is functional and tested, but we are currently waiting for
direct approval from Hollyburn Sailing Club to parse their webcam feed.
Do not enable in production until explicit permission is granted.
"""
import requests
import time
from pathlib import Path
from datetime import datetime
import random

BASE_URL = "http://onsite.hollyburnsailingclub.ca:8081/"
OUTPUT_DIR = Path("~/site/data/webcam").expanduser()
QUALITY = 50  # 1-100, higher = better quality/larger file

class YawcamGrabber:
    def __init__(self):
        self.session = requests.Session()
        self.session_id = None
        self.quality = QUALITY
        
    def generate_session_id(self):
        """Generate random session ID like the JavaScript does"""
        self.session_id = random.random()
        
    def handshake(self):
        """Establish session with Yawcam server"""
        self.generate_session_id()
        
        try:
            url = f"{BASE_URL}get"
            params = {
                'id': self.session_id,
                'r': random.random()
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            status = response.text.strip()
            
            if status == "ok":
                return True
            elif status == "2many":
                print("❌ Too many viewers - try again later")
                return False
            elif status == "pass":
                print("❌ Webcam requires password")
                return False
            else:
                print(f"⚠️  Server response: '{status}'")
                return False
                
        except Exception as e:
            print(f"❌ Handshake failed: {e}")
            return False
    
    def grab_image(self):
        """Grab a single image after successful handshake"""
        if not self.session_id:
            return False
            
        try:
            url = f"{BASE_URL}out.jpg"
            params = {
                'q': self.quality,
                'id': self.session_id,
                'r': int(time.time() * 1000)
            }
            
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            if len(response.content) < 1000:
                print(f"⚠️  Response too small ({len(response.content)} bytes)")
                return False
            
            if not response.content.startswith(b'\xff\xd8'):
                print("⚠️  Not a valid JPEG")
                return False
            
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            
            latest_file = OUTPUT_DIR / "hollyburn_latest.jpg"
            
            # Atomic write
            tmp = latest_file.with_suffix(".jpg.tmp")
            tmp.write_bytes(response.content)
            tmp.replace(latest_file)
            
            size_kb = len(response.content) / 1024
            print(f"✅ Hollyburn webcam: {size_kb:.1f} KB")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to grab image: {e}")
            return False


def main():
    grabber = YawcamGrabber()
    
    if not grabber.handshake():
        return 1
    
    time.sleep(0.5)
    
    if not grabber.grab_image():
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
