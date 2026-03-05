#!/usr/bin/env python3
from pathlib import Path

from PIL import Image

BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "scripts" / "eye-png.png"
OUT = BASE / "src" / "main" / "resources" / "static"
SIZES = [16, 32, 48, 64, 180, 192]

img = Image.open(SRC).convert("RGBA")
for size in SIZES:
    resized = img.resize((size, size), Image.Resampling.LANCZOS)
    resized.save(OUT / f"favicon-{size}x{size}.png", "PNG")

icon_sizes = [(16, 16), (32, 32), (48, 48)]
icon_images = [img.resize((w, h), Image.Resampling.LANCZOS) for w, h in icon_sizes]
icon_images[0].save(
    OUT / "favicon.ico",
    format="ICO",
    sizes=icon_sizes,
    append_images=icon_images[1:],
)
