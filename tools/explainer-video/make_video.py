"""Render the AIRS-Lite explainer video: animated scenes (Pillow) + narration (narrate.ps1) -> MP4 (ffmpeg).

Usage:
  powershell -File narrate.ps1                 # once: build/audio/<scene>.wav
  python make_video.py [--out media/airs-lite-explainer.mp4] [--preview SCENE]
Needs Pillow, numpy and imageio-ffmpeg (bundled ffmpeg binary).
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import wave

import numpy as np
from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
AUDIO_DIR = os.path.join(HERE, "build", "audio")
W, H, FPS = 1280, 720, 30
LEAD = 0.5          # seconds of silence before narration starts in each scene
TAIL = 0.9          # seconds after narration ends
FADE = 0.35

# ───────────────────────────── palette & fonts ─────────────────────────────
BG = (11, 18, 32)
PANEL = (20, 30, 52)
PANEL2 = (30, 43, 72)
LINE = (51, 65, 95)
TEXT = (236, 241, 248)
MUTED = (148, 163, 184)
TEAL = (45, 212, 191)
BLUE = (96, 165, 250)
AMBER = (245, 158, 11)
RED = (239, 68, 68)
GREEN = (34, 197, 94)
PURPLE = (167, 139, 250)
BAND = {"Low": GREEN, "Medium": AMBER, "High": (249, 115, 22), "Critical": RED}

FONTS = {
    "regular": "C:/Windows/Fonts/segoeui.ttf",
    "semibold": "C:/Windows/Fonts/seguisb.ttf",
    "bold": "C:/Windows/Fonts/segoeuib.ttf",
    "light": "C:/Windows/Fonts/segoeuil.ttf",
    "mono": "C:/Windows/Fonts/consola.ttf",
    "emoji": "C:/Windows/Fonts/seguiemj.ttf",
}
_font_cache: dict = {}


def font(size: int, weight: str = "regular") -> ImageFont.FreeTypeFont:
    key = (size, weight)
    if key not in _font_cache:
        _font_cache[key] = ImageFont.truetype(FONTS[weight], size)
    return _font_cache[key]


# ───────────────────────────── animation helpers ─────────────────────────────

def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def ease(x):
    x = clamp(x)
    return 1 - (1 - x) ** 3


def appear(t, start, dur=0.5):
    return ease((t - start) / dur)


def mix(c, a, bg=BG):
    a = clamp(a)
    return tuple(int(bg[i] + (c[i] - bg[i]) * a) for i in range(3))


class Canvas:
    def __init__(self, img: Image.Image):
        self.img = img
        self.d = ImageDraw.Draw(img)

    def text(self, xy, s, size=24, color=TEXT, a=1.0, anchor="la", weight="regular", bg=BG):
        if a <= 0.01:
            return
        self.d.text(xy, s, font=font(size, weight), fill=mix(color, a, bg), anchor=anchor)

    def text_w(self, s, size, weight="regular"):
        return self.d.textlength(s, font=font(size, weight))

    def wrap(self, xy, s, size, max_w, color=TEXT, a=1.0, weight="regular", bg=BG, gap=1.25, anchor_x="l"):
        words, lines, cur = s.split(), [], ""
        for w_ in words:
            trial = (cur + " " + w_).strip()
            if self.text_w(trial, size, weight) <= max_w:
                cur = trial
            else:
                lines.append(cur)
                cur = w_
        if cur:
            lines.append(cur)
        x, y = xy
        for i, line in enumerate(lines):
            anchor = "ma" if anchor_x == "m" else "la"
            self.text((x, y + i * size * gap), line, size, color, a, anchor, weight, bg)
        return len(lines) * size * gap

    def box(self, b, fill=PANEL, a=1.0, r=14, outline=None, width=2, bg=BG):
        if a <= 0.01:
            return
        self.d.rounded_rectangle(b, radius=r, fill=mix(fill, a, bg),
                                 outline=mix(outline, a, bg) if outline else None, width=width)

    def line(self, p1, p2, color=LINE, a=1.0, width=3, bg=BG):
        if a > 0.01:
            self.d.line([p1, p2], fill=mix(color, a, bg), width=width)

    def arrow(self, p1, p2, color=TEAL, a=1.0, prog=1.0, width=3, bg=BG, head=11):
        """Line from p1 towards p2 drawn up to `prog`, with an arrowhead once complete."""
        if a <= 0.01 or prog <= 0:
            return
        x = p1[0] + (p2[0] - p1[0]) * prog
        y = p1[1] + (p2[1] - p1[1]) * prog
        self.d.line([p1, (x, y)], fill=mix(color, a, bg), width=width)
        if prog > 0.97:
            ang = math.atan2(p2[1] - p1[1], p2[0] - p1[0])
            pts = [p2, (p2[0] - head * math.cos(ang - 0.45), p2[1] - head * math.sin(ang - 0.45)),
                   (p2[0] - head * math.cos(ang + 0.45), p2[1] - head * math.sin(ang + 0.45))]
            self.d.polygon(pts, fill=mix(color, a, bg))

    def dashed(self, p1, p2, color=LINE, a=1.0, width=2, dash=8, bg=BG):
        if a <= 0.01:
            return
        length = math.dist(p1, p2)
        for k in range(0, int(length), dash * 2):
            f0, f1 = k / length, min(1.0, (k + dash) / length)
            self.d.line([(p1[0] + (p2[0] - p1[0]) * f0, p1[1] + (p2[1] - p1[1]) * f0),
                         (p1[0] + (p2[0] - p1[0]) * f1, p1[1] + (p2[1] - p1[1]) * f1)],
                        fill=mix(color, a, bg), width=width)

    def packet(self, p1, p2, t, speed=0.8, color=TEAL, a=1.0, n=2, r=5, bg=BG):
        """Dots travelling along an arrow: data in motion."""
        if a <= 0.01:
            return
        for k in range(n):
            f = (t * speed + k / n) % 1.0
            x = p1[0] + (p2[0] - p1[0]) * f
            y = p1[1] + (p2[1] - p1[1]) * f
            self.d.ellipse([x - r, y - r, x + r, y + r], fill=mix(color, a * (0.4 + 0.6 * math.sin(math.pi * f)), bg))

    def emoji(self, ch, center, size=56, a=1.0):
        if a <= 0.01:
            return
        icon = emoji_img(ch, size)
        if a < 0.99:
            alpha = icon.getchannel("A").point(lambda v: int(v * a))
        else:
            alpha = icon.getchannel("A")
        self.img.paste(icon.convert("RGB"), (int(center[0] - icon.width / 2), int(center[1] - icon.height / 2)), alpha)

    def check(self, center, a=1.0, color=GREEN, r=13, bg=BG):
        if a <= 0.01:
            return
        x, y = center
        self.d.ellipse([x - r, y - r, x + r, y + r], fill=mix(color, a, bg))
        self.d.line([(x - r * 0.45, y), (x - r * 0.1, y + r * 0.38), (x + r * 0.5, y - r * 0.35)],
                    fill=mix(BG, a, mix(color, a, bg)), width=3)

    def pill(self, center, s, color=TEAL, a=1.0, size=18, pad=14, bg=BG, weight="semibold"):
        tw = self.text_w(s, size, weight)
        x, y = center
        self.box([x - tw / 2 - pad, y - size * 0.85, x + tw / 2 + pad, y + size * 0.85],
                 fill=mix(color, 0.18, bg), a=a, r=int(size), outline=color, width=2, bg=bg)
        self.text((x, y), s, size, color, a, "mm", weight, bg)

    def header(self, title, sub=None, t=0.0):
        self.text((64, 52), title, 40, TEXT, appear(t, 0.1), "la", "bold")
        if sub:
            self.text((66, 104), sub, 20, MUTED, appear(t, 0.3), "la")


_emoji_cache: dict = {}


def emoji_img(ch, size):
    key = (ch, size)
    if key not in _emoji_cache:
        f = font(109, "emoji")  # Segoe UI Emoji renders best at its native strike, then scale
        tmp = Image.new("RGBA", (150, 150), (0, 0, 0, 0))
        ImageDraw.Draw(tmp).text((75, 75), ch, font=f, embedded_color=True, anchor="mm")
        bbox = tmp.getbbox() or (0, 0, 150, 150)
        tmp = tmp.crop(bbox)
        scale = size / max(tmp.width, tmp.height)
        _emoji_cache[key] = tmp.resize((max(1, int(tmp.width * scale)), max(1, int(tmp.height * scale))),
                                       Image.LANCZOS)
    return _emoji_cache[key]


def make_background():
    y = np.linspace(0, 1, H)[:, None]
    x = np.linspace(0, 1, W)[None, :]
    top, bottom = np.array(BG, float), np.array((14, 24, 44), float)
    grad = top + (bottom - top) * (y * 0.8 + x * 0.2)[..., None]
    img = Image.fromarray(grad.astype(np.uint8), "RGB")
    d = ImageDraw.Draw(img)
    for gx in range(40, W, 40):          # faint dot grid
        for gy in range(40, H - 110, 40):
            d.point((gx, gy), fill=(26, 37, 60))
    return img


# ───────────────────────────── scenes ─────────────────────────────
# Each scene: fn(c: Canvas, t: seconds into scene, T: scene length). Timings use fractions of T so
# visuals track the narration length.

def scene_title(c, t, T):
    cx, cy = W / 2, 270
    for k in range(4):                                   # radar pulse
        r = ((t * 70 + k * 90) % 360)
        a = 0.35 * (1 - r / 360) * appear(t, 0.0, 1.0)
        c.d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=mix(TEAL, a), width=2)
    c.emoji("🛡", (cx, cy - 115), 70, appear(t, 0.1))
    c.text((cx, cy), "AIRS-Lite", 96, TEXT, appear(t, 0.3), "mm", "bold")
    c.text((cx, cy + 78), "An AI Asset Risk Lakehouse", 36, MUTED, appear(t, 0.8), "mm", "light")
    c.text((cx, cy + 135), "Find every AI tool  ·  Score its risk  ·  Control it in real time", 24, TEAL,
           appear(t, 1.5), "mm", "semibold")
    stack = ["Apache Flink", "Apache Spark", "Apache Iceberg", "Apache Airflow", "Redpanda"]
    for i, s in enumerate(stack):
        c.pill((cx + (i - 2) * 215, cy + 205), s, BLUE, appear(t, 2.4 + i * 0.25), 17)


def scene_problem(c, t, T):
    c.header("Shadow AI is already inside every company", "Employees adopt AI tools in minutes. Nobody approves them.", t)
    co = (760, 175, 1215, 545)
    c.box(co, PANEL, appear(t, 0.3), 22, LINE)
    c.emoji("🏢", (co[0] + 48, co[1] + 42), 44, appear(t, 0.3))
    c.text((co[0] + 84, co[1] + 30), "Your company", 26, TEXT, appear(t, 0.3), "la", "semibold", PANEL)
    items = [("💬", "Browser chatbot"), ("💻", "Coding agent"), ("🔌", "MCP plug-in"),
             ("🧠", "Local model"), ("🧩", "Browser extension")]
    flying, landed = [], []
    for i, (em, label) in enumerate(items):
        start = T * (0.10 + i * 0.11)
        p = ease((t - start) / 1.3)
        if t < start:
            continue
        sx, sy = 120, 200 + i * 72
        tx, ty = co[0] + 70 + (i % 2) * 210, co[1] + 115 + (i // 2) * 80
        # Arc in, arriving horizontally early (p = 0.8) and finishing with a vertical drop into the slot.
        x = sx + (tx - sx) * clamp(p / 0.8)
        y = sy + (ty - sy) * p - 150 * math.sin(math.pi * p)
        (landed if p > 0.97 else flying).append((em, label, x, y, start))
    for em, _, x, y, _ in flying:            # icons in flight pass behind those already inside
        c.emoji(em, (x, y), 38, 1.0)
    for em, label, x, y, start in landed:
        c.box([x - 24, y - 24, x + 24, y + 24], PANEL, 1, 8, bg=PANEL)
        c.emoji(em, (x, y), 38, 1.0)
        c.text((x + 30, y), label, 19, TEXT, appear(t, start + 1.2, 0.3), "lm", "semibold", PANEL)
        c.text((x + 30, y + 22), "UNAPPROVED", 12, RED, appear(t, start + 1.3), "lm", "bold", PANEL)
    s = T * 0.80
    c.text((75, 300), "Shadow AI", 54, AMBER, appear(t, s), "la", "bold")
    c.text((78, 365), "Invisible to firewalls, antivirus and network tools", 22, MUTED, appear(t, s + 0.4), "la")


def scene_why(c, t, T):
    c.header("Why it matters", "Four reasons companies can't ignore AI they don't know about", t)
    cards = [("📤", "Data leaks", "Prompts and uploads carry customer data, code and secrets out of the company.", RED),
             ("🤖", "Autonomous agents", "Agents run code, query databases and call APIs on their own.", PURPLE),
             ("📜", "Regulation", "EU AI Act and ISO 42001 expect an inventory and evidence for the AI in use.", BLUE),
             ("💸", "Wasted spend", "Overlapping AI licences that nobody uses, bought team by team.", AMBER)]
    for i, (em, title, body, col) in enumerate(cards):
        a = appear(t, T * (0.05 + i * 0.22), 0.6)
        x0 = 64 + i * 293
        y_off = (1 - a) * 30
        b = [x0, 175 + y_off, x0 + 270, 455 + y_off]
        c.box(b, PANEL, a, 18, None)
        c.d.rectangle([b[0] + 18, b[1], b[2] - 18, b[1] + 5], fill=mix(col, a))
        c.emoji(em, (x0 + 135, 250 + y_off), 64, a)
        c.text((x0 + 135, 320 + y_off), title, 26, TEXT, a, "mm", "bold", PANEL)
        c.wrap((x0 + 135, 352 + y_off), body, 18, 228, MUTED, a, bg=PANEL, anchor_x="m")


def scene_analogy(c, t, T):
    c.header("Airport security, but for AI", "Every part of the project has a familiar counterpart", t)
    rows = [("📋", "List of banned items", "AI risk catalog: every tool scored 0–10"),
            ("📡", "Scanners at every gate", "Live event stream from every laptop"),
            ("👮", "Officer decides", "Real-time engine: allow · warn · block"),
            ("🙈", "Blurred faces on CCTV", "Sensitive data masked before storage"),
            ("📼", "Sealed CCTV recordings", "Tamper-proof, hash-chained audit log")]
    c.text((180, 160), "AIRPORT", 16, MUTED, appear(t, 0.4), "la", "bold")
    c.text((690, 160), "AIRS-LITE", 16, TEAL, appear(t, 0.4), "la", "bold")
    for i, (em, left, right) in enumerate(rows):
        a = appear(t, T * (0.06 + i * 0.16), 0.5)
        y = 215 + i * 76
        c.box([110, y - 30, 560, y + 30], PANEL, a, 14)
        c.emoji(em, (150, y), 34, a)
        c.text((185, y), left, 22, TEXT, a, "lm", "semibold", PANEL)
        c.arrow((580, y), (660, y), TEAL, a, appear(t, T * (0.06 + i * 0.16) + 0.3, 0.5))
        c.box([675, y - 30, 1215, y + 30], PANEL2, a, 14, TEAL, 2)
        c.text((700, y), right, 22, TEXT, a, "lm", "semibold", PANEL2)


def scene_architecture(c, t, T):
    c.header("How it is built", "Batch intelligence + real-time decisions on one shared lakehouse", t)

    def node(b, title, sub, col, a, em=None):
        c.box(b, PANEL, a, 16, col, 2)
        x = (b[0] + b[2]) / 2
        if em:
            c.emoji(em, (b[0] + 30, (b[1] + b[3]) / 2), 30, a)
            x += 14
        c.text((x, (b[1] + b[3]) / 2 - 11), title, 21, TEXT, a, "mm", "bold", PANEL)
        c.text((x, (b[1] + b[3]) / 2 + 15), sub, 14, MUTED, a, "mm", "regular", PANEL)

    a_batch, a_stream = appear(t, T * 0.08), appear(t, T * 0.30)
    a_ice, a_af, a_bi = appear(t, T * 0.52), appear(t, T * 0.66), appear(t, T * 0.80)
    src, spark = [40, 230, 250, 310], [320, 230, 540, 310]
    fleet, rp, flink = [40, 450, 250, 530], [320, 450, 540, 530], [610, 450, 830, 530]
    ice, bi, af = [890, 280, 1080, 480], [1120, 330, 1250, 430], [440, 140, 760, 195]
    node(src, "Public sources", "5 APIs: HF, MCP, npm, OSV…", BLUE, a_batch)
    node(spark, "Apache Spark", "batch: catalog & scoring", BLUE, a_batch, "⚡")
    c.arrow((250, 270), (320, 270), BLUE, a_batch, appear(t, T * 0.08 + 0.3))
    node(fleet, "500 laptops", "simulated AI activity", TEAL, a_stream, "💻")
    node(rp, "Redpanda", "Kafka-compatible event bus", TEAL, a_stream)
    node(flink, "Apache Flink", "real-time verdicts", TEAL, a_stream, "🌊")
    c.arrow((250, 490), (320, 490), TEAL, a_stream, appear(t, T * 0.30 + 0.3))
    c.arrow((540, 490), (610, 490), TEAL, a_stream, appear(t, T * 0.30 + 0.5))
    c.arrow((430, 310), (430, 450), AMBER, a_stream, appear(t, T * 0.30 + 0.7))
    c.text((440, 380), "risk scores", 15, AMBER, a_stream, "lm", "semibold")
    # Iceberg as a stacked "table" symbol
    c.box(ice, PANEL2, a_ice, 20, PURPLE, 3)
    c.emoji("🧊", ((ice[0] + ice[2]) / 2, ice[1] + 55), 52, a_ice)
    c.text(((ice[0] + ice[2]) / 2, ice[1] + 120), "Apache Iceberg", 22, TEXT, a_ice, "mm", "bold", PANEL2)
    c.text(((ice[0] + ice[2]) / 2, ice[1] + 148), "shared lakehouse", 15, MUTED, a_ice, "mm", "regular", PANEL2)
    c.arrow((540, 270), (890, 330), PURPLE, a_ice, appear(t, T * 0.52 + 0.2))
    c.arrow((830, 490), (890, 430), PURPLE, a_ice, appear(t, T * 0.52 + 0.4))
    node(af, "Apache Airflow", "orchestrates every batch job", AMBER, a_af, "🗓")
    c.dashed((500, 195), (500, 230), AMBER, a_af)      # Airflow -> Spark
    c.dashed((720, 195), (720, 450), AMBER, a_af)      # Airflow -> Flink job health
    node(bi, "Dashboards", "Trino · Superset", GREEN, a_bi)
    c.arrow((1080, 380), (1120, 380), GREEN, a_bi, appear(t, T * 0.80 + 0.2))
    if a_stream > 0.9:
        c.packet((250, 490), (320, 490), t, 0.9, TEAL)
        c.packet((540, 490), (610, 490), t, 0.9, TEAL)
    if a_ice > 0.9:
        c.packet((540, 270), (890, 330), t, 0.5, PURPLE)
        c.packet((830, 490), (890, 430), t, 0.7, PURPLE)


PHASES = [("0", "Platform"), ("1", "Risk catalog"), ("2", "Streaming"), ("3", "Dashboards"),
          ("4", "Investigation"), ("5", "Hardening")]


def scene_progress(c, t, T):
    c.header("Where we are", "Six phases, each ending in something that runs end to end", t)
    x0, x1, y = 130, 1150, 330
    step = (x1 - x0) / 5
    c.line((x0, y), (x1, y), LINE, appear(t, 0.2), 6)
    done_p = appear(t, T * 0.25, T * 0.25)
    c.line((x0, y), (x0 + step * done_p, y), GREEN, 1, 6)
    for i, (n, name) in enumerate(PHASES):
        x = x0 + i * step
        a = appear(t, 0.3 + i * 0.12)
        done = i < 2 and done_p >= i / 1.0 * 0.999 and t > T * 0.25 + i * T * 0.12
        nxt = i == 2 and t > T * 0.6
        col = GREEN if done else (AMBER if nxt else LINE)
        r = 30 + (3 * math.sin(t * 4) if nxt else 0)
        c.d.ellipse([x - r, y - r, x + r, y + r], fill=mix(PANEL, a), outline=mix(col, a), width=4)
        if done:
            c.check((x, y), 1, GREEN, 18)
        else:
            c.text((x, y), n, 26, TEXT, a, "mm", "bold", PANEL)
        c.text((x, y + 58), name, 21, TEXT if (done or nxt) else MUTED, a, "mm", "semibold")
        if nxt:
            c.pill((x, y - 70), "NEXT", AMBER, 1, 15)
    pa = appear(t, T * 0.45)
    bar = [230, 480, 1050, 512]
    c.box(bar, PANEL, pa, 16)
    c.box([bar[0], bar[1], bar[0] + (bar[2] - bar[0]) * (2 / 6) * ease((t - T * 0.45) / 1.2), bar[3]], GREEN, pa, 16)
    c.text((640, 552), "2 of 6 phases complete", 24, TEXT, pa, "mm", "semibold")


SERVICES = ["redpanda", "redpanda-console", "seaweedfs (S3)", "s3-init", "iceberg-rest", "flink-jobmanager",
            "flink-taskmanager", "spark-master", "spark-worker", "postgres", "airflow", "trino", "superset"]
SMOKE = ["Spark creates the Iceberg table", "Flink SQL appends rows", "Airflow DAG submits a Spark job",
         "Trino reads all three writers", "Spark verifies the snapshots"]


def scene_phase0(c, t, T):
    c.header("Phase 0 · The platform", "13 services, one command:  ./airs.sh up", t)
    for i, s in enumerate(SERVICES):
        a = appear(t, 0.3 + i * T * 0.025, 0.4)
        col, row = i % 3, i // 3
        b = [64 + col * 205, 160 + row * 70, 254 + col * 205, 210 + row * 70]
        c.box(b, PANEL, a, 12)
        c.d.ellipse([b[0] + 14, b[1] + 19, b[0] + 26, b[1] + 31], fill=mix(GREEN, a, PANEL))
        c.text((b[0] + 36, (b[1] + b[3]) / 2), s, 16, TEXT, a, "lm", "semibold", PANEL)
    c.text((64, 640 - 85), "≈ 4.2 GB RAM for the whole stack", 18, MUTED, appear(t, T * 0.4), "la")
    p = [720, 160, 1215, 550]
    c.box(p, PANEL, appear(t, T * 0.35), 18, LINE)
    c.text((745, 190), "Smoke test", 26, TEXT, appear(t, T * 0.35), "la", "bold", PANEL)
    for i, s in enumerate(SMOKE):
        st = T * (0.42 + i * 0.09)
        a = appear(t, st, 0.4)
        y = 255 + i * 50
        c.check((760, y), a, GREEN, 13, PANEL)
        c.text((785, y), s, 19, TEXT, a, "lm", "regular", PANEL)
    c.pill((967, 512), "PASSED · twice in a row", GREEN, appear(t, T * 0.9), 18, bg=PANEL)


def scene_phase1(c, t, T):
    c.header("Phase 1 · The AI risk catalog", "catalog_daily: an Airflow DAG that runs every night", t)
    sources = [("🤗", "Hugging Face", "4,000 models & datasets"), ("🔌", "MCP Registry", "18,000 servers"),
               ("📦", "PyPI + npm", "3,245 packages"), ("🐙", "GitHub", "repo health"),
               ("🚨", "OSV.dev", "security advisories")]
    for i, (em, name, sub) in enumerate(sources):
        a = appear(t, 0.3 + i * T * 0.03)
        b = [40, 150 + i * 82, 270, 215 + i * 82]
        c.box(b, PANEL, a, 12, BLUE, 2)
        c.emoji(em, (b[0] + 30, (b[1] + b[3]) / 2), 30, a)
        c.text((b[0] + 58, b[1] + 21), name, 18, TEXT, a, "lm", "bold", PANEL)
        c.text((b[0] + 58, b[1] + 44), sub, 14, MUTED, a, "lm", "regular", PANEL)
        c.arrow((270, (b[1] + b[3]) / 2), (318, 345), BLUE, a, appear(t, 0.6 + i * T * 0.03))
    stages = [("Bronze", "raw, append-only", "🗄", BLUE),
              ("Resolve", "25,266 → 22,063", "🔗", PURPLE),
              ("Score", "capability · exposure · governance", "🎯", AMBER),
              ("History", "SCD2 versions", "🕒", TEAL),
              ("Redpanda", "asset-risk-updates", "📣", GREEN)]
    for i, (name, sub, em, col) in enumerate(stages):
        st = T * (0.18 + i * 0.13)
        a = appear(t, st)
        x0 = 320 + i * 188
        b = [x0, 275, x0 + 168, 415]
        c.box(b, PANEL, a, 16, col, 2)
        c.emoji(em, (x0 + 84, 310), 38, a)
        c.text((x0 + 84, 352), name, 21, TEXT, a, "mm", "bold", PANEL)
        c.wrap((x0 + 84, 372), sub, 13, 150, MUTED, a, bg=PANEL, anchor_x="m")
        if i:
            c.arrow((x0 - 20, 345), (x0, 345), col, a, appear(t, st + 0.2), 3, head=8)
            if a > 0.9:
                c.packet((x0 - 20, 345), (x0, 345), t, 1.2, col, n=1, r=4)
    c.text((320 + 2.5 * 188 - 10, 470), "Every score keeps its reasons, e.g. “can execute shell commands (+4)”",
           18, MUTED, appear(t, T * 0.85), "mm")


def counter(v, p, fmt="{:,.0f}"):
    return fmt.format(v * ease(p))


def scene_proof_numbers(c, t, T):
    c.header("Proof · measured on real data", "Every number comes from the pipeline's own checks", t)
    tiles = [(22063, "{:,.0f}", "AI assets scored", TEAL),
             (99.9, "{:.1f}%", "matching precision", GREEN),
             (77, "{:.0f}%", "matching recall", BLUE),
             (31, "{:.0f}", "unit tests passing", PURPLE),
             (16, "{:.0f}", "data-quality checks", AMBER),
             (0, "{:.0f}", "assets with two current rows", GREEN)]
    for i, (v, fmt, label, col) in enumerate(tiles):
        st = T * (0.05 + i * 0.1)
        a = appear(t, st)
        col_i, row = i % 3, i // 3
        b = [64 + col_i * 230, 165 + row * 190, 274 + col_i * 230, 330 + row * 190]
        c.box(b, PANEL, a, 18)
        c.text(((b[0] + b[2]) / 2, b[1] + 65), counter(v, (t - st) / 1.2, fmt), 46, col, a, "mm", "bold", PANEL)
        c.wrap(((b[0] + b[2]) / 2, b[1] + 105), label, 17, 180, MUTED, a, bg=PANEL, anchor_x="m")
    ca = appear(t, T * 0.55)
    panel = [790, 165, 1220, 520]
    c.box(panel, PANEL, ca, 18)
    c.text((815, 195), "Risk bands (current)", 22, TEXT, ca, "la", "bold", PANEL)
    bands = [("Low", 17906), ("Medium", 3378), ("High", 778), ("Critical", 1)]
    for i, (b, n) in enumerate(bands):
        y = 260 + i * 62
        p = appear(t, T * 0.6 + i * 0.25, 1.0)
        width = 230 * (n / 17906) ** 0.5 * p       # square-root scale so small bands stay visible
        c.text((815, y), b, 18, TEXT, ca, "lm", "semibold", PANEL)
        c.box([905, y - 13, 905 + max(width, 4), y + 13], BAND[b], ca, 6, bg=PANEL)
        c.text((915 + max(width, 4), y), f"{int(n * ease(p)):,}", 17, MUTED, ca, "lm", "regular", PANEL)
    c.text((815, 500), "√ scale · Critical = chromadb (open critical advisory)", 13, MUTED, ca, "la", "regular", PANEL)


def scene_proof_bugs(c, t, T):
    c.header("Proof · real data found real bugs", "Each one found, fixed, and locked in with a test or a check", t)
    cards = [("Similar names ≠ same tool", "gen-ai MCP server merged with Google's AI SDK",
              "Plausibility guard + unit test"),
             ("Chained merges", "Two different MCP servers became one asset",
              "One server per package + DQ check"),
             ("Ghost assets", "A merged package stayed marked ‘current’",
              "SCD2 retirement + Redpanda tombstone"),
             ("Everything looked safe", "First formula scored 99% of assets Low",
              "Recalibrated weights, documented")]
    for i, (title, found, fixed) in enumerate(cards):
        st = T * (0.05 + i * 0.2)
        a = appear(t, st)
        col_i, row = i % 2, i // 2
        b = [64 + col_i * 585, 160 + row * 200, 624 + col_i * 585, 340 + row * 200]
        c.box(b, PANEL, a, 18)
        c.text((b[0] + 24, b[1] + 32), title, 24, TEXT, a, "lm", "bold", PANEL)
        c.text((b[0] + 24, b[1] + 78), "FOUND", 13, RED, a, "lm", "bold", PANEL)
        c.text((b[0] + 92, b[1] + 78), found, 17, MUTED, a, "lm", "regular", PANEL)
        fa = appear(t, st + T * 0.1)
        c.check((b[0] + 36, b[1] + 132), fa, GREEN, 13, PANEL)
        c.text((b[0] + 60, b[1] + 132), "FIXED", 13, GREEN, fa, "lm", "bold", PANEL)
        c.text((b[0] + 120, b[1] + 132), fixed, 18, TEXT, fa, "lm", "semibold", PANEL)


def gauge(c, center, score, p, a, bg=PANEL):
    x, y = center
    r = 78
    box = [x - r, y - r, x + r, y + r]
    c.d.arc(box, 180, 360, fill=mix(LINE, a, bg), width=16)
    v = score * ease(p)
    band = "Critical" if v >= 9 else "High" if v >= 7 else "Medium" if v >= 5 else "Low"
    if v > 0:
        c.d.arc(box, 180, 180 + 180 * v / 10, fill=mix(BAND[band], a, bg), width=16)
    c.text((x, y - 18), f"{v:.1f}", 40, TEXT, a, "mm", "bold", bg)
    return band


def scene_examples(c, t, T):
    c.header("What the catalog says", "Same scoring rules, very different answers", t)
    cards = [("pkg:pypi/chromadb", "AI vector database", 9.0, "Open critical advisory on the version in use"),
             ("agent:claude-code", "coding agent · NOT approved", 7.0, "Runs shell commands, writes files, not on the allow-list"),
             ("agent:cursor", "coding agent · approved", 5.8, "Same powers, but governed and on the allow-list")]
    for i, (aid, kind, score, why) in enumerate(cards):
        st = T * (0.08 + i * 0.27)
        a = appear(t, st)
        x0 = 64 + i * 390
        b = [x0, 165, x0 + 365, 545]
        c.box(b, PANEL, a, 20)
        c.text((x0 + 182, 200), aid, 20, TEXT, a, "mm", "bold", PANEL)
        c.text((x0 + 182, 228), kind, 16, MUTED, a, "mm", "regular", PANEL)
        band = gauge(c, (x0 + 182, 350), score, (t - st - 0.3) / 1.5, a)
        if t > st + 1.8:
            c.pill((x0 + 182, 395), band.upper(), BAND[band], 1, 18, bg=PANEL)
        c.wrap((x0 + 182, 440), why, 17, 310, MUTED, appear(t, st + 1.0), bg=PANEL, anchor_x="m")


def scene_world(c, t, T):
    c.header("Why this matters for the world", "Companies can adopt AI safely, not blindly", t)
    cx, cy = 640, 355
    c.emoji("🌍", (cx, cy), 120, appear(t, 0.3))
    for k in range(3):
        r = 90 + ((t * 40 + k * 40) % 120)
        c.d.ellipse([cx - r, cy - r, cx + r, cy + r], outline=mix(TEAL, 0.25 * (1 - (r - 90) / 120) * appear(t, 0.3)), width=2)
    who = [("🔒", "Security teams", "see and stop risky AI tools", (250, 230)),
           ("📑", "Compliance", "audit-ready evidence for regulators", (1030, 230)),
           ("💰", "Finance", "cut overlapping, idle licences", (250, 480)),
           ("📊", "Leaders", "one clear picture of AI risk", (1030, 480))]
    for i, (em, title, body, (x, y)) in enumerate(who):
        a = appear(t, T * (0.15 + i * 0.17))
        b = [x - 185, y - 60, x + 185, y + 60]
        c.box(b, PANEL, a, 18, TEAL, 2)
        c.emoji(em, (b[0] + 42, y), 40, a)
        c.text((b[0] + 80, y - 14), title, 22, TEXT, a, "lm", "bold", PANEL)
        c.text((b[0] + 80, y + 16), body, 16, MUTED, a, "lm", "regular", PANEL)
        start = (b[2], y) if x < cx else (b[0], y)
        c.line(start, (cx + (-80 if x < cx else 80), cy + (-30 if y < cy else 30)), LINE, a, 2)


def scene_next(c, t, T):
    c.text((W / 2, 150), "Next: Phase 2 · real-time streaming", 40, TEXT, appear(t, 0.1), "mm", "bold")
    steps = [("💻", "Simulated laptops"), ("📨", "Redpanda"), ("🌊", "Flink"), ("🚦", "allow · warn · block")]
    for i, (em, s) in enumerate(steps):
        a = appear(t, 0.5 + i * 0.4)
        x = 230 + i * 275
        c.box([x - 105, 225, x + 105, 345], PANEL, a, 18, TEAL, 2)
        c.emoji(em, (x, 265), 40, a)
        c.text((x, 315), s, 18, TEXT, a, "mm", "semibold", PANEL)
        if i:
            c.arrow((x - 170, 285), (x - 105, 285), TEAL, a, appear(t, 0.7 + i * 0.4))
            if a > 0.9:
                c.packet((x - 170, 285), (x - 105, 285), t, 1.0, TEAL, n=1)
    a = appear(t, T * 0.45)
    c.emoji("🐙", (W / 2, 430), 48, a)
    c.text((W / 2, 490), "github.com/sayanah1121/AIRS-Lite-an-AI-Asset-Risk-Lakehouse", 22, TEAL, a, "mm", "mono")
    c.text((W / 2, 540), "AIRS-Lite · Flink · Spark · Iceberg · Airflow · Redpanda", 18, MUTED, appear(t, T * 0.6), "mm")


SCENES = {
    "title": scene_title, "problem": scene_problem, "why": scene_why, "analogy": scene_analogy,
    "architecture": scene_architecture, "progress": scene_progress, "phase0": scene_phase0,
    "phase1": scene_phase1, "proof_numbers": scene_proof_numbers, "proof_bugs": scene_proof_bugs,
    "examples": scene_examples, "world": scene_world, "next": scene_next,
}


# ───────────────────────────── captions, timeline, audio ─────────────────────────────

def caption_chunks(text, audio_len, max_chars=95):
    sentences = re.findall(r"[^.!?]+[.!?]?", text)
    chunks = []
    for s in (s.strip() for s in sentences if s.strip()):
        while len(s) > max_chars:
            cut = s.rfind(",", 0, max_chars)
            cut = cut if cut > 30 else s.rfind(" ", 0, max_chars)
            chunks.append(s[:cut + 1].strip())
            s = s[cut + 1:].strip()
        chunks.append(s)
    total = sum(len(c) for c in chunks)
    out, start = [], LEAD
    for ch in chunks:
        dur = audio_len * len(ch) / total
        out.append((start, start + dur, ch))
        start += dur
    return out


def draw_caption(c, chunks, t):
    for s, e, text in chunks:
        if s <= t < e + 0.15:
            tw = c.text_w(text, 22, "semibold")
            c.d.rounded_rectangle([W / 2 - tw / 2 - 18, 648, W / 2 + tw / 2 + 18, 692], radius=12, fill=(5, 9, 18))
            c.text((W / 2, 670), text, 22, TEXT, 1, "mm", "semibold", (5, 9, 18))
            return


def wav_seconds(path):
    with wave.open(path) as w:
        return w.getnframes() / w.getframerate()


def build_audio(scenes, out_path):
    """Concatenate narration clips with the per-scene padding, add a quiet synth pad, write 44.1 kHz WAV."""
    rate = 44100
    parts = []
    for sc in scenes:
        with wave.open(sc["wav"]) as w:
            data = np.frombuffer(w.readframes(w.getnframes()), np.int16).astype(np.float32) / 32768
            src_rate = w.getframerate()
        data = np.interp(np.arange(0, len(data), src_rate / rate), np.arange(len(data)), data)  # resample
        seg = np.zeros(int(sc["dur"] * rate), np.float32)
        off = int(LEAD * rate)
        seg[off:off + len(data)] = data[: len(seg) - off]
        parts.append(seg)
    voice = np.concatenate(parts)
    t = np.arange(len(voice)) / rate
    pad = sum(np.sin(2 * np.pi * f * t) for f in (110.0, 164.81, 220.0, 277.18)) / 4
    pad *= 0.035 * (0.6 + 0.4 * np.sin(2 * np.pi * t / 9))                    # slow swell
    fade = np.minimum(1, np.minimum(t / 2, (t[-1] - t) / 3))
    mixdown = np.clip(voice * 0.95 + pad * fade, -1, 1)
    with wave.open(out_path, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes((mixdown * 32767).astype(np.int16).tobytes())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "media", "airs-lite-explainer.mp4"))
    ap.add_argument("--preview", help="render PNG stills of one scene (or 'all') instead of the video")
    args = ap.parse_args()

    with open(os.path.join(HERE, "storyboard.json"), encoding="utf-8") as f:
        board = json.load(f)
    scenes = []
    for sc in board:
        wav = os.path.join(AUDIO_DIR, f"{sc['id']}.wav")
        alen = wav_seconds(wav)
        dur = max(sc["min_seconds"], LEAD + alen + TAIL)
        scenes.append({**sc, "wav": wav, "alen": alen, "dur": dur,
                       "captions": caption_chunks(sc["narration"], alen)})
    total = sum(s["dur"] for s in scenes)
    print(f"{len(scenes)} scenes, {total:.1f} s")

    base = make_background()

    def frame(sc_idx, t):
        sc = scenes[sc_idx]
        img = base.copy()
        c = Canvas(img)
        SCENES[sc["id"]](c, t, sc["dur"])
        c.text((W - 40, 34), "AIRS-Lite", 16, MUTED, 1, "ra", "semibold")
        for i in range(len(scenes)):                      # scene progress dots
            x = W - 40 - (len(scenes) - 1 - i) * 14
            col = TEAL if i <= sc_idx else LINE
            c.d.ellipse([x - 3, 62, x + 3, 68], fill=col)
        draw_caption(c, sc["captions"], t)
        fa = min(1, t / FADE, (sc["dur"] - t) / FADE)
        if fa < 1:
            img = Image.blend(base, img, max(0.0, fa))
        return img

    if args.preview:
        out_dir = os.path.join(HERE, "build", "preview")
        os.makedirs(out_dir, exist_ok=True)
        for i, sc in enumerate(scenes):
            if args.preview not in ("all", sc["id"]):
                continue
            frame(i, sc["dur"] * 0.92).save(os.path.join(out_dir, f"{i:02d}_{sc['id']}.png"))
        print(f"previews in {out_dir}")
        return

    import imageio_ffmpeg
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    audio_path = os.path.join(HERE, "build", "narration_mix.wav")
    build_audio(scenes, audio_path)
    cmd = [ffmpeg, "-y", "-loglevel", "error",
           "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{W}x{H}", "-r", str(FPS), "-i", "-",
           "-i", audio_path,
           "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p",
           "-c:a", "aac", "-b:a", "160k", "-shortest", "-movflags", "+faststart", args.out]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    n = 0
    for i, sc in enumerate(scenes):
        frames = int(round(sc["dur"] * FPS))
        for f in range(frames):
            proc.stdin.write(frame(i, f / FPS).tobytes())
            n += 1
        print(f"  rendered {sc['id']:<14} {frames:5d} frames", flush=True)
    proc.stdin.close()
    proc.wait()
    print(f"wrote {args.out} ({n} frames, {n / FPS:.1f} s)")


if __name__ == "__main__":
    main()
