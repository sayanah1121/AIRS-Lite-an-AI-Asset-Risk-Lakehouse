# Explainer video

An animated, narrated walkthrough of AIRS-Lite: the problem, the architecture, progress so far, the proof, and why it matters.

Every frame is drawn with Pillow (no video editor), narrated offline with the Windows `System.Speech` voice, and encoded with ffmpeg.
To refresh the video after a new phase, edit `storyboard.json` (narration) and the scene functions in `make_video.py`, then re-run the two steps below.

```bash
powershell -File tools/explainer-video/narrate.ps1
```
```bash
python tools/explainer-video/make_video.py
```

- Output: `media/airs-lite-explainer.mp4` (1280×720, 30 fps, H.264 + AAC)
- `--preview all` writes one PNG still per scene to `build/preview/` for a quick layout check
- Requirements: `pip install pillow numpy imageio-ffmpeg` (imageio-ffmpeg bundles the ffmpeg binary)
