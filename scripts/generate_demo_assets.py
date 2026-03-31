from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "demo-site" / "assets"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WIDTH, HEIGHT = 1280, 760
BACKGROUND = "#f4efe6"
TEXT = "#11261f"
MUTED = "#49635d"


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    names = ["DejaVuSans-Bold.ttf", "DejaVuSans.ttf"] if bold else ["DejaVuSans.ttf"]
    for name in names:
        try:
            return ImageFont.truetype(name, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _draw_card(title: str, subtitle: str, accent: str, eyebrow: str) -> Image.Image:
    img = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND)
    draw = ImageDraw.Draw(img)
    display = _font(78, bold=True)
    body = _font(34)
    small = _font(26)

    draw.rounded_rectangle([(56, 56), (WIDTH - 56, HEIGHT - 56)], radius=34, fill="#fffaf2", outline=accent, width=8)
    draw.text((108, 118), eyebrow.upper(), font=small, fill=MUTED)
    draw.text((108, 188), title, font=display, fill=TEXT)
    draw.text((108, 320), subtitle, font=body, fill=accent)
    draw.text((108, 410), "Bundle: CRM integrity gate demo", font=body, fill=TEXT)
    draw.text((108, 470), "Artifacts: release decision, weekly summary, ops insights", font=body, fill=TEXT)
    draw.text((108, 530), "Surface: GitHub Pages + Great Expectations Data Docs", font=body, fill=TEXT)
    return img


def create_screenshots() -> None:
    pass_img = _draw_card(
        "CRM Sync Ready",
        "Clean ownership, valid joins, recent touches, and healthy pipeline hygiene",
        "#157a6e",
        "after cleanup",
    )
    fail_img = _draw_card(
        "CRM Sync Blocked",
        "Duplicate identities, broken joins, and stale pipeline defects detected",
        "#c44536",
        "before cleanup",
    )
    pass_img.save(OUTPUT_DIR / "validation-pass.png", optimize=True)
    fail_img.save(OUTPUT_DIR / "validation-fail.png", optimize=True)


def create_story_gif() -> None:
    frames: list[Image.Image] = []
    states = [
        ("Before cleanup", "Duplicate email + broken joins + inactive owner", "#c44536"),
        ("Blocked sync", "Release decision: Blocked", "#d47f37"),
        ("After cleanup", "Routing corrected and pipeline hygiene restored", "#2b7a78"),
        ("Ready to sync", "Release decision: Ready", "#157a6e"),
    ]
    title_font = _font(62, bold=True)
    body_font = _font(32)

    for stage, detail, color in states:
        frame = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND)
        draw = ImageDraw.Draw(frame)
        draw.rounded_rectangle([(80, 80), (WIDTH - 80, HEIGHT - 80)], radius=30, fill="#fffaf2", outline=color, width=8)
        draw.text((124, 172), stage, font=title_font, fill=color)
        draw.text((124, 298), detail, font=body_font, fill=TEXT)
        draw.text((124, 378), "DuckDB rollups + JSON artifacts + static dashboard", font=body_font, fill=MUTED)
        frames.append(frame)

    frames[0].save(
        OUTPUT_DIR / "workflow.gif",
        save_all=True,
        append_images=frames[1:],
        duration=1500,
        loop=0,
        optimize=True,
    )


if __name__ == "__main__":
    create_screenshots()
    create_story_gif()
