from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "demo-site" / "assets"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WIDTH, HEIGHT = 1200, 720


def _primary_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try:
        return ImageFont.truetype("DejaVuSans-Bold.ttf", size=size)
    except OSError:
        return ImageFont.load_default()


def _draw_card(title: str, subtitle: str, accent: str, background: str) -> Image.Image:
    img = Image.new("RGB", (WIDTH, HEIGHT), background)
    draw = ImageDraw.Draw(img)
    font_large = _primary_font(72)
    font_body = _primary_font(38)

    draw.rectangle([(60, 60), (WIDTH - 60, HEIGHT - 60)], outline=accent, width=10)
    draw.text((100, 120), title, font=font_large, fill=accent)
    draw.text((100, 240), subtitle, font=font_body, fill="#ffffff")
    draw.text((100, 320), "Data source: canonical bundle", font=font_body, fill="#ffffff")
    draw.text((100, 380), "Validation layer: Great Expectations", font=font_body, fill="#ffffff")
    return img


def create_screenshots() -> None:
    pass_img = _draw_card(
        "Validation Success",
        "Score, schema, and certification rules all green",
        "#2f855a",
        "#0f172a",
    )
    fail_img = _draw_card(
        "Validation Failure",
        "Freshness exceeded the 24-hour threshold",
        "#c53030",
        "#0f172a",
    )
    pass_path = OUTPUT_DIR / "validation-pass.png"
    fail_path = OUTPUT_DIR / "validation-fail.png"
    pass_img.save(pass_path, optimize=True)
    fail_img.save(fail_path, optimize=True)


def create_story_gif() -> None:
    frames: list[Image.Image] = []
    states = [
        ("Bad dataset", "Missing candidate join + duplicates", "#c53030"),
        ("Failed release", "Release decision: Blocked", "#dd6b20"),
        ("Corrected dataset", "Fresh batch arrives", "#3182ce"),
        ("Ready release", "Release decision: Ready", "#2f855a"),
    ]
    font_large = _primary_font(64)
    font_small = _primary_font(36)

    for stage, detail, color in states:
        frame = Image.new("RGB", (WIDTH, HEIGHT), "#020617")
        draw = ImageDraw.Draw(frame)
        draw.rectangle([(80, 80), (WIDTH - 80, HEIGHT - 80)], outline=color, width=8)
        draw.text((120, 160), stage, font=font_large, fill=color)
        draw.text((120, 280), detail, font=font_small, fill="#e2e8f0")
        draw.text((120, 360), "Automated CI + GX checkpoint", font=font_small, fill="#94a3b8")
        frames.append(frame)

    gif_path = OUTPUT_DIR / "workflow.gif"
    frames[0].save(
        gif_path,
        save_all=True,
        append_images=frames[1:],
        duration=1600,
        loop=0,
        optimize=True,
    )


if __name__ == "__main__":
    create_screenshots()
    create_story_gif()
