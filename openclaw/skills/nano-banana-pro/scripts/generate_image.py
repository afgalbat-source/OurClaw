#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "google-genai>=1.0.0",
#     "pillow>=10.0.0",
#     "requests>=2.31.0",
# ]
# ///
"""
Generate images using Google's Nano Banana Pro (Gemini 3 Pro Image) API or custom API.

Usage:
    # 官方 Google API (默认)
    uv run generate_image.py --prompt "图片描述" --filename "output.png" [--resolution 1K|2K|4K] [--api-key KEY]
    
    # 自定义 API
    uv run generate_image.py --prompt "图片描述" --filename "output.png" --custom-api [--provider openai|stability|custom] [--api-key KEY]
"""

import argparse
import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional


def get_api_config(args) -> Dict[str, Any]:
    """获取 API 配置 - 支持官方和自定义 API"""
    # 如果使用自定义 API
    if args.custom_api:
        config = {
            "use_custom": True,
            "base_url": args.custom_base_url or os.environ.get("CUSTOM_API_BASE_URL", ""),
            "endpoint": args.custom_endpoint or os.environ.get("CUSTOM_API_ENDPOINT", "/images/generations"),
            "headers": {},
            "provider": args.provider or os.environ.get("CUSTOM_API_PROVIDER", "openai").lower(),
            "api_key": args.api_key or os.environ.get("CUSTOM_API_KEY", ""),
            "timeout": int(os.environ.get("CUSTOM_API_TIMEOUT", "60"))
        }
        
        # 设置 headers
        auth_header = os.environ.get("CUSTOM_API_AUTH_HEADER", "")
        if not auth_header and config["api_key"]:
            auth_header = f"Bearer {config['api_key']}"
        
        config["headers"] = {
            "Authorization": auth_header,
            "Content-Type": "application/json"
        }
        
        return config
    
    # 使用官方 Google API
    else:
        api_key = args.api_key or os.environ.get("GEMINI_API_KEY")
        return {
            "use_custom": False,
            "api_key": api_key
        }


def build_custom_request(provider: str, prompt: str, resolution: str) -> Dict[str, Any]:
    """构建自定义 API 请求体"""
    size_map = {
        "1K": "1024x1024",
        "2K": "1792x1024",
        "4K": "2048x2048"
    }
    image_size = size_map.get(resolution, "1024x1024")
    
    if provider == "openai":
        return {
            "model": os.environ.get("CUSTOM_API_MODEL", "dall-e-3"),
            "prompt": prompt,
            "n": 1,
            "size": image_size,
            "response_format": "url"
        }
    elif provider == "stability":
        width, height = map(int, image_size.split('x'))
        return {
            "text_prompts": [{"text": prompt, "weight": 1}],
            "cfg_scale": 7,
            "width": width,
            "height": height,
            "steps": 30,
            "samples": 1
        }
    else:
        custom_json = os.environ.get("CUSTOM_API_REQUEST_BODY", "")
        if custom_json:
            try:
                payload = json.loads(custom_json)
                if isinstance(payload, dict):
                    payload_str = json.dumps(payload)
                    payload_str = payload_str.replace("${PROMPT}", prompt)
                    payload_str = payload_str.replace("${RESOLUTION}", resolution)
                    payload_str = payload_str.replace("${SIZE}", image_size)
                    return json.loads(payload_str)
            except json.JSONDecodeError:
                pass
        return {"prompt": prompt, "size": image_size}


def extract_custom_image(response: Dict[str, Any], provider: str) -> Optional[str]:
    """从自定义 API 响应提取图片 URL/base64"""
    if provider == "openai":
        if "data" in response and len(response["data"]) > 0:
            return response["data"][0].get("url") or response["data"][0].get("b64_json")
    elif provider == "stability":
        if "artifacts" in response and len(response["artifacts"]) > 0:
            return response["artifacts"][0].get("base64")
    else:
        url_path = os.environ.get("CUSTOM_API_RESPONSE_PATH", "data[0].url")
        parts = url_path.split(".")
        value = response
        for part in parts:
            if "[" in part and "]" in part:
                key = part.split("[")[0]
                index = int(part.split("[")[1].split("]")[0])
                value = value[key][index]
            else:
                value = value.get(part)
        return value
    return None


def save_image(image_data: bytes, output_path: Path) -> None:
    """保存图片为 PNG 格式"""
    from PIL import Image
    from io import BytesIO
    
    image = Image.open(BytesIO(image_data))
    
    if image.mode == 'RGBA':
        rgb_image = Image.new('RGB', image.size, (255, 255, 255))
        mask = image.split()[3] if len(image.split()) > 3 else None
        rgb_image.paste(image, mask=mask)
        rgb_image.save(str(output_path), 'PNG')
    elif image.mode == 'RGB':
        image.save(str(output_path), 'PNG')
    else:
        image.convert('RGB').save(str(output_path), 'PNG')


def generate_google_image(config: Dict, prompt: str, filename: str, resolution: str, input_image_path: Optional[str]):
    """使用 Google API 生成图片"""
    from google import genai
    from google.genai import types
    from PIL import Image as PILImage
    
    client = genai.Client(api_key=config["api_key"])
    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    input_image = None
    output_resolution = resolution
    if input_image_path:
        try:
            input_image = PILImage.open(input_image_path)
            print(f"Loaded input image: {input_image_path}")
            
            if resolution == "1K":
                width, height = input_image.size
                max_dim = max(width, height)
                if max_dim >= 3000:
                    output_resolution = "4K"
                elif max_dim >= 1500:
                    output_resolution = "2K"
                else:
                    output_resolution = "1K"
                print(f"Auto-detected resolution: {output_resolution} (from input {width}x{height})")
        except Exception as e:
            print(f"Error loading input image: {e}", file=sys.stderr)
            sys.exit(1)
    
    if input_image:
        contents = [input_image, prompt]
        print(f"Editing image with resolution {output_resolution}...")
    else:
        contents = prompt
        print(f"Generating image with resolution {output_resolution}...")
    
    try:
        response = client.models.generate_content(
            model="gemini-3-pro-image-preview",
            contents=contents,
            config=types.GenerateContentConfig(
                response_modalities=["TEXT", "IMAGE"],
                image_config=types.ImageConfig(
                    image_size=output_resolution
                )
            )
        )
        
        image_saved = False
        for part in response.parts:
            if part.text is not None:
                print(f"Model response: {part.text}")
            elif part.inline_data is not None:
                image_data = part.inline_data.data
                if isinstance(image_data, str):
                    import base64
                    image_data = base64.b64decode(image_data)
                save_image(image_data, output_path)
                image_saved = True
        
        if image_saved:
            full_path = output_path.resolve()
            print(f"\nImage saved: {full_path}")
        else:
            print("Error: No image was generated in the response.", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error generating image: {e}", file=sys.stderr)
        sys.exit(1)


def generate_custom_image(config: Dict, prompt: str, filename: str, resolution: str):
    """使用自定义 API 生成图片"""
    import requests
    
    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    full_url = f"{config['base_url']}{config['endpoint']}"
    payload = build_custom_request(config["provider"], prompt, resolution)
    
    print(f"Using provider: {config['provider']}")
    print(f"Request URL: {full_url}")
    print(f"Generating image with resolution {resolution}...")
    
    try:
        response = requests.post(
            full_url,
            headers=config["headers"],
            json=payload,
            timeout=config["timeout"]
        )
        response.raise_for_status()
        
        result = response.json()
        image_data = extract_custom_image(result, config["provider"])
        
        if not image_data:
            print("Error: No image data in API response", file=sys.stderr)
            sys.exit(1)
        
        if image_data.startswith("http"):
            print(f"Downloading image: {image_data}")
            img_response = requests.get(image_data, timeout=config["timeout"])
            img_response.raise_for_status()
            image_bytes = img_response.content
        else:
            import base64
            image_bytes = base64.b64decode(image_data)
        
        save_image(image_bytes, output_path)
        full_path = output_path.resolve()
        print(f"\nImage saved: {full_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error: API request failed - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Generate images using Google Gemini or custom API"
    )
    parser.add_argument(
        "--prompt", "-p",
        required=True,
        help="Image description/prompt"
    )
    parser.add_argument(
        "--filename", "-f",
        required=True,
        help="Output filename (e.g., sunset-mountains.png)"
    )
    parser.add_argument(
        "--input-image", "-i",
        help="Optional input image path for editing/modification (Google API only)"
    )
    parser.add_argument(
        "--resolution", "-r",
        choices=["1K", "2K", "4K"],
        default="1K",
        help="Output resolution: 1K (default), 2K, or 4K"
    )
    parser.add_argument(
        "--api-key", "-k",
        help="API key (Gemini or custom API)"
    )
    
    # 自定义 API 选项
    parser.add_argument(
        "--custom-api", "-C",
        action="store_true",
        help="Use custom API instead of Google Gemini"
    )
    parser.add_argument(
        "--provider", "-P",
        choices=["openai", "stability", "custom"],
        default="openai",
        help="Custom API provider (default: openai)"
    )
    parser.add_argument(
        "--custom-base-url",
        help="Custom API base URL"
    )
    parser.add_argument(
        "--custom-endpoint",
        help="Custom API endpoint path"
    )

    args = parser.parse_args()
    config = get_api_config(args)
    
    # 验证配置
    if not config["use_custom"] and not config["api_key"]:
        print("Error: No API key provided.", file=sys.stderr)
        print("Please either:", file=sys.stderr)
        print("  1. Provide --api-key argument", file=sys.stderr)
        print("  2. Set GEMINI_API_KEY environment variable", file=sys.stderr)
        print("  3. Or use --custom-api for custom API", file=sys.stderr)
        sys.exit(1)
    
    if config["use_custom"]:
        if not config["base_url"]:
            print("Error: Custom API requires base URL", file=sys.stderr)
            print("Set CUSTOM_API_BASE_URL or use --custom-base-url", file=sys.stderr)
            sys.exit(1)
        if not config["headers"]["Authorization"]:
            print("Error: Custom API requires authorization", file=sys.stderr)
            print("Set CUSTOM_API_KEY or CUSTOM_API_AUTH_HEADER or use --api-key", file=sys.stderr)
            sys.exit(1)
    
    # 选择 API
    if config["use_custom"]:
        generate_custom_image(config, args.prompt, args.filename, args.resolution)
    else:
        if args.input_image:
            generate_google_image(config, args.prompt, args.filename, args.resolution, args.input_image)
        else:
            generate_google_image(config, args.prompt, args.filename, args.resolution, None)


if __name__ == "__main__":
    main()