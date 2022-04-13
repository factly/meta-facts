from pathlib import Path


async def get_formats_available(output_file_path: str):
    file_path = Path(output_file_path).suffix.strip(".")
    return {"formats_available": file_path}
