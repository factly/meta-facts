from pathlib import Path


async def get_output_file_name(full_file_path: str):
    home_user = str(Path(full_file_path).home())
    output_file_name = full_file_path.split("/projects/")[-1].split(home_user)[
        -1
    ]
    return {"output_file_name": output_file_name}
