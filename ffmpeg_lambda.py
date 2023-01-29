import os
import logging
from typing import Union, Tuple
import subprocess
from urllib.parse import unquote
from pathlib import Path
import shutil

import boto3
import botocore

RUNNING_IN_LAMBDA = os.environ.get("LAMBDA_TASK_ROOT")
if RUNNING_IN_LAMBDA:
    FFMPEG_DIR = "/opt/ffmpeg"
    TEMP_DIR = "/tmp"
else:
    FFMPEG_DIR = "/opt/homebrew/bin/ffmpeg"
    TEMP_DIR = "/tmp"

logger = logging.getLogger("cloudvideo_ffmpeg")
logger.setLevel(logging.DEBUG)

s3 = boto3.client("s3")

S3_SOURCE_BUCKET = "videocloud-s3"
S3_SOURCE_PATH = "uploads/"
S3_RENDERED_PATH = "rendered/"


def get_ffmpeg_command(
    task: str, file_path: str = "", rendered_file_path: str = ""
) -> Tuple[Union[list, None], list]:

    command_header = [
        FFMPEG_DIR,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-i",
        file_path,
    ]

    command_footer = [
        "-profile:v",
        "baseline",
        "-y",
        rendered_file_path,
    ]

    ffmpeg_tasks = {
        "h264_mp4_light": [
            *command_header,
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-preset",
            "veryfast",
            "-crf",
            "26",
            *command_footer,
        ],
        "h264_mp4_medium": [
            *command_header,
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-preset",
            "medium",
            "-crf",
            "22",
            *command_footer,
        ],
    }

    command = []
    if task not in ffmpeg_tasks.keys():
        command = None
    else:
        command = ffmpeg_tasks[task]
    task_types = ffmpeg_tasks.keys()

    return command, task_types


def download_video(s3_bucket: str, s3_key: str, file_path: str) -> bool:
    success = True
    try:
        s3.download_file(Bucket=s3_bucket, Key=s3_key, Filename=file_path)
        logger.info("download complete")
        logger.info("file size of downloaded video: %s", os.path.getsize(file_path))
    except Exception as err:
        logger.error(err)
        success = False
    return success


def render_video(file_path: str, rendered_file_path: str) -> bool:
    success = True

    if not os.path.isfile(file_path):
        logger.error("video file not downloaded")
        raise Exception("video file not downloaded")

    logger.info("rendering video to path: %s", str(rendered_file_path))

    ffmpeg_command = get_ffmpeg_command("h264_mp4_light", file_path, rendered_file_path)

    res = subprocess.run(ffmpeg_command, capture_output=True)

    logger.info(f"response: {res}")

    logger.info(f"rendered video size is {os.path.getsize(rendered_file_path)}")

    return success


def upload_video(s3_bucket: str, rendered_s3_key: str, rendered_file_path: str) -> bool:
    success = True

    logger.info(
        f"uploading rendered video, {rendered_file_path} to key: {rendered_s3_key}"
    )

    logger.info(
        f"before upload, rendered video size is {os.path.getsize(rendered_file_path)}"
    )

    try:
        s3.upload_file(rendered_file_path, s3_bucket, rendered_s3_key)
        logger.info("upload complete")
    except Exception as err:
        logger.error(err)
        success = False
    return success


def check_available_space(s3_bucket: str, s3_key: str) -> None:
    available_bytes, _ = get_available_space(TEMP_DIR)
    object_bytes = get_obj_file_size(s3_bucket, s3_key)
    if object_bytes < 0:
        logger.debug(
            "key not found, bucket: %s, key: %s",
            s3_bucket,
            s3_key,
        )
        raise Exception("key not found in bucket")
    elif object_bytes > available_bytes:
        logger.debug("Out of space")
        raise Exception("Out of space")


def get_obj_file_size(bucket: str, key: str) -> Union[int, None]:
    object_size = None
    try:
        res = s3.head_object(Bucket=bucket, Key=key)
        object_size = res.get("ContentLength")
    except botocore.exceptions.ClientError as err:
        logger.debug(err)
    return object_size


def clean_up_folder(folder_path: str) -> bool:
    success = True
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))
            success = False

    return success


def get_available_space(path: str) -> Tuple[int, int]:
    statvfs = os.statvfs(path)
    user_bytes = statvfs.f_frsize * statvfs.f_bavail
    free_bytes = statvfs.f_frsize * statvfs.f_bfree
    return (user_bytes, free_bytes)


def get_ffmpeg_version() -> str:
    ffmpeg_version = "unknown"
    proc = subprocess.Popen(
        [FFMPEG_DIR, "-version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, _ = proc.communicate()
    stdout = out.decode("utf-8")
    if stdout.startswith("ffmpeg version"):
        pos = stdout.find(" ", 15)
        if pos > -1:
            ffmpeg_version = stdout[15:pos]
    return ffmpeg_version


def handler(event, context) -> dict:
    logger.debug("videocloud_ffmpeg called: %s", event)
    logger.debug("running ffmpeg version: %s", get_ffmpeg_version())

    filename = unquote(event.get("filename", ""))
    tasks = event.get("tasks", None)

    logger.info("filename: %s, tasks: %s", filename, tasks)

    s3_bucket = S3_SOURCE_BUCKET
    s3_key = S3_SOURCE_PATH + filename
    rendered_filename = f"{str(Path(filename).stem)}_rendered.mp4"
    rendered_s3_key = S3_RENDERED_PATH + rendered_filename

    assert s3_key != S3_SOURCE_PATH, "filename is required"
    assert filename, "filename is required"
    assert tasks, "task(s) are required"
    _, task_types = get_ffmpeg_command()
    for task in tasks:
        assert task in task_types, f"task must be one of: {task_types}"

    logger.info("bucket: %s, key: %s, task: %s", s3_bucket, s3_key, task)

    # Set video path in temporary directory
    local_video_path = f"{TEMP_DIR}/{filename}"
    rendered_file_path = f"{TEMP_DIR}/{rendered_filename}"

    # Clean tmp folder before starting
    subprocess.call(f"rm -rf /tmp/*", shell=True)

    check_available_space(s3_bucket, s3_key)

    # Downloads video from s3 into temporary directory
    logger.info("downloading video from key: %s", s3_key)
    if not download_video(s3_bucket, s3_key, local_video_path):
        raise Exception("download failed, file may not exist")

    # Renders the video into temporary temp/rendered/ directory
    logger.info("rendering video")
    if not render_video(local_video_path, rendered_file_path):
        raise Exception("rendering failed")

    # Uploads rendered video to /rendered/ directory in s3
    logger.info("uploading rendered video to key: %s", rendered_s3_key)
    if not upload_video(s3_bucket, rendered_s3_key, rendered_file_path):
        raise Exception("upload failed")

    # No longer need source files or rendered files
    if not clean_up_folder(folder_path=TEMP_DIR):
        logger.info("failed to clean folder: %s", TEMP_DIR)

    logger.info("rendering complete")
    return {"data": "success"}
