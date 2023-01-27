import os
import logging
from typing import Union, Tuple
import subprocess
from urllib.parse import unquote

import boto3
import botocore

RUNNING_IN_LAMBDA = os.environ.get("LAMBDA_TASK_ROOT")
if RUNNING_IN_LAMBDA:
    FFMPEG_DIR = "/opt/bin/ffmpeg"
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

tasks_types = ["render", "segment", "obj_detect"]


def handler(event, context) -> dict:
    logger.debug("videocloud_ffmpeg called: %s", event)
    logger.debug("running ffmpeg version: %s", get_ffmpeg_version())

    filename = unquote(event.get("filename", ""))
    tasks = event.get("tasks", None)

    logger.info("filename: %s, tasks: %s", filename, tasks)

    s3_bucket = S3_SOURCE_BUCKET
    s3_key = S3_SOURCE_PATH + filename
    rendered_s3_key = S3_RENDERED_PATH + filename

    assert s3_key != S3_SOURCE_PATH, "filename is required"
    assert filename, "filename is required"
    for task in tasks:
        assert task, "task is required"
        assert task in tasks_types, f"task must be one of: {tasks_types}"

    logger.info("bucket: %s, key: %s, task: %s", s3_bucket, s3_key, task)

    # Set video path in temporary directory
    local_video_path = f"{TEMP_DIR}/{filename}"
    rendered_file_path = f"{TEMP_DIR}/{filename}_rendered.mp4"

    check_available_space(s3_bucket, s3_key)

    logger.info("downloading video from key: %s", s3_key)
    if not download_video(s3_bucket, s3_key, local_video_path):
        raise Exception("download failed, file may not exist")

    logger.info("rendering video")
    if not render_video(local_video_path, rendered_file_path):
        raise Exception("rendering failed")

    logger.info("uploading rendered video to key: %s", rendered_s3_key)
    if not upload_video(s3_bucket, rendered_s3_key, rendered_file_path):
        raise Exception("upload failed")

    # No longer need source file
    if not clean_up_file(local_video_path):
        logger.info("failed to remove source file: %s", local_video_path)
    if not clean_up_file(rendered_file_path):
        logger.info("failed to remove source file: %s", rendered_file_path)

    logger.info("rendering complete")
    return {"data": "success"}


def download_video(s3_bucket: str, s3_key: str, file_path: str) -> bool:
    success = True
    try:
        s3.download_file(s3_bucket, s3_key, file_path)
    except Exception as err:
        logger.error(err)
        success = False
    return success


def render_video(file_path: str, rendered_file: str) -> bool:
    success = True

    if not os.path.isfile(file_path):
        logger.error("video file not downloaded")
        raise Exception("video file not downloaded")

    ffmpeg_command = [
        FFMPEG_DIR,
        "-flags2",
        "+export_mvs",
        "-i",
        file_path,
        "-vf",
        str(
            r"split[src],codecview=mv=pf+bf+bb[vex],[vex][src]blend=all_mode=difference128,eq=contrast=7:brightness=-1:gamma=1.5"
        ),
        "-c:v",
        "libx264",
        rendered_file,
    ]

    pipe = subprocess.Popen(
        ffmpeg_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    out, err = pipe.communicate()
    if err:
        logger.error(err)
        success = False
        return success

    out = out.decode("utf-8")
    logger.debug("ffmpeg return:", out)

    return success


def upload_video(s3_bucket: str, s3_key: str, file_path: str) -> bool:
    success = True
    try:
        s3.upload_file(file_path, s3_bucket, s3_key)
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


def clean_up_file(path: str) -> bool:
    success = True
    if path and os.path.exists(path):
        logger.debug("Removing: %s", path)
        try:
            os.remove(path)
        except:
            success = False
            pass
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
