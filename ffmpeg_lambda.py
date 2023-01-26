import os
import logging
from typing import Union, Tuple
import subprocess

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


def handler(event, context) -> dict:
    logger.debug("videocloud_ffmpeg called: %s", event)
    logger.debug("running ffmpeg version: %s", get_ffmpeg_version())

    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_key = event["Records"][0]["s3"]["object"]["key"]

    assert s3_bucket, "bucket not found"
    assert s3_key, "key not found"

    # Get the file name only
    _, source_video_file_name = os.path.split(s3_key)

    # Set video path in temporary directory
    local_video_path = f"{TEMP_DIR}/{source_video_file_name}"
    rendered_file_path = f"{TEMP_DIR}/rendered_video.mp4"

    check_available_space(s3_bucket, s3_key)

    if not download_video(s3_bucket, s3_key, local_video_path):
        raise Exception("download failed")

    if not render_video(local_video_path, rendered_file_path):
        raise Exception("rendering failed")

    if not upload_video(s3_bucket, s3_key, rendered_file_path):
        raise Exception("upload failed")

    # No longer need source file
    if not clean_up_file(local_video_path):
        logger.info("failed to remove source file: %s", local_video_path)
    if not clean_up_file(rendered_file_path):
        logger.info("failed to remove source file: %s", rendered_file_path)

    return {"data": "success"}


def download_video(s3_bucket: str, s3_key: str, file_path: str) -> bool:
    success = True
    try:
        s3.download_file(s3_bucket, s3_key, file_path)
    except Exception as err:
        logger.error(err)
        success = False
    return success


def upload_video(s3_bucket: str, s3_key: str, file_path: str) -> bool:
    success = True
    try:
        s3.upload_file(file_path, s3_bucket, s3_key)
    except Exception as err:
        logger.error(err)
        success = False
    return success


def render_video(file_path: str, rendered_file: str) -> bool:
    success = True

    if not os.path.isfile(file_path):
        raise Exception("video file not downloaded")

    args = [
        FFMPEG_DIR,
        "-loglevel",
        "quiet",
        "-y",
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

    proc = subprocess.Popen(
        args,
        cwd=TEMP_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    out, err = proc.communicate()
    if err:
        logger.error(err)
        success = False
        return success

    out = out.decode("utf-8")
    logger.debug(out)

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
