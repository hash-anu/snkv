"""
Build script for the snkv Python extension.

Before compiling the C extension this script:
  1. Runs `make snkv.h` in the repo root to regenerate the amalgamated header.
  2. Copies the freshly built snkv.h into this directory so the compiler
     finds it alongside snkv_module.c.

Run from the repo root:
    pip install -e python/
or from this directory:
    python setup.py build_ext --inplace
"""

import os
import shutil
import subprocess
import sys
import urllib.request
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Absolute path of the repository root (one level above this file).
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HERE      = os.path.abspath(os.path.dirname(__file__))


class BuildExtWithHeader(_build_ext):
    """Custom build_ext that regenerates snkv.h before compiling."""

    def run(self) -> None:
        self._regenerate_snkv_header()
        # Ensure the package directory exists so the inplace copy succeeds
        # on fresh clones where snkv/ may not yet contain any built artifact.
        os.makedirs(os.path.join(HERE, "snkv"), exist_ok=True)
        super().run()

    def build_extension(self, ext: Extension) -> None:
        self._regenerate_snkv_header()
        super().build_extension(ext)

    def _regenerate_snkv_header(self) -> None:
        target = os.path.join(HERE, "snkv.h")

        # Only regenerate once per build run.
        if getattr(self, "_snkv_h_ready", False):
            return

        print("-- snkv: running `make snkv.h` in repo root...")
        try:
            # In MSYS2 (MinGW64/UCRT64) shells, MSYSTEM is set and plain
            # 'make' is on PATH.  In a bare native-Windows MinGW installation
            # the command is 'mingw32-make'.
            if sys.platform == "win32":
                make_cmd = "make" if os.environ.get("MSYSTEM") else "mingw32-make"
            else:
                make_cmd = "make"
            result = subprocess.run(
                [make_cmd, "snkv.h"],
                cwd=REPO_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            if result.stdout:
                print(result.stdout, end="")
        except FileNotFoundError:
            # make not found — fall back to existing file, or download from GitHub
            if os.path.exists(target):
                print(f"-- snkv: `make` not found; using existing {target}")
                self._snkv_h_ready = True
                return
            print("-- snkv: `make` not found and no local snkv.h; downloading from GitHub...")
            self._download_snkv_header(target)
            self._snkv_h_ready = True
            return
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"`make snkv.h` failed (exit {e.returncode}):\n{e.stderr}"
            ) from e

        # Copy the generated header from repo root into python/
        src = os.path.join(REPO_ROOT, "snkv.h")
        if not os.path.exists(src):
            raise FileNotFoundError(
                f"`make snkv.h` succeeded but {src} was not found."
            )
        shutil.copy2(src, target)
        print(f"-- snkv: copied {src} -> {target}")
        self._snkv_h_ready = True

    def _download_snkv_header(self, target: str) -> None:
        import json
        import tempfile
        import zipfile
        from urllib.error import HTTPError

        headers = {"Accept": "application/vnd.github+json"}

        # 1a. Try GitHub Releases API first (works once releases are published).
        tag = None
        release_zip_ok = False
        api_url = "https://api.github.com/repos/hash-anu/snkv/releases/latest"
        try:
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req) as resp:
                tag = json.loads(resp.read())["tag_name"]   # e.g. "v0.1.3"
        except HTTPError as exc:
            if exc.code == 404:
                pass  # No releases published yet — fall through to tags API
            else:
                raise RuntimeError(
                    f"GitHub API error ({api_url}): {exc}\n"
                    "Either install make (Linux/macOS) or ensure internet access."
                ) from exc

        # 1b. If no releases, fall back to the latest git tag.
        if tag is None:
            tags_url = "https://api.github.com/repos/hash-anu/snkv/tags"
            try:
                req = urllib.request.Request(tags_url, headers=headers)
                with urllib.request.urlopen(req) as resp:
                    tags = json.loads(resp.read())
                if not tags:
                    raise RuntimeError("No tags found in the snkv repository.")
                tag = tags[0]["name"]   # most-recent tag first
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to fetch tags from GitHub ({tags_url}):\n{exc}\n"
                    "Either install make (Linux/macOS) or ensure internet access."
                ) from exc

        version = tag.lstrip("v")   # "0.1.3"

        # 2a. Try the release asset zip (requires a published GitHub Release).
        zip_url = (
            f"https://github.com/hash-anu/snkv/releases/download/"
            f"{tag}/snkv-{version}.zip"
        )
        print(f"-- snkv: downloading {zip_url} ...")
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, f"snkv-{version}.zip")
            try:
                urllib.request.urlretrieve(zip_url, zip_path)
                release_zip_ok = True
            except HTTPError as exc:
                if exc.code == 404:
                    release_zip_ok = False  # asset not uploaded yet
                else:
                    raise RuntimeError(
                        f"Failed to download release zip ({zip_url}): {exc}\n"
                        "Either install make (Linux/macOS) or ensure internet access."
                    ) from exc

            if release_zip_ok:
                # Extract snkv.h from release/snkv-{version}/include/
                inner_path = f"snkv-{version}/include/snkv.h"
                with zipfile.ZipFile(zip_path) as zf:
                    if inner_path not in zf.namelist():
                        raise RuntimeError(
                            f"Expected '{inner_path}' inside {zip_url} but it was not found.\n"
                            f"Available entries: {zf.namelist()[:10]}"
                        )
                    with zf.open(inner_path) as src_f:
                        with open(target, "wb") as dst_f:
                            dst_f.write(src_f.read())
                print(f"-- snkv: extracted snkv.h {tag} -> {target}")
                return

        # 2b. Release asset not available — download raw snkv.h from the tag.
        raw_url = (
            f"https://raw.githubusercontent.com/hash-anu/snkv/{tag}/snkv.h"
        )
        print(f"-- snkv: release asset not found; downloading raw {raw_url} ...")
        try:
            urllib.request.urlretrieve(raw_url, target)
            print(f"-- snkv: downloaded snkv.h {tag} from GitHub -> {target}")
        except Exception as exc:
            raise RuntimeError(
                f"Failed to download snkv.h from GitHub ({raw_url}):\n{exc}\n"
                "Either install make (Linux/macOS) or ensure internet access."
            ) from exc


# ---------------------------------------------------------------------------
# Platform-specific linker flags
# ---------------------------------------------------------------------------

extra_link_args: list = []

if sys.platform == "darwin":
    extra_link_args = ["-lpthread", "-lm"]

# ---------------------------------------------------------------------------
# C extension
# ---------------------------------------------------------------------------

ext = Extension(
    "snkv._snkv",
    sources=["snkv_module.c"],
    include_dirs=[HERE],
    extra_link_args=extra_link_args,
)

# ---------------------------------------------------------------------------
# setup()
# ---------------------------------------------------------------------------

setup(
    ext_modules=[ext],
    cmdclass={"build_ext": BuildExtWithHeader},
)
