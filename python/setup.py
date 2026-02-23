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

        # 1. Resolve the latest release tag.
        api_url = "https://api.github.com/repos/hash-anu/snkv/releases/latest"
        try:
            req = urllib.request.Request(api_url, headers={"Accept": "application/vnd.github+json"})
            with urllib.request.urlopen(req) as resp:
                tag = json.loads(resp.read())["tag_name"]   # e.g. "v0.1.3"
        except Exception as exc:
            raise RuntimeError(
                f"Failed to fetch latest release tag from GitHub ({api_url}):\n{exc}\n"
                "Either install make (Linux/macOS) or ensure internet access."
            ) from exc

        # 2. Download the release zip, e.g. snkv-0.1.3.zip
        version = tag.lstrip("v")                           # "0.1.3"
        zip_url = (
            f"https://github.com/hash-anu/snkv/releases/download/"
            f"{tag}/snkv-{version}.zip"
        )
        print(f"-- snkv: downloading {zip_url} ...")
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = os.path.join(tmp, f"snkv-{version}.zip")
            try:
                urllib.request.urlretrieve(zip_url, zip_path)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to download release zip from GitHub ({zip_url}):\n{exc}\n"
                    "Either install make (Linux/macOS) or ensure internet access."
                ) from exc

            # 3. Extract and copy snkv.h from release/snkv-{version}/include/
            inner_path = f"release/snkv-{version}/include/snkv.h"
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
