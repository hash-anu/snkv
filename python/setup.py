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
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Absolute path of the repository root (one level above this file).
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
HERE      = os.path.abspath(os.path.dirname(__file__))


class BuildExtWithHeader(_build_ext):
    """Custom build_ext that regenerates snkv.h before compiling."""

    def run(self) -> None:
        self._regenerate_snkv_header()
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
            make_cmd = "mingw32-make" if sys.platform == "win32" else "make"
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
            # make not found - fall back to using an existing snkv.h if present
            if not os.path.exists(target):
                raise RuntimeError(
                    "`make` not found and no pre-built snkv.h exists in python/.\n"
                    "Install make (Linux: apt install make, macOS: xcode-select --install,\n"
                    "Windows: use MSYS2 shell) and re-run the build."
                )
            print(f"-- snkv: `make` not found; using existing {target}")
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
    name="snkv",
    version="0.2.0",
    description="Python bindings for SNKV - crash-safe embedded key-value store",
    license="Apache-2.0",
    url="https://github.com/hash-anu/snkv",
    python_requires=">=3.8",
    packages=["snkv"],
    package_data={
        "snkv": ["py.typed", "_snkv.pyi"],
    },
    ext_modules=[ext],
    cmdclass={"build_ext": BuildExtWithHeader},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: C",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
