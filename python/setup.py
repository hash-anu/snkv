"""
Build script for the snkv Python extension.

Before compiling the C extension this script:
  1. Runs `make snkv.h` in the repo root to regenerate the amalgamated header.
  2. Copies the freshly built snkv.h into this directory so the compiler
     finds it alongside snkv_module.c.
  3. Copies the native vector store (kvstore_vec.c + usearch) into vec/.
     It is compiled into the same extension; usearch_impl.cpp needs C++17.

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
VEC_DIR   = os.path.join(HERE, "vec")

# Vector store sources: (path in repo, path under vec/).
_VEC_FILES = [
    ("include/kvstore_vec.h",                      "kvstore_vec.h"),
    ("src/kvstore_vec.c",                          "kvstore_vec.c"),
    ("src/usearch/usearch.h",                      "usearch.h"),
    ("src/usearch/usearch_impl.cpp",               "usearch_impl.cpp"),
    ("src/usearch/include/usearch/index.hpp",         "include/usearch/index.hpp"),
    ("src/usearch/include/usearch/index_dense.hpp",   "include/usearch/index_dense.hpp"),
    ("src/usearch/include/usearch/index_plugins.hpp", "include/usearch/index_plugins.hpp"),
]


def _copy_vec_sources() -> None:
    """Copy the vector store sources from the repo into vec/.

    Inside the repo the copies are refreshed on every build.  From an sdist
    (no repo around it) the copies shipped in the sdist are used as-is.
    """
    if not os.path.exists(os.path.join(REPO_ROOT, "src", "kvstore_vec.c")):
        missing = [dst for _, dst in _VEC_FILES
                   if not os.path.exists(os.path.join(VEC_DIR, dst))]
        if missing:
            raise RuntimeError(
                "Vector store sources not found: run the build from the snkv "
                f"repository, or use an sdist that includes vec/ ({missing})"
            )
        return
    for src, dst in _VEC_FILES:
        target = os.path.join(VEC_DIR, dst)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        shutil.copy2(os.path.join(REPO_ROOT, src), target)


# Copy now as well, so `python -m build --sdist` packs the sources.
_copy_vec_sources()


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
        _copy_vec_sources()
        self._compile_cpp_as_cpp17()
        super().build_extension(ext)

    def _compile_cpp_as_cpp17(self) -> None:
        """Add the C++17 flag to .cpp files only (C files must not get it)."""
        cc = self.compiler
        if getattr(cc, "_snkv_cpp17", False):
            return
        cc._snkv_cpp17 = True
        if cc.compiler_type == "msvc":
            # MSVC has no _Float16: use the conversions in vec/fp16/fp16.h.
            for ext in self.extensions:
                ext.define_macros = [m for m in ext.define_macros
                                     if m[0] != "USEARCH_USE_FP16LIB"]
                ext.define_macros.append(("USEARCH_USE_FP16LIB", "1"))
            # MSVC applies /std:c++17 to C++ files only; /EHsc for exceptions.
            orig_compile = cc.compile

            def compile_msvc(sources, *args, **kwargs):
                cpp   = [s for s in sources if s.endswith(".cpp")]
                other = [s for s in sources if not s.endswith(".cpp")]
                objects = []
                if other:
                    objects += orig_compile(other, *args, **kwargs)
                if cpp:
                    kw = dict(kwargs)
                    kw["extra_postargs"] = (list(kwargs.get("extra_postargs") or [])
                                            + ["/std:c++17", "/EHsc"])
                    objects += orig_compile(cpp, *args, **kw)
                return objects

            cc.compile = compile_msvc
        else:
            orig = cc._compile

            def _compile(obj, src, ext, cc_args, extra_postargs, pp_opts):
                if src.endswith(".cpp"):
                    extra_postargs = list(extra_postargs) + ["-std=c++17"]
                return orig(obj, src, ext, cc_args, extra_postargs, pp_opts)

            cc._compile = _compile

    def _regenerate_snkv_header(self) -> None:
        target = os.path.join(HERE, "snkv.h")

        # Only regenerate once per build run.
        if getattr(self, "_snkv_h_ready", False):
            return

        # Outside the repo (an unpacked sdist) there is no Makefile: use the
        # snkv.h shipped in the sdist, or download it if missing.
        if not os.path.exists(os.path.join(REPO_ROOT, "Makefile")):
            if not os.path.exists(target):
                print("-- snkv: not in the snkv repo; downloading snkv.h from GitHub...")
                self._download_snkv_header(target)
            self._snkv_h_ready = True
            return

        # Try `make snkv.h` first (works on Linux, macOS, and Windows MSYS2).
        # If make is not on PATH, fall back to downloading from GitHub — same
        # on all platforms.
        try:
            result = subprocess.run(
                ["make", "snkv.h"],
                cwd=REPO_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            if result.stdout:
                print(result.stdout, end="")

            # Copy the generated header from repo root into python/
            src = os.path.join(REPO_ROOT, "snkv.h")
            if not os.path.exists(src):
                raise FileNotFoundError(
                    f"`make snkv.h` succeeded but {src} was not found."
                )
            shutil.copy2(src, target)
            print(f"-- snkv: copied {src} -> {target}")

        except FileNotFoundError:
            # make not on PATH — download from GitHub (same on all platforms)
            if os.path.exists(target):
                print(f"-- snkv: `make` not found; using existing {target}")
            else:
                print("-- snkv: `make` not found; downloading snkv.h from GitHub...")
                self._download_snkv_header(target)

        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"`make snkv.h` failed (exit {e.returncode}):\n{e.stderr}"
            ) from e

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
    sources=["snkv_module.c", "vec/kvstore_vec.c", "vec/usearch_impl.cpp"],
    include_dirs=[HERE, VEC_DIR, os.path.join(VEC_DIR, "include")],
    define_macros=[("USEARCH_USE_FP16LIB", "0")],
    extra_link_args=extra_link_args,
    language="c++",   # link with the C++ runtime (usearch)
)

# ---------------------------------------------------------------------------
# setup()
# ---------------------------------------------------------------------------

setup(
    ext_modules=[ext],
    cmdclass={"build_ext": BuildExtWithHeader},
    entry_points={"console_scripts": ["snkvctl=snkv.snkvctl:main"]},
)
