# type: ignore
import setuptools

MAJOR               = 0
MINOR               = 1
MICRO               = 2
VERSION             = f"{MAJOR}.{MINOR}.{MICRO}"

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="vw-executor",
    version=VERSION,
    author="Alexey Taymanov",
    author_email="ataymano@gmail.com",
    description="Helpers for driving vw execution from python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/VowpalWabbit/data-science",
    license="BSD 3-Clause License",
    packages=["vw_executor"],
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering"
    ],
    install_requires = ['pandas>=1.0.0', 'tqdm>=4.0.0', 'vowpalwabbit >= 8.10.0'],
    python_requires=">=3.6",
    tests_require=['unittest']
)