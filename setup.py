"""Setup script for EcoVoyage package."""

from setuptools import setup, find_packages

setup(
    name="ecovoyage",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    description="An eco-friendly travel planning package",
    author="EcoVoyage Team",
    author_email="atrawog@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.12",
) 