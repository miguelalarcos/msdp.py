import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="msdp",
    version="0.0.5",
    author="Miguel Ángel Alarcos Torrecillas",
    author_email="miguel.alarcos@gmail.com",
    description="Subscription Data Protocol for server side Python and asyncio",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/miguelalarcos/msdp.py",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)