import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mds-easy-libs",
    version="0.0.1",
    author="Nicola Padovano",
    author_email="nicola.padovano@mediaset.it",
    description="Mediaset Easy Libraries",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/npado/mds-easy-libs",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=['slackclient>=2.1.0', 'requests', 'urllib3', 'validators']
)
