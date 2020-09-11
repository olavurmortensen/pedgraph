import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = ['neo4j-driver>=4.1.1',
                    'tqdm==4.48.2']

setuptools.setup(
    name="pedgraph",
    version="0.0.1",
    author="Ólavur Mortensen",
    author_email="olavurmortensen@gmail.com",
    description="PedGraph -- Multidimensional network database for pedigree analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/olavurmortensen/pedgraph",
    packages=setuptools.find_packages(),
    install_requires=INSTALL_REQUIRES,
    python_requires='>=3.6',
)
