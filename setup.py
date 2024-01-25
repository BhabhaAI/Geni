import re
from os import path
from setuptools import setup, find_packages

def read_requirements(path):
    with open(path, "r") as f:
        requirements = f.read().splitlines()
        processed_requirements = []

        for req in requirements:
            # For git or other VCS links
            if req.startswith("git+") or "@" in req:
                pkg_name = re.search(r"(#egg=)([\w\-_]+)", req)
                if pkg_name:
                    processed_requirements.append(pkg_name.group(2))
                else:
                    # You may decide to raise an exception here,
                    # if you want to ensure every VCS link has an #egg=<package_name> at the end
                    continue
            else:
                processed_requirements.append(req)
        return processed_requirements


requirements = read_requirements("requirements.txt")
here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='GenHi',
    version='0.0.1',
    description='Generate Synthetic Hindi & Roman Hindi Datasets',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/BhabhaAI/GenHi', 
    author='Bhabha AI',
    packages=find_packages(),
    include_package_data=True,
    author_email='contact@bhabha.ai',
    license='Apache-2.0',
    python_requires='>=3.10',
    install_requires=requirements,
)
