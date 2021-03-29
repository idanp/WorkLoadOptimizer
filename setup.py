import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
     name='WLO',
     version='0.0.4',
     scripts=['ExecutorManager.py'],
     author="Idan Perez",
     author_email="kimpatz@gmail.com",
     description="This is Workload Optimizer framework - more can be found at the git page",
     long_description='file: README.md',
     long_description_content_type="text/markdown",
     url="https://github.com/idanp/OWL",
     packages=setuptools.find_packages(),
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )