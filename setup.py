from setuptools import setup, find_packages

def make_package():
    """
    Function to make python packages

    Run the command below in your terminal

    >>> python setup.py bdist_wheel
    """

    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()

    setup(
        name='fire-incidents',
        version='1.0',
        long_description=long_description,
        long_description_content_type="text/markdown",
        #package_dir={"": ""},
        packages=find_packages(exclude=("tests",)),
        python_requires=">=3",
        install_requires=['awscli==1.19.0', 'boto3==1.17.0', 'botocore==1.20.0', 's3transfer==0.3.4',
                          'urllib3==1.25.11', 'requests==2.24.0', 'XlsxWriter==1.3.7', 'numpy==1.19.5', 'pandas==1.1.5',
                          'pyarrow==3.0.0', 's3fs==0.4.2', 'awswrangler==2.5.0', 'scramp==1.2.0',
                          'PyYAML==5.4.1'],
        include_package_data=True,
        package_data={'': ['*.json', '*.yaml','*.sql', '*.conf']}
    )


if __name__ == '__main__':
    make_package()
