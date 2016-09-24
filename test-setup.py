from setuptools import setup

setup(
    name="tx-manager",
    version="0.0.4",
    author="unfoldingWord",
    author_email="unfoldingword.org",
    description="Unit test setup file.",
    keywords="",
    url="https://github.org/unfoldingWord-dev/tx-manager",
    packages=['tx_manager'],
    long_description='Unit test setup file',
    classifiers=[],
    install_requires=[
        'requests',
        'tx-shared-tools'
    ],
    test_suite='tests'
)
