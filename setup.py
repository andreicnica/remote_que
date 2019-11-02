""" This is the install script. Be careful to update the version from time to
    time and to add any additional entry points as you develop them. Also,
    please keep the requirements list up to date.
"""

from setuptools import setup, find_packages


VERSION = '0.1.0'

print('-- Installing remote_que ' + VERSION)
with open("./remote_que/version.py", 'w') as f:
    f.write("__version__ = '{}'\n".format(VERSION))


setup(
    name="remote-que",
    version=VERSION,
    description="Remote launch que of linux commands",
    entry_points={
        "console_scripts": [
            "remote-que=remote_que.cmds:start",
        ]
    },
    packages=find_packages(),
    url="https://github.com/andreicnica/remote_que.git",
    author="Andrei Nica",
    author_email="andreic.nica@gmail.com",
    license="MIT",
    install_requires=[],
    zip_safe=False,
)
