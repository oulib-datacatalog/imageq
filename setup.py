#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='imageq',
      version='0.3',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'pillow==6.2.2',
          'boto3',
      ],
)
