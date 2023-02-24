#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='imageq',
      version='0.4',
      packages= find_packages(),
      install_requires=[
          'celery==5.2.7',
          'pillow==9.4.0',
          'boto3',
      ],
)
