#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='imageq',
      version='0.0',
      packages= find_packages(),
      package_data={'imageq':['tasks/templates/*.tmpl','imageq/tasks/templates/*.tmpl']},
      install_requires=[
          'celery==3.1.22',
          'pymongo==3.2.1',
          'paramiko==1.16.0',
          'requests==2.9.1',
          'pillow==3.3.1',
          'opencv-python==3.2.0.6',
          'jinja2',
      ],
     dependency_links=[
          'http://github.com/ouinformatics/dockertask/tarball/master#egg=dockertask-0.0',
      ],
     include_package_data=True,
)
