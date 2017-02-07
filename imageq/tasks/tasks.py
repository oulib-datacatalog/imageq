from celery.task import task
from dockertask import docker_task
from PIL import Image
from subprocess import check_call, check_output
from tempfile import NamedTemporaryFile
import os,boto3,requests

#Default base directory
basedir = "/data/"
hostname = "https://cc.lib.ou.edu"

#imagemagick needs to be installed within the docker container


def _processimage(inpath, outpath, outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Internal function to create image derivatives
    """

    try:
        image = Image.open(inpath)
    except (IOError, OSError):
        # workaround for Pillow not handling 16bit images
        if "16-bit" in check_output(("identify", inpath)):
            with NamedTemporaryFile() as tmpfile:
                check_call(("convert", inpath, "-depth", "8", tmpfile.name))
                image = Image.open(tmpfile.name)
        else:
            raise Exception

    if crop:
        image = image.crop(crop)

    if scale:
        imagefilter = getattr(Image, filter.upper())
        size = [x * scale for x in image.size]
        image.thumbnail(size, imagefilter)

    image.save(outpath, outformat)


@task()
def processimage(inpath, outpath, outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Digilab TIFF derivative Task

    args:
      inpath - path string to input image
      outpath - path string to output image
      outformat - string representation of image format - default is "TIFF"
      scale - percentage to scale by represented as a decimal
      filter - string representing filter to apply to resized image - default is "ANTIALIAS"
      crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
    """

    task_id = str(processimage.request.id)
    #create Result Directory
    resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)
    os.makedirs(resultpath)

    _processimage(inpath=os.path.join(basedir, inpath),
                  outpath=os.path.join(resultpath, outpath),
                  outformat=outformat,
                  filter=filter,
                  scale=scale,
                  crop=crop
                  )

    return "{0}/oulib_tasks/{1}".format(hostname, task_id)

@task()
def catalog_derivative_gen(bags,outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Digilab data catalog derivative generation

    args:
      bags - comma separated string of bag names
    kwargs
      outformat - string representation of image format - default is "TIFF"
      scale - percentage to scale by represented as a decimal
      filter - string representing filter to apply to resized image - default is "ANTIALIAS"
      crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
    """
    task_id = str(catalog_derivative_gen.request.id)
    #create Result Directory
    resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)
    os.makedirs(resultpath)
    #s3 boto
    s3_client = boto3.client('s3')
    #select each bag
    for bag in bags.split(','):
        url_template="%s/api/catalog/data/catalog/bagit_inventory/.json?page_size=1&query={'filter':{'s3.valid':True,'bag':'%s'},'projection':{'s3':1}}"
        r=requests.get(url_template % (hostname,bag))
        data=r.json()["results"]
        src_input = os.path.join(resultpath,'src/',bag)
        output = os.path.join(resultpath,'output/',bag)
        os.makedirs(src_input)
        os.makedirs(output)
        print src_input,output
        #download source files
        for itm in data:
            bucket = itm['s3']['bucket']
            for fle in itm['s3']["verified"]:
                if fle.split('/')[-1].split('.')[-1].lower() == 'tif' or fle.split('/')[-1].split('.')[-1].lower() == 'tiff':
                    s3_client.download_file(bucket,fle,"{0}/{1}".format(src_input,fle.split('/')[-1]))
                    _processimage(inpath="{0}/{1}".format(src_input,fle),
                        outpath="{0}/{1}.{2}".format(output,fle.split('/')[-1].split('.')[0].lower(),outformat),
                        outformat=outformat,
                        filter=filter,
                        scale=scale,
                        crop=crop)
    return "{0}/oulib_tasks/{1}".format(hostname, task_id)
