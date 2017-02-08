from celery.task import task
from PIL import Image
import cv2
import os

#Default base directory
basedir = "/data/"
hostname = "http://localhost"


def _processimage(inpath, outpath, outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
    Internal function to create image derivatives
    """

    try:
        image = Image.open(inpath)
    except (IOError):
        # workaround for Pillow not handling 16bit images
        im = cv2.imread(inpath) # opencv loads as 8bit BGR color
        im = cv2.cvtColor(im, cv2.COLOR_BGR2RGB) # realign to RGB
        image = Image.fromarray(im)

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
