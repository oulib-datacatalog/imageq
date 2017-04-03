from celery.task import task
from dockertask import docker_task
from PIL import Image
from subprocess import check_call, check_output, call
from tempfile import NamedTemporaryFile
import os,boto3,requests,shlex,shutil,json

#Default base directory
basedir = "/data/web_data/static"
hostname = "https://cc.lib.ou.edu"
s3_derivative_stub="derivative-bags"
#imagemagick needs to be installed within the docker container


def _formatextension(imageformat):
    """ get common file extension of image format """
    imgextensions = {"JPEG": "jpg",
                     "TIFF": "tif",
                             }
    try:
        return imgextensions[imageformat.upper()]
    except KeyError:
        return imageformat.lower()


def _params_as_string(outformat="", filter="", scale=None, crop=None):
    """
    Internal function to return image processing parameters as a single string
    Input: outformat="TIFF", filter="ANTIALIAS", scale=0.5, crop=[10, 10, 200, 200]
    Returns: tiff_050_antialias_10_10_200_200
    """
    imgformat = outformat.lower()
    imgfilter = filter.lower() if scale else None  # do not include filter if not scaled
    imgscale = str(int(scale * 100)).zfill(3) if scale else "100"
    imgcrop = "_".join((str(x) for x in crop)) if crop else None
    return "_".join((x for x in (imgformat, imgscale, imgfilter, imgcrop) if x))


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
def derivative_generation(bags,s3_bucket='ul-bagit',s3_source='source',s3_destination='derivative',outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None):
    """
        This task is used for derivative generation for the OU Library. This does not use the data catalog.
        Bags do not have to be valid. TIFF or TIF files are transformed and upload to AWS S3 destination.

        args:
            bags (string): comma separated list of bags. Example - 'bag1,bag2,bag3'
        kwargs:
            s3_bucket (string); Defult 'ul-bagit' 
            s3_source (string): Default 'source-bags'
            s3_destination (string): Default 'derivative-bags'
            outformat - string representation of image format - default is "TIFF". 
                        Available Formats: http://pillow.readthedocs.io/en/3.4.x/handbook/image-file-formats.html
            scale - percentage to scale by represented as a decimal
            filter - string representing filter to apply to resized image - default is "ANTIALIAS"
            crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
    """
    task_id = str(derivative_generation.request.id)
    #create Result Directory
    resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)
    os.makedirs(resultpath)
    #s3 boto
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    formatparams = _params_as_string(nutformat=outformat, filter=filter, scale=scale, crop=crop)
    for bag in bags.split(','):
        derivative_keys=[]
        src_input = os.path.join(resultpath,'src/',bag)
        output = os.path.join(resultpath,'derivative/',bag)
        os.makedirs(src_input)
        os.makedirs(output)
        source_location = "{0}/{1}/data".format(s3_source,bag)
        for obj in bucket.objects.filter(Prefix=source_location):
            filename=obj.key
            if filename.split('.')[-1].lower()=='tif' or filename.split('.')[-1].lower()=='tiff':
                inpath="{0}/{1}".format(src_input,filename.split('/')[-1])
                s3.meta.client.download_file(bucket.name, filename, inpath)
                outpath="{0}/{1}.{2}".format(output,filename.split('/')[-1].split('.')[0].lower(),_formatextension(outformat))
                #process image
                _processimage(inpath=inpath,outpath=outpath,outformat=outformat,filter=filter,scale=scale,crop=crop)
                #upload derivative to s3
                fname=filename.split('/')[-1].split('.')[0].lower()
                s3_key = "{0}/{1}/{2}/{3}.{4}".format(s3_destination,bag,formatparams,fname,_formatextension(outformat))
                derivative_keys.append(s3_key)
                #upload to 
                s3.meta.client.upload_file(outpath, bucket.name, s3_key)
                os.remove(inpath)
        shutil.rmtree(os.path.join(resultpath,'src/',bag))
    shutil.rmtree(os.path.join(resultpath,'src/'))
    return {"local_derivatives":"{0}/oulib_tasks/{1}".format(hostname, task_id),"s3_destination":s3_destination,"s3_bags":bags, "task_id":task_id, "format_parameters": formatparams} 
