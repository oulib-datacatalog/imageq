from celery.task import task
from dockertask import docker_task
from PIL import Image
from subprocess import check_call, check_output, call
from tempfile import NamedTemporaryFile
import os,boto3,requests,shlex,shutil,json

#Default base directory
basedir = "/data/web_data/static"
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
def catalog_derivative_gen(bags,outformat="TIFF", filter="ANTIALIAS", scale=None, crop=None, datacatalog=True):
    """
    Digilab data catalog derivative generation

    args:
      bags - comma separated string of bag names
    kwargs
      outformat - string representation of image format - default is "TIFF". Available Formats: http://pillow.readthedocs.io/en/3.4.x/handbook/image-file-formats.html
      scale - percentage to scale by represented as a decimal
      filter - string representing filter to apply to resized image - default is "ANTIALIAS"
      crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
    """
    task_id = str(catalog_derivative_gen.request.id)
    #create Result Directory
    resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)
    os.makedirs(resultpath)
    #s3 boto
    s3 = boto3.resource('s3')
    #get data catalog token
    f1=open('/code/cybercom_token','r')
    token = "Token {0}".format(f1.read().strip())
    f1.close()
    headers ={'Content-Type':'application/json',"Authorization":token}
    url_new_catalog="https://cc.lib.ou.edu/api/catalog/data/catalog/digital_objects/.json"
    #select each bag
    for bag in bags.split(','):
        url_template="%s/api/catalog/data/catalog/bagit_inventory/.json?page_size=1&query={'filter':{'s3.valid':True,'bag':'%s'}}"
        r=requests.get(url_template % (hostname,bag))
        data=r.json()["results"]
        src_input = os.path.join(resultpath,'src/',bag)
        output = os.path.join(resultpath,'derivative/',bag)
        os.makedirs(src_input)
        os.makedirs(output)
        derivatives={}
        #download source files
        for itm in data:
            bucket = itm['s3']['bucket']
            for fle in itm['s3']["verified"]:
                if fle.split('/')[-1].split('.')[-1].lower() == 'tif' or fle.split('/')[-1].split('.')[-1].lower() == 'tiff':
                    inpath="{0}/{1}".format(src_input,fle.split('/')[-1])
                    s3.meta.client.download_file(bucket, fle, inpath)
                    outpath="{0}/{1}.{2}".format(output,fle.split('/')[-1].split('.')[0].lower(),outformat)
                    out_url = "{0}/oulib_tasks/{1}/derivative/{2}.{3}".format(hostname, task_id,fle.split('/')[-1].split('.')[0].lower(),outformat)
                    _processimage(inpath=inpath,outpath=outpath,outformat=outformat,filter=filter,scale=scale,crop=crop)
                    derivatives[fle.split('/')[-1].split('.')[0].lower()]={"location":out_url,"outformat":outformat,"filter":filter,"scale":scale,"crop":crop}
                    os.remove(inpath)
        shutil.rmtree(os.path.join(resultpath,'src/',bag))
        #set data catalog new inventory
        if datacatalog:
            req = requests.get(url_new_catalog + "?query={'filter':{'bag':'%s'}}" % (bag))
            d1=req.json()
            if d1['count']>0:
                newdata=d1['results'][0]
                newdata['derivatives']=derivatives
            else:
                newdata={}
                newdata['locations']={'s3':data[0]['s3'],'nas':data[0]['nas'],'norfile':data[0]['norfile']}
                newdata['bag']=bag
                newdata['department']=data[0]['department']
                newdata['project']=data[0]['project']
                newdata['derivatives']=derivatives
            req=requests.post(url_new_catalog,data=json.dumps(newdata),headers=headers)         
    shutil.rmtree(os.path.join(resultpath,'src/'))
    return "{0}/oulib_tasks/{1}".format(hostname, task_id)
