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
      datacatalog - Perform data catalog update - Default is True
    """
    task_id = str(catalog_derivative_gen.request.id)
    #create Result Directory
    resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)
    os.makedirs(resultpath)
    #s3 boto
    s3 = boto3.resource('s3')
    #catalog url template
    url_template="%s/api/catalog/data/catalog/bagit_inventory/.json?page_size=1&query={'filter':{'s3.valid':True,'bag':'%s'}}"
    #select each bag
    for bag in bags.split(','):
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
                #filter for tiff or tiff files
                if fle.split('/')[-1].split('.')[-1].lower() == 'tif' or fle.split('/')[-1].split('.')[-1].lower() == 'tiff':
                    #download file from s3
                    s3.meta.client.download_file(bucket, fle, inpath)
                    #setup paths from image processing
                    inpath="{0}/{1}".format(src_input,fle.split('/')[-1])
                    outpath="{0}/{1}.{2}".format(output,fle.split('/')[-1].split('.')[0].lower(),outformat)
                    out_url = "{0}/oulib_tasks/{1}/derivative/{2}/{3}.{4}".format(hostname, task_id,bag,fle.split('/')[-1].split('.')[0].lower(),outformat)
                    #process image
                    _processimage(inpath=inpath,outpath=outpath,outformat=outformat,filter=filter,scale=scale,crop=crop)
                    filename=fle.split('/')[-1].split('.')[0].lower()
                    derivative_info={"filename":filename,"local_file":outpath,"location":out_url,"outformat":outformat,"filter":filter,"scale":scale,"crop":crop}
                    os.remove(inpath)
                    if datacatalog:
                        data_catalog(bag,derivative_info, itm)
        shutil.rmtree(os.path.join(resultpath,'src/',bag))
    shutil.rmtree(os.path.join(resultpath,'src/'))
    return "{0}/oulib_tasks/{1}".format(hostname, task_id)


def data_catalog(bag,derivative_info, org_data,database='catalog',collection='digital_objects'):
    """ This is a first stab at getting information into the data catalog. Will need to figure out 
        processs. This gives the creation of a digital object collection with derivative information.
        Problems to solve:
            Duplication minimize file creation 
            split catalog
            access derivatives for specific reason - add a tags field for identification for specific application
 
    """
    #get data catalog token
    f1=open('/code/cybercom_token','r')
    token = "Token {0}".format(f1.read().strip())
    f1.close()
    headers ={'Content-Type':'application/json',"Authorization":token}
    url_dc_collection="{0}/api/catalog/data/{1}/{2}/.json".format(hostname,database,collection)
    # check for existing metadata listing
    req = requests.get(url_dc_collection + "?query={'filter':{'bag':'%s'}}" % (bag))
    d_object=req.json()
    if d_object['count']>0:
        newdata=d_object['results'][0]
        if not 'derivatives' in newdata.keys:
            newdata['derivatives']={derivative_info["filename"]:[]}
        if derivative_info["filename"] in newdata['derivatives'].keys:
             newdata['derivatives'][derivative_info["filename"]].append(derivative_info)
        else:
            newdata['derivatives'][derivative_info["filename"]]= [derivative_info]           
        #update metadata
        uls=url_dc_collection.split('.')
        update_url="{0}/{1}/.{2}".format(uls[0],newdata['_id'],uls[1]
        req=requests.put(update_url,data=json.dumps(newdata),headers=headers)
    else:
        newdata={}
        newdata['locations']={'s3':org_data['s3'],'nas':org_data['nas'],'norfile':org_data['norfile']}
        newdata['bag']=bag
        newdata['department']=org_data['department']
        newdata['project']=org_data['project']
        newdata['derivatives']={derivative_info["filename"]:[derivative_info]}
        # Add new record
        req=requests.post(url_dc_collection,data=json.dumps(newdata),headers=headers)

