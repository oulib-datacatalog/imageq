from nose.tools import assert_true, assert_false, assert_equal, nottest

from PIL import Image

def test_image():
   image = Image.new("RGB", (10,10))
   assert_equal(image.mode, "RGB")

def test_open_16bit_image():
    image = Image.open("tests/img.png")
