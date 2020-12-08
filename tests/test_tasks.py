import sys
from nose.tools import assert_true, assert_false, assert_equal, nottest
try:
    from unittest.mock import MagicMock, Mock, patch
except ImportError:
    from mock import MagicMock, Mock, patch

from imageq.tasks.tasks import _formatextension, _params_as_string
from PIL import Image

def test__formatextension():
    assert_equal(_formatextension("jpeg"), "jpg")
    assert_equal(_formatextension("JPEG"), "jpg")
    assert_equal(_formatextension("tiff"), "tif")
    assert_equal(_formatextension("TiFF"), "tif")
    assert_equal(_formatextension("png"), "png")
    assert_equal(_formatextension("PNG"), "png")


def test__params_as_string():
    assert_equal(
        _params_as_string("TIFF","antialias",0.4),
        "tiff_040_antialias"
    )   
    assert_equal(
        _params_as_string("jpeg","antialias",0.4),
        "jpeg_040_antialias"
    )
    assert_equal(
        _params_as_string("jpeg","antialias",0.4,[10, 10, 200, 200]),
        "jpeg_040_antialias_10_10_200_200"
    )


def test_image():
   image = Image.new("RGB", (10,10))
   assert_equal(image.mode, "RGB")


def test_open_16bit_image():
    image = Image.open("tests/img.png")
    assert_true(image)
