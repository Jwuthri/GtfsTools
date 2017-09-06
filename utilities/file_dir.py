"""Tools to work on dir or file."""
import os
from glob import fnmatch
from zipfile import ZipFile
from hashlib import sha1


def hashfile(filepath):
    """Create a sha1 for a file, or dir."""
    sha = sha1()
    fichier = open(filepath, 'rb')
    try:
        sha.update(fichier.read())
    finally:
        fichier.close()

    return sha.hexdigest()


def get_file_dir(afile):
    """Return the directory of a file."""
    return os.path.dirname(os.path.abspath(afile))


def get_file_name(afile):
    """Return the basename of a file."""
    return os.path.basename(os.path.abspath(afile))


def get_same_files(path, pattern):
    """Return all files in a directory with the same pattern."""
    for dirpath, dirnames, filenames in os.walk(path):
        for f in fnmatch.filter(filenames, pattern):
            yield os.path.join(dirpath, f)


def list_zip_files(afile):
    """Buff a zip."""
    if not isinstance(afile, ZipFile):
        afile = ZipFile(afile)

    zfiles = [zf.filename for zf in afile.filelist]

    return zfiles
