import os
import tarfile

import pdb

def tar(input_dir):
    """
    Creates a tar.gz tarball of the provided directory.
    """
    tarball_name = os.path.basename(input_dir) + ".tar.gz"
    with tarfile.open(tarball_name, mode="w:gz") as tb:
        tb.add(name=input_dir, arcname=os.path.basename(input_dir))
    return tarball_name

if __name__ == "__main__":
    tar(os.getcwd())
