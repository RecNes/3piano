import os


def get_upload_folder(working_dir):
    upload_dir = os.path.join(working_dir, 'uploads')
    if not os.path.exists(upload_dir):
        os.mkdir(upload_dir)
    return upload_dir
