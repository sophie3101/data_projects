import requests, zipfile, io, os,re, shutil
# def download_n_extract(url, des_folder):
#     os.makedirs(des_folder, exist_ok=True)
#     try:
#         extracted_csv_files=None
#         response = requests.get(url)
#         response.raise_for_status()
#         with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
#             print("ZIP contents:", zip_file.namelist())  
#             zip_file.extractall(des_folder)
#             extracted_csv_files=[f for f in zip_file.namelist() if f.endswith(".csv") and "__MACOSX" not in f]
#         # print(extracted_csv_files)
#         return extracted_csv_files
#     except Exception as e:
#         print("ERROR in downloading zip file: ", e)
#         raise
def remove_file(file_path):
    os.remove(file_path)
    
def remove_folder(des_folder):
    if os.path.exists(des_folder):
        print(f"{des_folder} exists, delete ")
        shutil.rmtree(des_folder)

def download_n_extract(url, des_folder):
    
    os.makedirs(des_folder, exist_ok=True)
    try:
        response = requests.get(url)
        response.raise_for_status()
        extract(io.BytesIO(response.content), des_folder)
        extracted_files = []
        for root, _, files in os.walk(des_folder):
            for f in files:
                if "__MACOSX" not in root and f.endswith(".csv"):
                    extracted_files.append(os.path.join(root, f))

        # print(extracted_files)
        return extracted_files
    
    except Exception as e:
        print("ERROR in downloading or extracting zip file:", e)
        raise

def extract(filecontent, des_folder, file_zip_path=None, seen_zips=None):
    if seen_zips is None:
        seen_zips = set()

    try:
        with zipfile.ZipFile(filecontent) as zip_file:
            zip_file.extractall(des_folder)
    except zipfile.BadZipFile as e:
        print(f"ERROR: Invalid zip file: {e}")
        raise
    finally:
        if file_zip_path and os.path.exists(file_zip_path):
            print(f"Removing {file_zip_path}")
            os.remove(file_zip_path)

    # Find all zip files after extraction
    nested_zips = []
    for root, dirs, files in os.walk(des_folder):
        for filename in files:
            if filename.lower().endswith(".zip"):
                file_path = os.path.join(root, filename)
                if file_path not in seen_zips and os.path.exists(file_path):
                    nested_zips.append(file_path)
                    seen_zips.add(file_path)

    # Now extract each nested zip
    for nested_zip_path in nested_zips:
        print(f"Found nested zip: {nested_zip_path}")
        try:
            with open(nested_zip_path, 'rb') as f:
                nested_content = io.BytesIO(f.read())
                extract(nested_content, os.path.dirname(nested_zip_path), nested_zip_path, seen_zips)
        except Exception as e:
            print(f"ERROR extracting nested zip {nested_zip_path}: {e}")
if __name__=="__main__":    
    # download_n_extract("https://s3.amazonaws.com/tripdata/202404-citibike-tripdata.zip","tmp/")
    # download_n_extract("https://s3.amazonaws.com/tripdata/2016-citibike-tripdata.zip","tmp/")
    download_n_extract("https://s3.amazonaws.com/tripdata/2020-citibike-tripdata.zip","tmp/")
    