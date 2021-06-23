import json
import os
import logging
import requests

logging.basicConfig()

class SDK:
    def __init__(self, url: str):
        self.url = url
        self.logger = logging.getLogger()
        self.logger.setLevel(level=logging.INFO)

    def list(self, container_name: str) -> None:
        """Deletes a blob from a container

               :param str container_name:
                   The name of the container.

               :return: List of blobs including directories or None
               :rtype: None

           """
        try:
            res = requests.get(self.url + 'list', json={'container_name': container_name})
            if res.status_code == 200:
                self.logger.info(f"Listing files of container {container_name}")
                return json.loads(res.content)
            else:
                return None
        except ConnectionError as ce:
            raise ce

    def upload(self, container_name: str, blob_name: str, path: str) -> bool:
        """Upload a blob to a container

                    :param str container_name:
                        The name of the container.

                    :param str blob_name:
                        The storage account name used to generate the shared access signature.

                    :param str path:
                        Absolute path where the source file is located.

                    :return: True if upload was successful else False
                    :rtype: bool

                """
        try:
            res = requests.get(self.url + 'upload', json={'container_name': container_name, 'blob_name': blob_name})
            if res.status_code == 200:
                uri = json.loads(res.content)['uri']
                with open(path, 'rb') as f:
                    requests.put(uri, data=f, headers={'x-ms-blob-type': 'BlockBlob'})
                self.logger.info(f"Blob uploaded {blob_name}")
                return True
            else:
                self.logger.error(f"Blob not uploaded {blob_name}")
                return False
        except ConnectionError as ce:
            raise ce
        except FileNotFoundError as fe:
            raise fe

    def download(self, container_name: str, blob_name: str, dir="", path="") -> bool:
        """Downloads a blob from a container

                            :param str container_name:
                                The name of the container.

                            :param str blob_name:
                                The storage account name used to generate the shared access signature.

                            :param str dir:
                                Path of download folder. e.g., file/

                            :param str path:
                                Absolute path of file getting downloaded. e.g., file/report.pbix

                            :return: True if download was successful else False
                            :rtype: bool

                        """
        try:
            res = requests.get(self.url + 'download', json={'container_name': container_name, 'blob_name': blob_name})
            if res.status_code == 200:
                uri = json.loads(res.content)['uri']
                abs_path = path if path else os.path.join(dir, blob_name)
                with open(abs_path, 'wb') as f:
                    s = requests.Session()
                    with s.get(uri, stream=True) as resp:
                        for data in resp.iter_content():
                            if data:
                                f.write(data)
                self.logger.info(f"Blob downloaded {path}{blob_name}")
                return True
            else:
                self.logger.error(f"Blob not downloaded {path}{blob_name}")
                return False
        except ConnectionError as ce:
            raise ce
        except FileNotFoundError as fe:
            raise fe

    def delete(self, container_name: str, blob_name: str) -> bool:
        """Deletes a blob from a container

            :param str container_name:
                The name of the container.

            :param str blob_name:
                The storage account name used to generate the shared access signature.

            :return: True if deletion was successful else False
            :rtype: bool

        """
        try:
            res = requests.delete(self.url + 'delete', json={'container_name': container_name, 'blob_name': blob_name})
            if res.status_code == 200:
                self.logger.info(f"Blob deleted {blob_name}")
                return True
            else:
                self.logger.error(f"Unable to delete blob {blob_name}")
                return False
        except ConnectionError as ce:
            raise ce
