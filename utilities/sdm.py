from urllib.parse import urlparse
from urllib.parse import urljoin
import urllib.request
import os
import requests

from requests_toolbelt.multipart.encoder import MultipartEncoder


class SdmHttpClient(object):

    base_url = "http://ida.grenoble.xrce.xerox.com"
    urls = {
        "upload": "ScheduleManager.Api/api/Schedule",
        "list": "ScheduleManager.Api/api/Schedule",
        "get": "ScheduleManager.Api/api/Schedule",
        "delete": "ScheduleManager.Api/api/Schedule",
    }

    def __init__(self, date):
        self.date = date.strftime("%Y-%m-%d")
        self.check_server()

    def url(self, action):
        return urljoin(self.base_url, self.urls[action])

    def sdm_alive(self):
        res = requests.get(self.base_url)
        if res.status_code == 200:
            return True
        else:
            return False

    def check_server(self):
        res = self.sdm_alive()
        if not res:
            msg = "SDM on {} did not respond to ping. Is it running?"
            raise ManagerException(msg.format(self.url))

    def list_gtfs(self, criteria):
        criteria["startdate"] = criteria["enddate"] = self.date
        if type not in criteria:
            criteria["type"] = "None"
        res = requests.get(
                '?'.join([
                    self.url("list"),
                    '&'.join(['='.join([k, v]) for k, v in criteria.items()])
                ])
            )
        return [gtfs for gtfs in res.json()]

    def get_gtfs(self, id, name="gtfs.zip"):
        gtfs_file = urllib.request.urlopen('?'.join([self.url("get"), '='.join(["id", id])]))
        gtfs_path = os.path.expanduser(os.path.join(os.getcwd(), "temp", name))
        with open(gtfs_path, "wb") as gtfs:
            gtfs.write(gtfs_file.read())

        return gtfs_path

    def delete_gtfs(self, id):
        return requests.delete('?'.join([self.url("delete"), '='.join(["id", id])]))

    def prepare_gtfs(self, gtfs_zip, gtfs_name , type):
        return MultipartEncoder(
            fields={
                "name": gtfs_name,
                "startdate": self.date,
                "enddate": self.date,
                "type": type,
                "schedule": (gtfs_zip, open(gtfs_zip, 'rb'), 'text/plain')
            }
        )

    def post_gtfs   (self, gtfs_zip, gtfs_name , type="planned"):
        multipart_data = self.prepare_gtfs(gtfs_zip, gtfs_name.lower() , type)
        res = requests.post(
            self.url("upload"),
            data=multipart_data,
            headers={'Content-Type': multipart_data.content_type}
        )
        if 200 != res.status_code:
            msg = "Error during the upload of the gtfs {} to SDM"
            raise ManagerException(msg.format(gtfs_name))


class ManagerException(Exception):
    """Class to generate the raised exception."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message