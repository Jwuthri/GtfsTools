from urllib.parse import urlparse
from urllib.parse import urljoin
import requests

from requests_toolbelt.multipart.encoder import MultipartEncoder


class XtpHttpClient(object):

    def __init__(self, gtfs_zip, gtfs_name, date, learn=False):
        self.gtfs_zip = gtfs_zip
        self.gtfs_name = str(gtfs_name).lower()
        self.date = date.strftime("%d-%m-%Y")
        self.enable_tilex = False
        backend_l = "get_itineraries?backend=learn&Overrides="
        backend_r = "get_itineraries?Overrides="
        self.endpoint = backend_l if learn else backend_r
        self.process = "get_preprocessing_status"
        self.upload = "upload_gtfs"
        self.allowed = [{
            "Sequence Id": ["SubSeq_Walk", "PT1", "SubSeq_Walk"],
            "Max Alternatives": 3,
            "Max Branching Level": 3,
            "Non-Scheduled-Mode Overrides": [
                {'Max Secs Duration': 120, 'Mode': 'Mode 3'},
                {'Max Secs Duration': 120, 'Mode': 'Mode 4'}
            ],
            "Scheduled-Mode Override": {'Max Secs Waiting': 3600}
        }]

        self.base_url = "http://vigie:1190"
        self.plan_url = urljoin(self.base_url, self.endpoint)
        self.proccess_url = urljoin(self.base_url, self.process)
        self.upload_url = urljoin(self.base_url, self.upload)
        self.check_server()

    def check_server(self):
        res = self.xtp_alive()
        if not res:
            msg = "XTP on {} did not respond to ping. Is it running?"
            raise PlannerException(msg.format(self.base_url))

    def xtp_alive(self):
        res = requests.get(self.base_url)
        if res.status_code == 200:
            return True
        else:
            return False

    def gtfs_exist(self):
        res = requests.get(self.proccess_url)
        list_gtfs = res.json().get('Order of process')

        return self.gtfs_name in list_gtfs

    def prepare_gtfs_to_push(self):
        return MultipartEncoder(
            fields={
                'xfs': (self.gtfs_zip, open(self.gtfs_zip, 'rb'), 'text/plain')
            }
        )

    def push_gtfs_to_xtp(self):
        multipart_data = self.prepare_gtfs_to_push()
        res = requests.post(
            self.upload_url,
            data=multipart_data,
            headers={'Content-Type': multipart_data.content_type}
        )
        if res.json().get("Status") != "Success":
            msg = "Error during the upload of the gtfs {} to XTP"
            raise PlannerException(msg.format(self.gtfs_name))

    def plan_search(self, time, origin, destination):
        data = dict(
            XFS=self.gtfs_name,
            Date=self.date,
            Time=self.fmt_min(time),
            Origins=self.fmt_loc(origin),
            Destinations=self.fmt_loc(destination)
        )
        data["Allowed Sequences"] = self.allowed
        data["Optimization Type"] = "Monocriterion"
        # hack due to xtp can't handle the ' in the request
        url = self.plan_url + str(data)

        return url.replace("'", '"'), data

    @staticmethod
    def fmt_min(time):
        if isinstance(time, int):
            return "%02d:%02d" % (time / 3600, (time % 3600) / 60)

        if isinstance(time, datetime.datetime):
            return time.strftime("%H:%M")

        raise Exception("Unable to format time", time, type(time))

    @staticmethod
    def fmt_loc(loc):
        """XTP demands "lon, lat" format hence we have (lat , lon)
        """
        return [loc]

    def search(self, time, origin, destination):
        query, data = self.plan_search(time, origin, destination)
        response = requests.get(
            query,
            timeout=1200,
            proxies={"http": None}
        )

        return response.json().get("Activated Sequences")

    def prepare_planner_cls(self):
        if not self.gtfs_exist():
            self.push_gtfs_to_xtp()

        return planner_closure(self)

    
def planner_closure(cls):
    """Encapsulates a trip planner
    """
    def plan_travel(*args, **kwargs):
        """Calls the planenr go function without the gtfs argument
        """
        return cls.search(*args, **kwargs)

    return plan_travel


class PlannerException(Exception):
    """Class to generate the raise exception."""

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message