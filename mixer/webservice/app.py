"""Mixer web app."""
import flask
import os
from glob import fnmatch
import base64
import hashlib
import zipfile
import multiprocessing

from mixer.gtfs.to_zip.controller import Controller


ABSOLUTE_PATH = os.path.abspath(os.path.dirname(__file__))
UPLOAD_ROOT = os.path.join(ABSOLUTE_PATH, 'uploads')

app = flask.Flask(__name__)


def check_json_payload(*attrs):
    if flask.request.mimetype != 'application/json':
        msg = "Mimetype is '{}' instead of 'application/json'"
        return bad_request(msg.format(flask.request.mimetype))

    if not flask.request.json:
        msg = "No JSON payload found: {}".format(flask.request.response)
        return bad_request(msg)

    for attr in attrs:
        if attr not in flask.request.json:
            msg = "Attribute '{}' not present in payload".format(attr)
            return bad_request(msg)


def get_resource(data):
    body = {
        "success": True,
        "result": data
    }

    return flask.make_response(flask.jsonify(body), 200)


def not_found(error, metainfo={}):
    body = {
        "success": False,
        "error": error
    }
    body.update(metainfo)

    return flask.make_response(flask.jsonify(body), 404)


@app.route("/")
def home():
    return flask.send_from_directory("static", "index.html")


def get_files(path, pattern):
    """Return all the files on a path that follow the patterns."""
    for dirpath, dirnames, filenames in os.walk(path):
        for f in fnmatch.filter(filenames, pattern):
            yield os.path.join(dirpath, f)


def get_gtfs_info(gtfs_file):
    return {
        "id": os.path.basename(gtfs_file.split(".")[0])
    }


def read_schedule(schedule_id):
    sdlpath = os.path.join(UPLOAD_ROOT, 'gtfs')
    result = []
    for sfile in get_files(sdlpath, "*.zip"):
        result.append(get_gtfs_info(sfile))

    if isinstance(schedule_id, str):
        result = filter(lambda x: x['id'] == schedule_id, result)
        if not result:
            return not_found(
                "Schedule with id '%s' does not exist" % schedule_id)

        result = result.pop()

    return result


def prepare_directory(path):
    """Check if a directory path exists, if not create it."""
    if not os.path.exists(path):
        os.makedirs(path)


def save_binary_payload(dirpath, filename, data):
    prepare_directory(dirpath)
    bdata = base64.b64decode(data)
    filepath = os.path.join(dirpath, filename)
    with open(filepath, "wb") as afile:
        afile.write(bdata)

    return filepath


def bad_request(error, metainfo={}):
    body = {
        "success": False,
        "error": error
    }
    body.update(metainfo)

    return flask.make_response(flask.jsonify(body), 400)


def save_new_schedule():
    sdlpath = os.path.join(UPLOAD_ROOT, 'gtfs')
    eschedule = flask.request.json.get('file')

    filepath = save_binary_payload(sdlpath, 'pd.zip', eschedule)

    with zipfile.ZipFile(filepath, 'r') as schedule:
        if schedule.testzip() is not None:
            os.remove(filepath)
            return bad_request("The zip file send is incorrect")

        crc_list = []
        for info in schedule.infolist():
            crc_list.append(str(info.CRC))

        crc_list.sort()
        schedule_hash = hashlib.sha1(
            "".join(crc_list).encode('utf-8')).hexdigest()

    schedule_id = os.path.join(sdlpath, schedule_hash) + ".zip"
    os.rename(filepath, schedule_id)

    return schedule_id


def created_resource(data):
    body = {
        "success": True,
        "result": data
    }

    return flask.make_response(flask.jsonify(body), 201)


@app.route("/mixer/schedules", methods=["GET"])
@app.route("/mixer/schedules/<schedule_id>", methods=["GET"])
def get_schedules(schedule_id=None):
    result = read_schedule(schedule_id)

    return get_resource(result)


class MixLauncher(multiprocessing.Process):
    """A very simple worker process for the simulation.
    """

    def __init__(self, schedule_id):
        super().__init__()
        self.daemon = True
        self.schedule_id = schedule_id

    def run(self):
        """Executes de function launch from this module.
        """
        return Controller(self.schedule_id).main()


@app.route("/mixer/mix", methods=["POST"])
def post_schedules():
    response = check_json_payload('file')
    if response:
        return response
    schedule_id = save_new_schedule()

    return mix(schedule_id)


def mix(schedule_id):
    directory, filename = Controller(schedule_id).main()

    return flask.send_from_directory(directory, filename)


if __name__ == "__main__":
    app.run('0.0.0.0', port=10003, debug=True, use_reloader=False)
