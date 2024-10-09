import argparse
import signal
import time
import sys
import threading
import logging
import json
import yaml
import requests

from adsb_actions.adsbactions import AdsbActions

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

LOW_FREQ_DELAY = 60  # 250 kts head-on closure over 60 sec = 4 nm. 40 kts = .6 nm
API_RATE_LIMIT = 1 # seconds between API queries
DEACTIVATE_SECS = 30 # no callback in this amount of time = deactivate the airport
EXPIRE_SECS = 31 # expire aircraft not seen in this many seconds
INNER_PROX_THRESH = 2.5
INNER_PROX_ALT = 2500
OUTFILE = "/tmp/output_events.txt"   # matching events go here
ALL_DATA_OUT = "/tmp/all_data.json"  # place to save all received data, for reproducibility

class AirportState:
    def __init__(self, name, latlongring, adsb_actions, logfile):
        self.name = name
        self.latlongring = latlongring
        self.active = False
        self.last_checked = 0
        self.last_activated = 0
        self.adsb_actions = adsb_actions
        self.logfile = logfile

    def call_api_and_process(self):
        logger.debug(f"Doing API query for for {self.name}")

        url = f"https://api.airplanes.live/v2/point/{self.latlongring[1]}/{self.latlongring[2]}/{self.latlongring[0]}"

        start_time = time.time()

        # Issue query
        try:
            response = requests.get(url, timeout=10)
            json_data = response.json()
        except Exception as e:      # pylint: disable=broad-except
            logger.error(f"error in API query: {str(e)}")
            return

        logger.info(f"got {len(json_data['ac'])} flights")

        # Process data from API call.
        # XXX should be optimized to not go to string then back to json:
        json_list = ""      # list of json objects, a weird format
        for line in json_data['ac']:
            line['now'] = json_data['now'] / 1000
            json_list += json.dumps(line) + "\n"

        self.logfile.write(json_list)
        self.adsb_actions.loop(string_data=json_list)
        self.last_checked = done_time = time.time()

        if (done_time - start_time) < API_RATE_LIMIT:
            time.sleep(API_RATE_LIMIT - (done_time - start_time))

class Event:
    def __init__(self, flight1, flight2, airport):
        self.f1str = flight1.to_str()
        self.f2str = flight2.to_str()
        self.f1loc = flight1.lastloc
        self.f2loc = flight2.lastloc
        self.airport = airport

    def to_str(self):
        return (f"Event at {self.airport} {int(self.f1loc.now)} dist "
                f"{abs(self.f1loc - self.f2loc)} "
                f"{self.f1str} === {self.f2str}")

class MonitorThread:
    def __init__(self, adsb_actions):
        self.airports = {}
        self.airports_lock = threading.Lock()
        self.monitor_thread = threading.Thread(target=self.monitor_thread_loop)
        self.adsb_actions = adsb_actions
        self.event_dict = {}
        self.event_file = open(OUTFILE, "w")
        self.event_file.write("log start at " + str(time.time()) + "\n")
        self.thread_running = False

    def run(self):
        self.thread_running = True
        self.monitor_thread.start()

    def dump_events(self):
        for event in self.event_dict.values():
            logger.debug(event.to_str())

    def handle_exit(self, *_):
        logger.debug("Dumping all events:")
        self.dump_events()
        self.event_file.close()
        list(self.airports.values())[0].logfile.close()
        sys.exit(0)

    def add_airport(self, name, latlongring, logfile):
        logger.info(f"Adding {name}")
        with self.airports_lock:
            self.airports[name] = AirportState(name, latlongring,
                                               self.adsb_actions, logfile)

    def activate_airport(self, name):
        if not self.thread_running:     # XXX hack
            return

        # print(f"Activating {name}")
        with self.airports_lock:
            self.airports[name].active = True
            self.airports[name].last_activated = time.time()

    def deactivate_airport(self, name):
        if not self.thread_running:
            return

        logger.info(f"Deactivating {name}")
        with self.airports_lock:
            self.airports[name].active = False

    def monitor_thread_loop(self):
        while True:
            start_loop_time = time.time()

            ret = self.check_all_airports()
            if ret == 0:        # didn't sleep in check_all_airports()
                time.sleep(1)

    def check_all_airports(self):
        """Query vicinity of each airport at a frequency determined according
        to each one's active state.  Returns number of API queries issued."""
        query_ctr = 0

        for airport in self.airports.values():
            if airport.active:
                if airport.last_activated + DEACTIVATE_SECS < time.time():
                    self.deactivate_airport(airport.name)
                    continue
                airport.call_api_and_process()
                query_ctr += 1
            else:
                if time.time() - airport.last_checked > LOW_FREQ_DELAY:
                    airport.call_api_and_process()
                    query_ctr += 1
                    logger.debug("low freq check done")
        logger.info(
            f"Done checking all, {len(self.event_dict)} events stored")
        return query_ctr

    def prox_callback(self, flight, flight2):
        try:
            airport = flight.flags['note']
        except:
            logger.error("No airport in flags")
            raise
        logger.info(
            f"prox callback activating {airport}: {flight.flight_id} {flight2.flight_id}")

        self.activate_airport(airport)

        flight_dist = abs(flight.lastloc - flight2.lastloc)
        flight_alt_delta = abs(flight.lastloc.alt_baro - flight2.lastloc.alt_baro)

        if flight_dist < INNER_PROX_THRESH and flight_alt_delta < INNER_PROX_ALT:
            has_logged = flight.flags.get('logged', False)
            if has_logged:
                logger.info(f"*** already logged {airport}: {flight_dist}nm, "
                      f"{flight.to_str()}, {flight2.to_str()}")
                return
            flight.flags['logged'] = True
            logger.info(f"*** below inner thresh range {airport}: {flight_dist}nm, "
                  f"{flight.to_str()}, {flight2.to_str()}")
            event = Event(flight, flight2, airport)
            self.event_dict[flight.lastloc.now] = event
            self.event_file.write(f"{event.to_str()}\n")

def generate_dicts_from_file(file_path):
    with open(file_path, "r") as file:
        for line in file:
            yield json.loads(line)

if __name__ == "__main__":
    logging.basicConfig(
    format='%(levelname)s: %(message)s', level=logging.INFO)

    # get yaml path from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path", help="Path to yaml file")
    parser.add_argument(
        "--data_file", type=str, help="Path to pre-recorded data file", required=False)
    args = parser.parse_args()
    yaml_path = args.yaml_path

    # read in yaml file
    with open(yaml_path, 'r') as stream:
        try:
            yaml_data = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.error(exc)
            sys.exit(-1)

    # open logfile for all data
    all_data_file = open(ALL_DATA_OUT, "w")

    # set up processing environment
    adsb_actions = AdsbActions(yaml_data=yaml_data, expire_secs=EXPIRE_SECS,
                               pedantic=True)
    monitor_thread = MonitorThread(adsb_actions)

    signal.signal(signal.SIGINT, monitor_thread.handle_exit)

    # register callback
    adsb_actions.register_callback("prox_cb",
                                   monitor_thread.prox_callback)

    if args.data_file:
        # just read in data from file, no threading needed
        iterator = generate_dicts_from_file(args.data_file)
        adsb_actions.loop(iterator_data=iterator)
    else:
        # read in airport details to monitor
        try:
            for rulename, rulebody in yaml_data['rules'].items():
                if rulebody['conditions']['latlongring']:
                    monitor_thread.add_airport(rulename,
                                            rulebody['conditions']['latlongring'],
                                            all_data_file)
        except Exception as ex:      # pylint: disable=broad-except
            logger.error("error in yaml file: " + str(ex))

        # start monitoring thread
        monitor_thread.run()
