import logging
import helics as h
import json
from pathlib import Path
from datetime import datetime
from oedisi.types.common import BrokerConfig
from oedisi.types.data_types import (
    Topology,
)
from pydantic import BaseModel
from topology_splitter import split_topology

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class ComponentParameters(BaseModel):
    name: str


class Subscriptions(object):
    t: h.HelicsInput


class Publications(object):
    t0: h.HelicsPublication
    t1: h.HelicsPublication
    t2: h.HelicsPublication
    t3: h.HelicsPublication
    t4: h.HelicsPublication


class HubFederate(object):
    def __init__(self, broker_config) -> None:
        self.sub = Subscriptions()
        self.pub = Publications()
        self.load_static_inputs()
        self.load_input_mapping()
        self.initilize(broker_config)
        self.load_component_definition()
        self.register_subscription()
        self.register_publication()

    def load_component_definition(self) -> None:
        path = Path(__file__).parent / "component_definition.json"
        with open(path, "r", encoding="UTF-8") as file:
            self.component_config = json.load(file)

    def load_input_mapping(self):
        path = Path(__file__).parent / "input_mapping.json"
        with open(path, "r", encoding="UTF-8") as file:
            self.inputs = json.load(file)

    def load_static_inputs(self):
        path = Path(__file__).parent / "static_inputs.json"
        with open(path, "r", encoding="UTF-8") as file:
            config = json.load(file)

        self.static = ComponentParameters(name=config.name)

    def initilize(self, broker_config) -> None:
        self.info = h.helicsCreateFederateInfo()
        self.info.core_name = self.static.name
        self.info.core_type = h.HELICS_CORE_TYPE_ZMQ
        self.info.core_init = "--federates=1"

        # h.helicsFederateInfoSetTimeProperty(self.info, h.helics_property_time_delta, 0.01)
        # h.helicsFederateSetFlagOption(self.fed, h.helics_flag_slow_responding, True)
        h.helicsFederateInfoSetBroker(self.info, broker_config.broker_ip)
        h.helicsFederateInfoSetBrokerPort(self.info, broker_config.broker_port)

        self.fed = h.helicsCreateValueFederate(self.static.name, self.info)
        h.helicsFederateSetTimeProperty(self.fed, h.HELICS_PROPERTY_TIME_PERIOD, 1)

    def register_subscription(self) -> None:
        self.sub.t = self.fed.register_subscription(self.inputs["sub_t"], "")

    def register_publication(self) -> None:
        self.pub.t0 = self.fed.register_publication(
            f"pub_t0", h.HELICS_DATA_TYPE_STRING, ""
        )
        self.pub.t1 = self.fed.register_publication(
            f"pub_t1", h.HELICS_DATA_TYPE_STRING, ""
        )
        self.pub.t2 = self.fed.register_publication(
            f"pub_t2", h.HELICS_DATA_TYPE_STRING, ""
        )
        self.pub.t3 = self.fed.register_publication(
            f"pub_t3", h.HELICS_DATA_TYPE_STRING, ""
        )
        self.pub.t4 = self.fed.register_publication(
            f"pub_t4", h.HELICS_DATA_TYPE_STRING, ""
        )

    def publish_all(self):
        t = Topology.parse_obj(self.sub.t.json)
        areas = split_topology(t)
        pubs = [pub for _, pub in vars(self.pub).items()]
        for i, area in enumerate(areas):
            pubs[i].publish(area.json())

    def run(self) -> None:
        logger.info(f"Federate connected: {datetime.now()}")
        h.helicsFederateEnterExecutingMode(self.fed)

        # setting up time properties
        update_interval = int(
            h.helicsFederateGetTimeProperty(self.fed, h.HELICS_PROPERTY_TIME_PERIOD)
        )

        granted_time = 0
        while granted_time <= h.HELICS_TIME_MAXTIME:
            request_time = granted_time + update_interval
            logger.debug(f"Requesting time {request_time}")
            granted_time = h.helicsFederateRequestTime(self.fed, request_time)
            logger.debug(f"Granted time {request_time}")

            if self.sub.t.is_updated():
                self.publish_all()

        self.stop()

    def stop(self) -> None:
        h.helicsFederateDisconnect(self.fed)
        h.helicsFederateFree(self.fed)
        h.helicsCloseLibrary()
        logger.info(f"Federate disconnected: {datetime.now()}")


def run_simulator(broker_config: BrokerConfig):
    sfed = HubFederate(broker_config)
    sfed.run()


if __name__ == "__main__":
    schema = ComponentParameters.schema_json(indent=2)
    with open("schema.json", "w") as f:
        f.write(schema)

    run_simulator(BrokerConfig(broker_ip="0.0.0.0"))
