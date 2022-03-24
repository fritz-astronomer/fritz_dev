import asyncio
from hashlib import md5
from typing import Tuple

from airflow.decorators import dag
from airflow.operators.python import PythonOperator, task
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.dates import days_ago


class AmbushTrigger(BaseTrigger):
    def __init__(self, agent="dogs", verb="slip"):
        self.agent = agent
        self.verb = verb
        super().__init__()

    def serialize(self) -> Tuple[str, str]:
        return ("many_triggers.AmbushTrigger", {"agent": self.agent, "verb": self.verb})

    async def run(self):

        hash = md5(self.verb.encode())
        hash.update(self.agent.encode())
        digest = hash.hexdigest()
        supprise_interval = (int(digest, 16) % 12000) / 100.0

        print(f"{self.agent} waiting for {supprise_interval} seconds")
        await asyncio.sleep(supprise_interval)
        yield TriggerEvent({"agent": self.agent, "verb": self.verb})


class AmbushSensorAsync(BaseSensorOperator):
    def __init__(self, verb="slip", agent="dogs", **kwargs):
        self.verb = verb
        self.agent = agent
        super().__init__(**kwargs)

    def execute(self, context):

        print(f"{self.agent} finds a super sneaky hiding place.")
        print(context)
        self.defer(
            trigger=AmbushTrigger(agent=self.agent, verb=self.verb),
            method_name="attack",
        )

    def attack(self, context, event=None):
        # print(f"Cry havoc, and let {event.verb} the {event.agent} of war!")
        print(f'Cry havoc, and let {event["verb"]} the {event["agent"]} of war!')


@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    default_args={"owner": "airflow"},
    catchup=False,
    tags=['core'],
)
def custom_trigger():

    for agent, verb in {
        "alligator": "bellow",
        "antelope": "snort",
        "badger": "growl",
        "bat": " squeak",
        "bear": "roar",
        "bee": "buzz",
        "cat": "meow",
        "cattle": "moo",
        "chicken": "cluck",
        "chinchilla": "squeak",
        "cicada": "chirp",
        "crab": "creak",
        "cricket": "chirp",
        "crow": "caw",
        "deer": "bleat",
        "dog": "bark",
        "dolphin": "click",
        "donkey": "bray",
        "duck": "quack",
        "eagle": "screech",
        "elephant": "trumpet",
        "elk": "bugle",
        "ferret": "dook",
        "fly": "buzz",
        "frog": "croak",
        "gaur": "low",
        "giraffe": "hum",
        "goat": "bleat",
        "goose": "honk",
        "grasshopper": "chirp",
        "hamster": "squeak",
        "hare": "squeak",
        "hippopotamus": "growl",
        "hornet": "buzz",
        "horse": "whinny",
        "hummingbird": "warble",
        "hyena": "laugh",
        "spotted": "hyena",
        "kangaroo": "chortle",
        "koala": "shriek",
        "lion": "roar",
        "magpie": "chatter",
        "monkey": "scream",
        "moose": "bellow",
        "mosquito": "whine",
        "mouse": "squeak",
        "owl": "hoot",
        "ox": "low",
        "parrot": "talk",
        "peacock": "scream",
        "pig": "oink",
        "pigeon": "coo",
        "rabbit": "squeak",
        "raccoon": "trill",
        "rat": "squeak",
        "raven": "caw",
        "rhinoceros": "bellow",
        "rook": "caw",
        "rooster": "crow",
        "seal": "bark",
        "seahorse": "click",
        "sheep": "baa",
        "snake": "hiss",
        "squirrel": "squeak",
        "swan": "trumpet",
        "tapir": "squeak",
        "turkey": "gobble",
        "vulture": "scream",
        "walrus": "groan",
        "whale": "sing",
        "boar": "grumble",
        "wildebeest": "low",
        "wolf": "howl",
        "zebra": "nicker",
    }.items():
        (
            AmbushSensorAsync(task_id=f"{agent}_prepare", verb=verb, agent=agent)
            >> PythonOperator(
                task_id=f"{agent}_ambush",
                python_callable=lambda: f"weary {agent} returns to peacetime",
            )
        )

    AmbushSensorAsync(task_id="Shakespeare_prepare") >> PythonOperator(
        task_id="Richard_III_ambush", python_callable=lambda: "The End"
    )


the_dag = custom_trigger()
