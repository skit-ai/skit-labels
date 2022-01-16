"""
Core datatypes involved in testing and impact analysis system
"""

import json
import uuid
from abc import ABC, abstractmethod
from typing import List

import attr
from pydash import py_


class Task(ABC):
    """
    This is basically X for any machine learning model. When this object
    contains a not None `tags` property, that is considered as it's y.
    """

    tags = attr.ib(default=None)
    is_gold: bool = attr.ib(default=False)

    @property
    @abstractmethod
    def id(self):
        """
        Unique identifier for this case
        """
        ...

    def __eq__(self, other):
        return self.id == other.id


class DictTask(Task, dict):
    """
    Dictionary task wrapping around any form of data.
    """

    id: str = attr.ib()

    @staticmethod
    def from_dict(d, id):
        task = DictTask(d)
        task.id = id
        return task


@attr.s(slots=True)
class SimulatedTurn:
    """
    One turn of each simulated call.

    Fields here have similar meaning as in the conversations table in
    production database.
    """

    id: str = attr.ib()
    type: str = attr.ib()
    sub_type: str = attr.ib()
    text: str = attr.ib()
    prediction = attr.ib()

    @staticmethod
    def from_dict(d, make_prediction=None):
        if make_prediction:
            pred = make_prediction(d["text"])
        else:
            if isinstance(d["prediction"], dict):
                pred = d["prediction"]
            else:
                pred = json.loads(d["prediction"])

        return SimulatedTurn(
            id=d["id"],
            type=d["type"],
            sub_type=d["sub_type"],
            text=d["text"],
            prediction=pred,
        )


@attr.s(slots=True)
class SimulatedCallTask(Task):
    """
    Task representing a single simulated call coming from plute's user
    simulator scripts.
    make_prediction is a function which generates prediction out of text
    """

    id: str = attr.ib()
    turns: List[SimulatedTurn] = attr.ib()

    @staticmethod
    def from_dict(d, make_prediction=None):
        return SimulatedCallTask(
            id=d["id"],
            turns=[
                SimulatedTurn.from_dict(td, make_prediction=make_prediction)
                for td in d["turns"]
                # NOTE: We remove bot turns (type = RESPONSE) for now. If you want
                #       that too for training models, note the following points:
                #       1. the simulated bot texts might not be the one used in production
                #       2. we don't get bot turns in plute production as of now.
                #       One you have considered both these cases, you can go ahead
                #       with a parser for bot turns too.
                if ("type" not in td) or (td["type"] == "INPUT")
            ],
        )


@attr.s(slots=True)
class CallTranscriptionTurn:
    """
    One turn of each agent-user transcribed call.

    Fields here have similar meaning as in the conversations table in
    production database.
    """

    id: str = attr.ib()
    type: str = attr.ib()
    text: str = attr.ib()

    @staticmethod
    def from_dict(d):

        return CallTranscriptionTurn(id=d["id"], type=d["type"], text=d["text"])


@attr.s(slots=True)
class CallTranscriptionTask(Task):
    """
    Task for using agent-user transcribed calls.
    """

    id: str = attr.ib()
    turns: List[CallTranscriptionTurn] = attr.ib()

    @staticmethod
    def from_dict(d, id):
        return CallTranscriptionTask(
            id=d["id"] if "id" in d else id,
            turns=[
                CallTranscriptionTurn.from_dict(td)
                for td in d["turns"]
                # NOTE: We remove bot turns (type = RESPONSE) for now. If you want
                #       that too for training models, note the following points:
                #       1. the simulated bot texts might not be the one used in production
                #       2. we don't get bot turns in plute production as of now.
                #       One you have considered both these cases, you can go ahead
                #       with a parser for bot turns too.
                if ("type" not in td) or (td["type"] == "INPUT")
            ],
        )


@attr.s(slots=True)
class AudioSegmentTask(Task):
    """
    Task for audio segment tagging like for VAD, diarization etc.
    """

    conversation_id: int = attr.ib()
    audio_url: str = attr.ib()

    @property
    def id(self):
        return self.conversation_id

    @staticmethod
    def from_dict(d):
        it = py_.pick(d, ["conversation_id", "audio_url"])
        return AudioSegmentTask(**it)


@attr.s(slots=True)
class ConversationTask(Task):
    """
    Plute style conversation (turn) task coming from tog.
    """

    alternatives = attr.ib()
    data_id = attr.ib()
    audio_url: str = attr.ib()
    call_uuid: str = attr.ib()
    conversation_uuid: str = attr.ib()
    state: str = attr.ib()
    reftime: str = attr.ib()
    prediction = attr.ib(default=None)
    raw = attr.ib(default=None)

    @property
    def id(self):
        return self.conversation_uuid

    @staticmethod
    def from_dict(d):
        call_uuid = (
            d.get("call_uuid") if d.get("call_uuid") is not None else d.get("call_id")
        )
        conversation_uuid = (
            d.get("conversation_uuid")
            if d.get("conversation_uuid") is not None
            else d.get("conversation_id")
        )
        if call_uuid is None or conversation_uuid is None:
            raise ValueError(f"No reference for call or conversation. {d.keys()}")
        if d.get("alternatives"):
            d["alternatives"] = json.dumps(d["alternatives"], ensure_ascii=False)
        return ConversationTask(
            **{
                **py_.pick(
                    d,
                    [
                        "alternatives",
                        "audio_url",
                        "state",
                        "reftime",
                        "prediction",
                    ],
                ),
                "data_id": str(conversation_uuid),
                "raw": d,
                "call_uuid": str(call_uuid),
                "conversation_uuid": str(conversation_uuid),
            }
        )


@attr.s(slots=True)
class DataGenerationTask(Task):
    """
    Data Generation Task.
    Direct intent-entity recording job.
    """

    id: str = attr.ib()

    @staticmethod
    def from_dict(d):
        return DataGenerationTask(id=d.get("id", uuid.uuid4().hex))
