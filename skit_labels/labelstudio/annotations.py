
from typing import Dict, Optional

from loguru import logger

from skit_labels import constants as const

def extract_intent(tag: Dict) -> Optional[str]:

    try:

        intent_tag = None

        # searching for the dictionary which has the intent, should have "from_name":"tag"
        for subset_tag in tag:
            if const.FROM_NAME in subset_tag and subset_tag[const.FROM_NAME] == const.FROM_NAME_INTENT:
                intent_tag = subset_tag[const.VALUE]
                break

        if intent_tag is None: return None

        if const.CHOICES in intent_tag:
            return intent_tag[const.CHOICES][0]
        elif const.TAXONOMY in intent_tag:
            return intent_tag[const.TAXONOMY][0][0]

    except Exception as e:
        logger.warning("please check tag column, it's unparseable to get a single value out")

    return None


def extract_incorrect_transcript(tag: Dict) -> bool:

    try:

        incorrect_transcription_tag = None

        # searching for the dictionary which has the incorrect-transcript, should have "from_name":"gold-data"
        for subset_tag in tag:
            if const.FROM_NAME in subset_tag \
            and subset_tag[const.FROM_NAME] == const.FROM_NAME_GOLD_DATA:
                incorrect_transcription_tag = subset_tag[const.VALUE]
                break

        if incorrect_transcription_tag is None: return False

        if const.CHOICES in incorrect_transcription_tag:
            return incorrect_transcription_tag[const.CHOICES][0] == const.INCORRECT_TRANSCRIPT

    except Exception as e:
        logger.warning(e)

    return False


def extract_gold_ready_for_training(tag: Dict) -> bool:

    try:

        ready_for_training_tag = None

        # searching for the dictionary which has the ready-for-training, should have "from_name":"gold-data"
        for subset_tag in tag:
            if const.FROM_NAME in subset_tag \
            and subset_tag[const.FROM_NAME] == const.FROM_NAME_GOLD_DATA:
                ready_for_training_tag = subset_tag[const.VALUE]
                break

        if ready_for_training_tag is None: return False

        if const.TAXONOMY in ready_for_training_tag:
            return ready_for_training_tag[const.TAXONOMY][0][0] == const.GOLD_READY_FOR_TRAINING

    except Exception as e:
        logger.warning(e)

    return False