
from typing import Dict, Optional, Union

from loguru import logger

from skit_labels import constants as const



def extract_annotation_related_to_intents(
        tag: Dict, 
        annotation_from_name_value: str, 
        annotation_search_parameter: str
    ) -> Optional[Union[str, bool]]:

    try:

        annotated_information = None
        incorrect_transcript_and_gold_data_constants = [const.INCORRECT_TRANSCRIPT, const.GOLD_READY_FOR_TRAINING]

        # searching for the dictionary which has the sub-annotation, 
        # should have matching "from_name": annotation_from_name_value
        for subset_tag in tag:
            if const.FROM_NAME in subset_tag and subset_tag[const.FROM_NAME] in annotation_from_name_value:
                annotated_information = subset_tag[const.VALUE]
                break

        if annotated_information is not None:

            if const.CHOICES in annotated_information:
                choices_value = annotated_information[const.CHOICES][0]

                # are we searching for intent or the other category (incorrect transcript & gold-ready-for-training)
                # old way was getting tagged with choices
                if annotation_search_parameter == const.FROM_NAME_INTENT:
                    return choices_value
                else:
                    return choices_value.lower() == annotation_search_parameter.lower()

            # new way of intent tagging is done with taxonomy
            elif const.TAXONOMY in annotated_information:
                return annotated_information[const.TAXONOMY][0][0]

    except Exception as e:
        logger.warning(e)

    if annotation_search_parameter in incorrect_transcript_and_gold_data_constants:
        return False
    else:
        return None

