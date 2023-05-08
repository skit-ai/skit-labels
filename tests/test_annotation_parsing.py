
from skit_labels import constants as const
from skit_labels.labelstudio import annotations



def test_extract_incorrect_transcript():

    tag = [
            {"id": "_FW0-mqUjQ", "type": "taxonomy", "value": {"taxonomy": [["_wheel_related_"]]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "ri87XjuiK7", "type": "choices", "value": {"choices": ["Incorrect Transcript"]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"}
        ]
    
    assert annotations.extract_annotation_related_to_intents(tag, const.FROM_NAME_GOLD_DATA, const.INCORRECT_TRANSCRIPT) == True
    assert annotations.extract_annotation_related_to_intents(tag, const.FROM_NAME_GOLD_DATA, const.GOLD_READY_FOR_TRAINING) == False


def test_extract_gold_ready_for_training():

    # [GOLD] READY FOR TRAINING
    upper_case_tag = [
            {"id": "i1ItHBjQao", "type": "taxonomy", "value": {"taxonomy": [["_repeat_"]]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "UZDXRwSyMe", "type": "taxonomy", "value": {"choices": ["[GOLD] READY FOR TRAINING"]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"}
        ]
    
    
    assert annotations.extract_annotation_related_to_intents(upper_case_tag, const.FROM_NAME_GOLD_DATA, const.GOLD_READY_FOR_TRAINING) == True
    assert annotations.extract_annotation_related_to_intents(upper_case_tag, const.FROM_NAME_GOLD_DATA, const.INCORRECT_TRANSCRIPT) == False

    # [GOLD] Ready for Training
    lower_case_tag = [{'id': 'SwhhaeW7Y3', 'type': 'choices', 'value': {'choices': ['[GOLD] Ready for Training']}, 'origin': 'manual', 'to_name': 'audio', 'from_name': 'gold-data'}, 
           {'id': 'SzopXVMrLj', 'type': 'taxonomy', 'value': {'taxonomy': [['application_status']]}, 'origin': 'manual', 'to_name': 'audio', 'from_name': 'tag'}
        ]
    
    assert annotations.extract_annotation_related_to_intents(lower_case_tag, const.FROM_NAME_GOLD_DATA, const.GOLD_READY_FOR_TRAINING) == True
    assert annotations.extract_annotation_related_to_intents(lower_case_tag, const.FROM_NAME_GOLD_DATA, const.INCORRECT_TRANSCRIPT) == False




def test_extract_intent_in_new_data_format():

    tag = [
            {"id": "i1ItHBjQao", "type": "taxonomy", "value": {"taxonomy": [["_repeat_"]]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "UZDXRwSyMe", "type": "taxonomy", "value": {"choices": ["[GOLD] READY FOR TRAINING"]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"}
        ]
    
    assert annotations.extract_annotation_related_to_intents(tag, const.FROM_NAME_INTENT, const.FROM_NAME_INTENT) == '_repeat_'


def test_extract_intent_in_old_data_format():

    tag = [
            {"id": "i1ItHBjQao", "type": "taxonomy", "value": {"choices": ["_repeat_"]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "UZDXRwSyMe", "type": "taxonomy", "value": {"choices": ["[GOLD] READY FOR TRAINING"]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"},
        ]
    
    assert annotations.extract_annotation_related_to_intents(tag, const.FROM_NAME_INTENT, const.FROM_NAME_INTENT) == '_repeat_'



