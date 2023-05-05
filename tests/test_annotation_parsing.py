
from skit_labels.labelstudio import annotations



def test_extract_incorrect_transcript():

    tag = [
            {"id": "_FW0-mqUjQ", "type": "taxonomy", "value": {"taxonomy": [["_wheel_related_"]]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "ri87XjuiK7", "type": "choices", "value": {"choices": ["Incorrect Transcript"]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"}
        ]
    
    assert annotations.extract_incorrect_transcript(tag) == True


def test_extract_gold_ready_for_training():

    tag = [
            {"id": "i1ItHBjQao", "type": "taxonomy", "value": {"taxonomy": [["_repeat_"]]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "UZDXRwSyMe", "type": "taxonomy", "value": {"taxonomy": [["[GOLD] READY FOR TRAINING"]]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"}
        ]
    
    assert annotations.extract_gold_ready_for_training(tag) == True


def test_extract_intent_in_new_data_format():

    tag = [
            {"id": "i1ItHBjQao", "type": "taxonomy", "value": {"taxonomy": [["_repeat_"]]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
            {"id": "UZDXRwSyMe", "type": "taxonomy", "value": {"taxonomy": [["[GOLD] READY FOR TRAINING"]]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"}
        ]
    
    assert annotations.extract_intent(tag) == '_repeat_'


def test_extract_intent_in_old_data_format():

    tag = [
            {"id": "UZDXRwSyMe", "type": "taxonomy", "value": {"taxonomy": [["[GOLD] READY FOR TRAINING"]]}, "origin": "manual", "to_name": "audio", "from_name": "gold-data"},
            {"id": "i1ItHBjQao", "type": "taxonomy", "value": {"choices": ["_repeat_"]}, "origin": "manual", "to_name": "audio", "from_name": "tag"}, 
        ]
    
    assert annotations.extract_intent(tag) == '_repeat_'

