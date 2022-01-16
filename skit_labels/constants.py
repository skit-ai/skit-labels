"""
Common constants used by the package.

We maintain this file to make sure that we don't have inconsistencies in the
constants used. It is common to notice spelling mistakes associated with strings
additionally, we don't get IDE's to automatically suggest values.

consider:

```python
a_dict["key"]
```

and 

```
a_dict[const.KEY]
```

In the latter case, a mature IDE will suggest the KEY constant, reducing time and ensuring consistency.
"""
TASK_TYPE__CONVERSATION = "conversation"
TASK_TYPE__SIMULATED_CALL = "simulated_call"
TASK_TYPE__AUDIO_SEGMENT = "audio_segment"
TASK_TYPE__DICT = "dict"
TASK_TYPE__CALL_TRANSCRIPTION = "call_transcription"
TASK_TYPE__DATA_GENERATION = "data_generation"

TASK_TYPES = [
    TASK_TYPE__CONVERSATION,
    TASK_TYPE__SIMULATED_CALL,
    TASK_TYPE__AUDIO_SEGMENT,
    TASK_TYPE__DICT,
    TASK_TYPE__CALL_TRANSCRIPTION,
    TASK_TYPE__DATA_GENERATION,
]

DOWNLOAD = "download"
UPLOAD = "upload"
DESCRIBE = "describe"
STATS = "stats"

OUTPUT_FORMAT__CSV = ".csv"
OUTPUT_FORMAT__SQLITE = ".sqlite"

SOURCE__DB = "tog"
SOURCE__DVC = "dvc"

DATASET_SERVER_URL = "DATASET_SERVER_URL"
DATA = "data"
DATA_ID = "data_id"
RAW = "raw"
CALL_UUID = "call_uuid"
CONVERSATION_UUID = "conversation_uuid"
UTTERANCES = "utterances"
ALTERNATIVES = "alternatives"
PRIORITY = "priority"
DATA_SOURCE = "data_source"
IS_GOLD = "is_gold"
DEFAULT_SOURCE = "calls"

UPLOAD_DATASET_SCHEMA = {
    "type": "object",
    "properties": {
        "state": {"type": "string"},
        "reftime": {"type": "string"},
        "audio_url": {
            "oneOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "properties": {
                        "bucket": {"type": "string"},
                        "key": {"type": "string"},
                    },
                    "required": ["bucket", "key"],
                },
            ]
        },
        "call_uuid": {"type": "string"},
        "conversation_uuid": {"type": "string"},
        "alternatives": {"type": "array"},
        "filter": {
            "type": "object",
            "properties": {
                "predicted_intent": {"type": "string"},
                "confidence": {"type": "number"},
                "current_state": {"type": "string"},
                "expected_slots": {"type": "array"},
                "acknowledged_slots": {"type": "array"},
                "smalltalk": {"type": "boolean"},
            },
        },
    },
    "required": [
        "state",
        "reftime",
        "audio_url",
    ],
}
