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
DESCRIBE = "describe"
STATS = "stats"

OUTPUT_FORMAT__CSV = ".csv"
OUTPUT_FORMAT__SQLITE = ".sqlite"

SOURCE__DB = "db"
SOURCE__DVC = "dvc"
