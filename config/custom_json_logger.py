import datetime as dt
import json
import logging
from typing import override

class custom_json_logger(logging.Formatter):
    def __init__(
        self,
        *,
        format_keys: dict[str,str] | None = None,
    ):
        super().__init__()
        self.format_keys = format_keys if format_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        record_dict = self._prepare_log_dict(record)
        return json.dumps(record_dict, default=str)
    
    def _prepare_log_dict(self, record: logging.LogRecord) -> dict:
        always_fields = {
            "message" : record.getMessage(),
            "timestamp" : dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)
        
        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)
        
        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.format_keys.items()
        }
        message.update(always_fields)
        return message

