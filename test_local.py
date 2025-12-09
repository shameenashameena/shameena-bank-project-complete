import json
from DetectFraudFunction.function_code import main

with open("sample_txn.json") as f:
    class DummyEvent:
        def get_json(self):
            return json.load(f)

    main(DummyEvent())
