import json

class JsonItemClass:
    name: str
    type: str
    config: dict
    input: str

    def __init__(self, name, type, input, config):
        self.type = type
        self.config = config
        self.input = input
        self.name = name

class InputClass(JsonItemClass):
    spark_options: dict [str, str]

    def __init__(self, name, type, config, spark_options):
        self.name = name
        self.type = type
        self.config = config
        self.spark_options = spark_options  

class DataflowClass:
    name: str
    inputs: list[InputClass]
    transformations: list[JsonItemClass]
    outputs: list[JsonItemClass]


def get_json(json_path):
    """
    Read JSON with json library
    """
    with open(json_path) as f:
        return json.load(f)
    
def get_inputs_from_dataflow(dataflow):
    inputs = []
    for input in dataflow["inputs"]:
        obj_input = InputClass(input["name"], input["type"], input["config"], input["spark_options"])
        inputs.append(obj_input)
    return inputs

def get_transformations_from_dataflow(dataflow):
    transformations = []
    for transformation in dataflow["transformations"]:
        obj_trans = JsonItemClass(transformation["name"], transformation["type"], transformation["input"], transformation["config"])
        transformations.append(obj_trans)
    return transformations

def get_outputs_from_dataflow(dataflow):
    outputs = []
    for output in dataflow["outputs"]:
        obj_output = JsonItemClass(output["name"], output["type"], output["input"], output["config"])
        outputs.append(obj_output)
    return outputs

    
def get_dataflows_from_json(json_path):
    dataflows = []

    metadata_json = get_json(json_path)
    
    for dataflow in metadata_json["dataflows"]:
        dataf = DataflowClass
        dataf.name = dataflow["name"]
        # Read inputs
        dataf.inputs = get_inputs_from_dataflow(dataflow)

        # Read transformations       
        dataf.transformations = get_transformations_from_dataflow(dataflow)

        #Read outputs
        dataf.outputs = get_outputs_from_dataflow(dataflow)
        
        dataflows.append(dataf)
        
    return dataflows