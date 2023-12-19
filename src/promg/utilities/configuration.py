import yaml
from pathlib import Path


class Configuration:
    def __init__(self, path=Path("config.yaml")):
        config = yaml.safe_load(open(path))
        self.semantic_header_path = Path(config['semantic_header_path'])
        self.dataset_description_path = Path(config['dataset_description_path'])
        self.import_directory = Path(config['import_directory'])
        self.db_name = config["db_name"]
        self.uri = config["uri"]
        self.user = config["user"]
        self.password = config["password"]
        self.verbose = config["verbose"]
        self.batch_size = config["batch_size"]
        self.use_sample = config["use_sample"]
        self.use_preprocessed_files = config["use_preprocessed_files"]

