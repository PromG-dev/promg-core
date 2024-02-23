import yaml
from pathlib import Path


class Configuration:
    def __init__(self, semantic_header_path, dataset_description_path, db_name, uri, user, password,
                 verbose, batch_size, use_sample, use_preprocessed_files, import_directory=None):
        self.semantic_header_path = semantic_header_path
        self.dataset_description_path = dataset_description_path
        self.import_directory = import_directory
        self.db_name = db_name
        self.uri = uri
        self.user = user
        self.password = password
        self.verbose = verbose
        self.batch_size = batch_size
        self.use_sample = use_sample
        self.use_preprocessed_files = use_preprocessed_files

    @staticmethod
    def init_conf_with_config_file(path=Path("config.yaml")):
        config = yaml.safe_load(open(path))
        _semantic_header_path = Path(config['semantic_header_path'])
        _dataset_description_path = Path(config['dataset_description_path'])
        if "import_directory" in config:
            _import_directory = Path(config['import_directory'])
        else:
            _import_directory = None
        _db_name = config["db_name"]
        _uri = config["uri"]
        _user = config["user"]
        _password = config["password"]
        _verbose = config["verbose"]
        _batch_size = config["batch_size"]
        _use_sample = config["use_sample"]
        _use_preprocessed_files = config["use_preprocessed_files"]

        return Configuration(semantic_header_path=_semantic_header_path,
                             dataset_description_path=_dataset_description_path,
                             import_directory=_import_directory,
                             db_name=_db_name,
                             uri=_uri,
                             user=_user,
                             password=_password,
                             verbose=_verbose,
                             batch_size=_batch_size,
                             use_sample=_use_sample,
                             use_preprocessed_files=_use_preprocessed_files)
