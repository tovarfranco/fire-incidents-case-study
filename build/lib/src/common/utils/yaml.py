import yaml
import boto3

def get_yaml(yaml_path):
    with open(yaml_path) as file:
        return yaml.load(file, Loader=yaml.FullLoader)
    
def load_yaml(filename: str):
    """Loads a YAML configuration file from s3

    .. versionadded:: 0.0.1

    Parameters
    ----------
        filename : str
            The filname of your yaml.
    Returns
    -------
    Dictionary with values defined in file
        dict.
    """
    data = None

    if filename.startswith("s3://"):
        path_file = filename.split("//")[1].split("/")
        bucket = path_file.pop(0)
        file = "/".join(path_file)

        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, file)
        data = yaml.load(obj.get()["Body"], Loader=yaml.SafeLoader)

    elif path.exists(filename):
        with open(filename, "r") as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
    else:
        print(f"File does not exist: {filename}")

    return data