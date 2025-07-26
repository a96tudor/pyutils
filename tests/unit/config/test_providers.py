import json

import yaml

from pyutils.config.providers import (
    FileConfigProvider,
    JSONConfigProvider,
    YAMLConfigProvider,
)


def test_file_config_provider_loads_and_caches(tmp_path, mocker):
    data = {"a": {"b": {"c": 1}}}
    file_path = tmp_path / "config.json"
    file_path.write_text(json.dumps(data))

    loader = mocker.Mock(side_effect=[data, Exception("loader called twice")])
    provider = FileConfigProvider(loader, file_path)

    secret = provider.provide(["a", "b", "c"])
    with secret.unlock() as sv:
        assert sv.secret == 1

    # second call should not invoke loader again
    secret = provider.provide(["a", "b", "c"])
    with secret.unlock() as sv:
        assert sv.secret == 1
    loader.assert_called_once()


def test_yaml_config_provider(tmp_path):
    content = {"outer": {"inner": "val"}}
    file_path = tmp_path / "config.yaml"
    file_path.write_text(yaml.safe_dump(content))

    provider = YAMLConfigProvider(file_path, base_config_path=["outer"])
    secret = provider.provide(["inner"])
    with secret.unlock() as sv:
        assert sv.secret == "val"


def test_json_config_provider(tmp_path):
    content = {"outer": {"inner": "val"}}
    file_path = tmp_path / "config.json"
    file_path.write_text(json.dumps(content))

    provider = JSONConfigProvider(file_path, base_config_path=["outer"])
    secret = provider.provide(["inner"])
    with secret.unlock() as sv:
        assert sv.secret == "val"
