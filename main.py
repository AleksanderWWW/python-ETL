"""
Main runnable module
"""
import yaml

# read config
with open("config.yaml", "r", encoding="utf-8") as config_fp:
    _CONFIG = yaml.safe_load(config_fp)


def main():
    ...
