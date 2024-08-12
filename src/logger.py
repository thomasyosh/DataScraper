import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s : %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
    handlers=[
        logging.FileHandler(filename="result.log", encoding="utf-8", mode="a"),
        logging.StreamHandler()]
    )