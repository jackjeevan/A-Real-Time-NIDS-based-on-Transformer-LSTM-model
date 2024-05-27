# Flow Generator

## Creating Virtualenv
```sh
virtualenv venv
```

## Activating Virtualenv
```sh
source venv/bin/activate
```

## Installing dependencies

```sh
pip install -r requirements.txt
```

## Usage
```sh
usage: python main.py [-h] (-i INPUT_INTERFACE | -f INPUT_FILE) [-c] [--output-file OUTPUT]

A Machine Learning based Real-time Intrusion Detection System in Network

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_INTERFACE, --interface INPUT_INTERFACE
                        This interface will be used to capture traffic in order to convert it into
                        the flow.
  -f INPUT_FILE, --file INPUT_FILE
                        This file will be converted to the flow.
  -c, --csv, --flow     The output will be store in the form of csv in output file.
  --output-file OUTPUT  default: flow.csv, The file output will be written to.
```

## Montoring interface in real time
```sh
python main.py -i interface -c
```
> Root privilege is require to performe analysis in real-time.
