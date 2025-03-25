
# AAFC Data Scanner 2025

<img src="icons/aafc_data_scanner_icon.ico" alt="AAFC Data Scanner Icon" width="100"/>

## Description

This updated code is made to scan [AAFC](https://agriculture.canada.ca/en)'s open data information on Canada's [Open Government Portal](https://search.open.canada.ca/opendata/) and the departmental [AAFC Open Data Catalogue](https://data-catalogue-donnees.agr.gc.ca/dataset/), to provide the user with a complete inventory of datasets and resources. The repository also includes a Power BI report that fetches the generated data to provide visualizations and statistics about all the published data.


## Content

- **aafc_data_scanner/** \
module containting the main code
    - **\_\_init\_\_.py** \
makes the folder a package
    - **\_\_main\_\_.py** \
main code to run for the app to start
    - **constants.py** \
contains project-wide constant variables
    - **data.py** \
module to retrieve useful data as dictionaries or data frames (Note: originally 
stored in helper files, but exporting the project as a single executable 
file requires no attached data, only code)
    - **helper_functions.py** \
helper functions for other modules to use
    - **inventories.py** \
contains `Inventory` class (main class to collect, store and export data from a given `DataCatalogue`)
    - **tools.py** \
contains `DataCatalogue` and its subclasses, along with `TenaciousSession` class, used by the main program to handle web requests

- **documentation/** ... \
contains data originally used by the program to run properly. This data was later moved to **./aafc_data_scanner/data.py** to allow export as a single executable file, but these were left here for documentation and understanding, along with data schemas of the output inventory tables.

- **icons/**  
    - **aafc_data_scanner_icon.ico**  
application's icon

- **tests/** \
code files for testing (unit tests); to be run from project's main folder using `py -m unittest` (add `-v` for more verbose)
    - **io/** ... \
sample I/O files for testing
    - ...

- **AAFC Open Data.pbix** \
Power BI report generating its visualizations from the exported CSVs in the **inventories/** folder.

- **LICENSE** \
MIT open-source license for use of this code.

- **cli.py** \
entry point to produce executable file

- **poetry.lock** \
lock file for automatic downloading of pinned dependencies with Poetry (see [Installation & Setup > Using Poetry](#using-poetry))

- **pyproject.toml** \
configuration file used by packaging tools, with project's metadata and dependencies

- **README.md** \
this very file, giving general indications about the project and how to use the code

- **requirements.txt** \
contains all dependencies required for this project, along with their version, so they can easily be installed within a local virtual environment (see [Installation & Setup > Using a manual virtual environment venv](#using-a-manual-virtual-environment-venv))



## Installation & Setup

To be able to properly handle dependencies, either use [Poetry](#using-poetry) (current standard for dependencies, packaging and virtual environments handling) or use a [manuel virtual environment **venv**](#using-a-manual-virtual-environment-venv) (more traditional way).


### Using Poetry

* If you don't have Poetry yet, you can install it following the steps on [this page](https://python-poetry.org/docs/#installation). Make sure you use pipx so Poetry gets installed in a separate location, in its own environment, and is automatically added to the path.
* Open a terminal in your current repository and enter:

```powershell
poetry install
```

* Poetry will automatically create a virtual environment in a separate location, then resolve and install the dependencies using **pyproject.toml** and **poetry.lock** files.
* To run the code in the virtualenvironment set by Poetry, use:

```powershell
poetry run py #<your python module>
```

(Replace `py` by `python` or `python3`, depending on your OS and how Python is installed on your computer and in your PATH).

* Alternatively, you can open a new shell set in that virtualenvironment, using:

```
poetry shell
```

From there, you can run your code directly with `py`/`python`/`python3`. You can exit that virtualenvironment-contained shell with the command `exit`.


### Using a manual virtual environment venv

* Have Python installed on your computer. You can download it [here](https://www.python.org/downloads/). The version used for this project was Python 3.12.0.
* To have the right dependencies, you will need to set up a virtual environment **venv** (already included with Python, so you don't need to install it) which will allow you to directly install all required dependencies in their proper versions from **requirements.txt**. Setting up the venv will be done differently on [Windows](#venv-setup-on-windows) and [Mac](#venv-setup-on-mac).


#### **venv** setup on Windows

Open a command line prompt in your local repo of this project and run:

```powershell
py -m venv venv
venv\Scripts\activate
py -m pip install -r requirements.txt

# to activate the environement, use:
venv\Scripts\activate
# to deactivate it / exit from the environment, use:
deactivate
```

Now that the venv is set up locally on your repo, make sure **venv/** is in your **.gitignore** file.


#### **venv** setup on Mac

Open a command line prompt in your local repo of this project and run:

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt

# to activate the environement, use:
source venv/bin/activate
# to deactivate it / exit from the environment, use:
deactivate
```

Now that the venv is set up locally on your repo, make sure **venv/** is in your **.gitignore** file.


## Extra documentation for both Users & Developers

 See the [AAFC Data Scanner documentation](https://001gc.sharepoint.com/:w:/s/75731/Ee5Nb4ESXBhNn1KU_LrZZvkBz-WVjEy80uqnBFzSX1YJ-Q?e=Wok5wX) for extra information, for both users and developers.


## How to export as a single executable file

Open a terminal in the project's main folder. First, make sure poetry is installed (`poetry install`) and the environment is activated (`poetry shell`).
You can then enter:

```powershell
pyinstaller --onefile -n "aafc_data_scanner" -i .\icons\aafc_data_scanner_icon.ico .\cli.py
```

The excutable file will be placed in a newly-created **dist** folder.
