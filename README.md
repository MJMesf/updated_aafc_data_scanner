
# AAFC Data Scanner

[Description](#description) \
[Content](#content) \
[Installation & Setup](#installation--setup) \
[How to use the code](#how-to-use-the-code)

## Description

This code is made to parse [AAFC](https://agriculture.canada.ca/en)'s open data, both on the departmental [AAFC Open Data Catalogue](https://data-catalogue-donnees.agr.gc.ca/dataset/) and on Canada's [Open Government Portal](https://search.open.canada.ca/opendata/), to provide the user with a complete inventory of datasets and resources, along with visualizations and statistics about all the published data.


## Content

- **data_scanner/** \
module containting the main program

- **test_files/** ... \
sample I/O files for testing

- **tests/** ... \
code files for testing (unittests)

- **poetry.lock** \
lock file for automatic downloading of pinned dependencies with Poetry (see [Installation & Setup > Using Poetry](#using-poetry))

- **pyproject.toml** \
configuration file used by packaging tools, with project's metadata and dependencies

- **README.md** \
this very file, giving general indications about the project and how to use it

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

Replace `py` by `python` or `python3`, depending on how Python is installed on your computer and in your PATH.

* Alternatively, you can open a new shell set in that virtualenvironment, using:

```
poetry shell
```

From there, you can run your code directly with `py` (or `python` or `python3`...). You can exit that virtualenvironment-contained shell with the command `exit`.


### Using a manual virtual environment venv

* Have Python installed on your computer. You can download it [here](https://www.python.org/downloads/). The version used for this project was Python 3.12.0.
* To have the right dependencies, you will need to set up a virtual environment **venv** (already included with Python, so you don't need to install it) which will allow you to directly install all required dependencies in their proper versions from **requirements.txt**. Setting up the venv will be done differently on [Windows](#venv-setup-on-windows) and [Mac](#venv-setup-on-mac).


#### **venv** setup on Windows

Open a Bash or command line prompt in your local repo of this project and run:

```powershell
py -m venv venv
venv\Scripts\activate
py -m pip install -r requirements.txt

# to exit from the environment at any moment, use:
deactivate
# to reactivate it, use:
venv\Scripts\activate
```

Now that the venv is set up locally on your repo, make sure **venv/** is in your **.gitignore** file.


#### **venv** setup on Mac

Open a Bash or command line prompt in your local repo of this project and run:

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt

# to exit from the environment at any moment, use:
deactivate
# to reactivate it, use:
source venv/bin/activate
```

Now that the venv is set up locally on your repo, make sure **venv/** is in your **.gitignore** file.


## How to use the code

> **TO BE COMPLETED**
