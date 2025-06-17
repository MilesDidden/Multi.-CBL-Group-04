# Multi-disciplinary CBL - Police Allocation in London

This project analyzes historical crime data in London and proposes police allocation strategies based on spatial and temporal crime patterns. The main goal is to support data-driven decision-making for public safety.

> **Note:** In order to run this project, you need access to the crime database.  
> This can be requested from **Group 04, Multidisciplinary CBL 2024–2025**.

---

## Project Structure
```
> MULTI.-CBL-GROUP-04/          # Parent folder
|
|-- model/                        # Contains model dependencies  
|-- utils/                        # Contains utilities for the Dash interactive tool  
|-- data/                         # Contains the database & a .graphml map of London for operational use cases of the model  
|-- main_db.py                    # .py file for the creation of the database  
|-- dashboard_app.py              # .py file for running the entire data pipeline  
|-- requirements.txt              # .txt file used for installing dependencies
```

---

# Setup instructions

### 1. Obtain the source code ...

#### ..., By Downloading
- Head over to https://github.com/MilesDidden/Multi.-CBL-Group-04/releases/tag/PreRelease, and download the zip file.
- Extract the zip file to a folder of your liking. 

#### ..., By git cloning
- In command prompt, execute the following:
```bash
git clone https://github.com/MilesDidden/Multi.-CBL-Group-04.git
```


### 2. Create and activate a Conda environment (recommended)
*Create a new python environment:*
```bash
conda create -n police-allocation-env python=3.11
```

*Activate your newly created python environment:*
```bash
conda activate police-allocation-env
```

*Change directory to the location where you extracted the repository:*  
```bash
cd C:\location\of\your\repository\MULTI.-CBL-GROUP-04
```

*Install dependencies:*
```bash
pip install -r requirements.txt
```

---

## Running the Code
After setting up your environment and obtaining the required database, you can:
1. Explore the code.
2. Run the interactive tool pipeline.

### Running code (using a GUI) --> Recommended
1. Open the parent folder: "MULTI.-CBL-GROUP-04".
2. Open the file "dashboard_app.py".
3. Use the built-in run button from the GUI.

### Running code (in command prompt)
1. Open cmd.exe
2. Navigate to the folder:
```bash
cd C:\location\of\your\repository\MULTI.-CBL-GROUP-04
```
3. Run the dashboard app file by using the following command line: (***Note: Ensure your python environment is activated!***)
```bash
python dashboard_app.py
```
