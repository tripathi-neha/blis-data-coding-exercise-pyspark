# Local Test Setup

This guide explains how to run the project tests on Windows. The repository is currently open through a GitHub virtual filesystem (`vscode-vfs://`), which is not a normal local directory. Pytest must run from a local copy of the project because it needs to read the test and CSV files from disk.

## Before cloning: preserve the current changes

A local clone or GitHub ZIP contains committed files only. If the changes in this VS Code workspace are still uncommitted, first commit and push them using the VS Code Source Control view (`Ctrl+Shift+G`). If you cannot push yet, clone/download the repository and copy the changed files, especially `blis/main.py`, into that local copy before testing.

## Prerequisites

Install these on Windows:

1. **Python 3.13** (or another Python version compatible with the selected PySpark release): [python.org/downloads/windows](https://www.python.org/downloads/windows/). Select the option to add Python to PATH. The Python Launcher (`py`) is useful but not required.
2. **Git for Windows**: [git-scm.com/download/win](https://git-scm.com/download/win). Close and reopen terminals after installation so PATH updates.
3. **Java JDK 17 or newer** for the current PySpark 4.x dependency: [Eclipse Temurin](https://adoptium.net/temurin/releases/). Make sure `java` is available in PATH; set `JAVA_HOME` if the installer does not do that for you.

Open a new Command Prompt and verify the tools:

```cmd
py -3.13 --version
git --version
java -version
```

If `py` is not available but `python` is, use `python` in the environment-creation command below.

## Option A: Command Prompt

### 1. Clone the repository locally

```cmd
if not exist "%USERPROFILE%\source\repos" mkdir "%USERPROFILE%\source\repos"
cd /d "%USERPROFILE%\source\repos"
git clone https://github.com/tripathi-neha/blis-data-coding-exercise-pyspark.git blis-data-coding-exercise-pyspark
cd /d "%USERPROFILE%\source\repos\blis-data-coding-exercise-pyspark"
```

If you already cloned it, skip `git clone` and `cd` to the existing project folder. The prompt should end with `blis-data-coding-exercise-pyspark`; it should not be at `C:\Users\neha1` or inside the virtual environment folder.

### 2. Create an isolated environment and install dependencies

The repository's `.gitignore` already ignores a folder named `venv`, so these commands keep the environment out of Git changes:

```cmd
py -3.13 -m venv venv
venv\Scripts\python.exe -m pip install --upgrade pip
venv\Scripts\python.exe -m pip install -r requirements.txt
```

If you used `python` rather than `py -3.13`, replace only the first command with:

```cmd
python -m venv venv
```

### 3. Run the tests from the project root

```cmd
venv\Scripts\python.exe -m pytest test\test_app_functions.py -q
```

No environment activation is required. Calling `venv\Scripts\python.exe` directly also avoids PowerShell execution-policy errors with `Activate.ps1`.

## Option B: VS Code with a local folder

1. Install Python, Git for Windows, and Java as described above.
2. In VS Code, run **Git: Clone** from the Command Palette (`Ctrl+Shift+P`), enter the repository URL, and choose a local parent folder such as `%USERPROFILE%\source\repos`.
3. Open the cloned project folder in VS Code. Confirm the Explorer shows `README.md`, `blis`, `test`, and `requirements.txt` at the project root.
4. Install/enable the Microsoft **Python** extension (Pylance can be installed alongside it).
5. Run **Python: Create Environment** from the Command Palette. Choose **Venv**, select your Python interpreter, and use the folder name `venv` so it is ignored by this repository's `.gitignore`. If prompted to install dependencies from `requirements.txt`, accept.
6. Open a new integrated terminal. Confirm its current directory is the local project root, then run:

```powershell
& .\venv\Scripts\python.exe -m pip install -r requirements.txt
& .\venv\Scripts\python.exe -m pytest .\test\test_app_functions.py -q
```

7. If VS Code is using the wrong interpreter, run **Python: Select Interpreter** and select `venv\Scripts\python.exe` inside the local project folder.

## Common problems

- **31 collection errors or tests collected from `C:\Users\...`:** pytest was started from your home directory. Change to the project root first, or pass the project test path shown above.
- **`git` is not recognized:** install Git for Windows, reopen Command Prompt, and check `git --version`.
- **PowerShell blocks `Activate.ps1`:** activation is optional. Run the venv's `python.exe` directly as shown above.
- **Java gateway or Spark startup error:** confirm `java -version` reports JDK 17 or newer and that `JAVA_HOME` points to the JDK installation if required.
- **`No module named pyspark` or `pytest`:** install dependencies using the venv's Python and `requirements.txt` from the project root.
- **No tests found or test file not found:** verify the current directory contains `test/test_app_functions.py` and that the command uses that exact relative path.
