import os
import sys
import subprocess

def create_virtualenv(venv_name="venv"):
    """Create a virtual environment"""
    print("Creating virtual environment...")
    subprocess.run([sys.executable, "-m", "venv", venv_name])

def upgrade_pip(venv_name="venv"):
    """Upgrade pip in the virtual environment"""
    print("Upgrading pip...")
    if os.name == "nt":
        python_executable = os.path.join(venv_name, "Scripts", "python.exe")
    else:
        python_executable = os.path.join(venv_name, "bin", "python")
    subprocess.run([python_executable, "-m", "pip", "install", "--upgrade", "pip"])

def install_dependencies(venv_name="venv"):
    """Install dependencies from requirements.txt"""
    # Path to requirements.txt at the project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    requirements_file = os.path.join(base_dir, "requirements.txt")

    if not os.path.isfile(requirements_file):
        print(f"Warning: '{requirements_file}' not found. Skipping dependency installation.")
        return

    print("Installing dependencies...")
    if os.name == "nt":  # Windows
        pip_path = os.path.join(venv_name, "Scripts", "pip")
    else:  # Mac/Linux
        pip_path = os.path.join(venv_name, "bin", "pip")

    subprocess.run([pip_path, "install", "-r", requirements_file])

def main():
    venv_name = "venv"

    # Check for Python version compatibility
    if sys.version_info < (3, 6):
        print("Python 3.6 or higher is required. Please update Python.")
        sys.exit(1)

    # Create virtual environment
    create_virtualenv(venv_name)

    # Upgrade pip
    upgrade_pip(venv_name)

    # Install dependencies
    install_dependencies(venv_name)

    print("Virtual environment setup complete!")
    if os.name == "nt":
        print(f"To activate it, run: venv\\Scripts\\activate")
    else:
        print(f"To activate it, run: source {venv_name}/bin/activate")

if __name__ == "__main__":
    main()
