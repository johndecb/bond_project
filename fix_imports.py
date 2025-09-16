import os
import re

BASE_DIR = "jcb_bond_project"

# The top-level modules inside your package
MODULES = [
    "jcb_bond_project",
    "jcb_loaders",
    "jcb_api",
    "jcb_core",
]


def update_imports_in_file(filepath):
    with open(filepath, "r") as f:
        content = f.read()

    updated = content
    for module in MODULES:
        # Replace "from X" with "from jcb_bond_project.X"
        updated = re.sub(rf"\bfrom {module}\b", f"from jcb_bond_project.{module}", updated)
        # Replace "import X" with "import jcb_bond_project.X"
        updated = re.sub(rf"\bimport {module}\b", f"import jcb_bond_project.{module}", updated)

    if updated != content:
        print(f"Updated imports in {filepath}")
        with open(filepath, "w") as f:
            f.write(updated)

def walk_and_update(base_dir):
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".py"):
                update_imports_in_file(os.path.join(root, file))

if __name__ == "__main__":
    walk_and_update(BASE_DIR)
