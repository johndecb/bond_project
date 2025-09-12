import os
import ast

# Change this to the root of your project
PROJECT_ROOT = "./"   # e.g. "./bond_project" or "./jcb_analytics"

output_file = "project_structure.txt"

def parse_file(path):
    """Parse .py file with ast and return classes/functions defined."""
    with open(path, "r", encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read(), filename=path)
        except SyntaxError as e:
            return {"error": f"SyntaxError: {e}"}

    funcs, classes = [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            funcs.append(node.name)
        elif isinstance(node, ast.ClassDef):
            classes.append(node.name)
    return {"functions": sorted(funcs), "classes": sorted(classes)}

summary = []

for root, dirs, files in os.walk(PROJECT_ROOT):
    for file in files:
        if file.endswith(".py"):
            path = os.path.join(root, file)
            relpath = os.path.relpath(path, PROJECT_ROOT)
            info = parse_file(path)
            summary.append((relpath, info))

# Write to text file
with open(output_file, "w", encoding="utf-8") as f:
    for relpath, info in summary:
        f.write(f"=== {relpath} ===\n")
        if "error" in info:
            f.write(f"  {info['error']}\n\n")
            continue
        if info["classes"]:
            f.write("  Classes:\n")
            for c in info["classes"]:
                f.write(f"    - {c}\n")
        if info["functions"]:
            f.write("  Functions:\n")
            for fn in info["functions"]:
                f.write(f"    - {fn}\n")
        f.write("\n")

print(f"Project structure written to {output_file}")