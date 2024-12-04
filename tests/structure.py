import os
from pathlib import Path

def print_directory_structure(startpath):
    """Print the directory structure starting from startpath."""
    output = []
    excluded_dirs = {'__pycache__', 'responses'}
    
    for root, dirs, files in os.walk(startpath):
        # Skip excluded directories and hidden files
        dirs[:] = [d for d in dirs if d not in excluded_dirs and not d.startswith('.')]
        files = [f for f in files if not f.endswith('.pyc') and not f.startswith('.')]
        
        level = root.replace(startpath, '').count(os.sep)
        indent = '│   ' * (level - 1) + '├── ' if level > 0 else ''
        if level > 0:
            output.append(f"{indent}{os.path.basename(root)}/")
        
        subindent = '│   ' * level + '├── '
        for f in files:
            output.append(f"{subindent}{f}")
    
    return '\n'.join(output)

if __name__ == "__main__":
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Print structure
    structure = print_directory_structure(current_dir)
    print("\nProject Structure:")
    print("================")
    print(structure)