import sys
print(sys.path)
try:
    from src.domain.enums import department
    print(f"Successfully imported: {department}")
    from src.lib.utils import random_brand
    print(f"Successfully imported: {random_brand}")
except ImportError as e:
    print(f"Import failed: {e}")
    import traceback
    traceback.print_exc()
