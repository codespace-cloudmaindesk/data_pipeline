import random
from src.domain.enums import department, branch_code, DEPARTMENT_MAPPING

def random_branch():
    return random.choice(list(branch_code))

def random_department_category():
    dept = random.choice(list(department))
    cat = random.choice(DEPARTMENT_MAPPING[dept])
    return dept, cat

def random_brand(category):
    # BRAND_MAPPING was commented out in enums.py, returning generic for now or uncomment in enums.py
    return "Generic Brand"
