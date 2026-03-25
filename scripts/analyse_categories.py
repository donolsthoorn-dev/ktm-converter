import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from collections import Counter

from modules.xml_loader import load_products


def analyse():

    print("Producten laden via xml_loader...")

    products = load_products()

    print(f"{len(products)} producten geladen")

    categories = Counter()

    for p in products:
        cat = p.get("category")

        if cat:
            categories[cat] += 1

    print("\nCategorie analyse:\n")

    for cat, count in categories.most_common():
        print(f"{cat:30} {count}")

    print("\nTotaal unieke categorieën:", len(categories))


if __name__ == "__main__":
    analyse()
