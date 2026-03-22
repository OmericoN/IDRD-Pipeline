# It should ONLY export the class — never instantiate it
from .db import IDRDDatabase

__all__ = ["IDRDDatabase"]