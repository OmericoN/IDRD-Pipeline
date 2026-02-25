# It should ONLY export the class — never instantiate it
from .db import PublicationDatabase

__all__ = ["PublicationDatabase"]