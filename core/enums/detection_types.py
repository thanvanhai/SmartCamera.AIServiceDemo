"""
Detection types enumeration for the Smart Camera AI Service.
"""

from enum import Enum


class DetectionType(str, Enum):
    """Types of objects that can be detected"""
    # General objects
    PERSON = "person"
    CAR = "car"
    TRUCK = "truck"
    BUS = "bus"
    MOTORCYCLE = "motorcycle"
    BICYCLE = "bicycle"
    
    # Animals
    CAT = "cat"
    DOG = "dog"
    BIRD = "bird"
    
    # Common objects
    BACKPACK = "backpack"
    UMBRELLA = "umbrella"
    HANDBAG = "handbag"
    SUITCASE = "suitcase"
    BOTTLE = "bottle"
    
    # Security related
    WEAPON = "weapon"
    SUSPICIOUS_OBJECT = "suspicious_object"
    
    # Custom categories
    UNKNOWN = "unknown"
    OTHER = "other"


class AlertLevel(str, Enum):
    """Alert levels for detections"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DetectionRegion(str, Enum):
    """Regions where detection can occur"""
    ENTRANCE = "entrance"
    EXIT = "exit"
    RESTRICTED_AREA = "restricted_area"
    PARKING_LOT = "parking_lot"
    HALLWAY = "hallway"
    ROOM = "room"
    OUTDOOR = "outdoor"
    UNKNOWN = "unknown"