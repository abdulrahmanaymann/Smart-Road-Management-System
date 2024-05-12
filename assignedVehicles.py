import warnings
import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"
warnings.filterwarnings("ignore", category=UserWarning, module="tensorflow")
warnings.filterwarnings("ignore", category=UserWarning, module="keras")

import cv2
import matplotlib as plt
import pytesseract
from PIL import Image
import sys
from vehicle_type import Get_vehicle_type


# Vehicels before Cropping
# -----------------------------
BEFORE_1 = "Datasets\\Egyptain Cars\\Before Cropping\\0022.jpg"
BEFORE_2 = "Datasets\\Egyptain Cars\\Before Cropping\\0026.jpg"
BEFORE_3 = "Datasets\\Egyptain Cars\\Before Cropping\\0033.jpg"
BEFORE_4 = "Datasets\\Egyptain Cars\\Before Cropping\\0035.jpg"
BEFORE_5 = "Datasets\\Egyptain Cars\\Before Cropping\\0037.jpg"
BEFORE_6 = "Datasets\\Egyptain Cars\\Before Cropping\\0055.jpg"
BEFORE_7 = "Datasets\\Egyptain Cars\\Before Cropping\\0058.jpg"
BEFORE_8 = "Datasets\\Egyptain Cars\\Before Cropping\\0067.jpg"
BEFORE_9 = "Datasets\\Egyptain Cars\\Before Cropping\\0082.jpg"
BEFORE_10 = "Datasets\\Egyptain Cars\\Before Cropping\\0095.jpg"
BEFORE_11 = "Datasets\\Egyptain Cars\\Before Cropping\\0608.jpg"
BEFORE_12 = "Datasets\\Egyptain Cars\\Before Cropping\\0609.jpg"
BEFORE_13 = "Datasets\\Egyptain Cars\\Before Cropping\\0718.jpg"

# Vehicels after Cropping
# -----------------------------
AFTER_1 = "Datasets\\Egyptain Cars\\After Cropping\\11.png"
AFTER_2 = "Datasets\\Egyptain Cars\\After Cropping\\12.png"
AFTER_3 = "Datasets\\Egyptain Cars\\After Cropping\\13.png"
AFTER_4 = "Datasets\\Egyptain Cars\\After Cropping\\9.png"
AFTER_5 = "Datasets\\Egyptain Cars\\After Cropping\\10.png"
AFTER_6 = "Datasets\\Egyptain Cars\\After Cropping\\3.png"
AFTER_7 = "Datasets\\Egyptain Cars\\After Cropping\\2.png"
AFTER_8 = "Datasets\\Egyptain Cars\\After Cropping\\4.png"
AFTER_9 = "Datasets\\Egyptain Cars\\After Cropping\\5.png"
AFTER_10 = "Datasets\\Egyptain Cars\\After Cropping\\6.png"
AFTER_11 = "Datasets\\Egyptain Cars\\After Cropping\\1.png"
AFTER_12 = "Datasets\\Egyptain Cars\\After Cropping\\7.png"
AFTER_13 = "Datasets\\Egyptain Cars\\After Cropping\\8.png"

vehicles = {
    BEFORE_1: AFTER_1,
    BEFORE_2: AFTER_2,
    BEFORE_3: AFTER_3,
    BEFORE_4: AFTER_4,
    BEFORE_5: AFTER_5,
    BEFORE_6: AFTER_6,
    BEFORE_7: AFTER_7,
    BEFORE_8: AFTER_8,
    BEFORE_9: AFTER_9,
    BEFORE_10: AFTER_10,
    BEFORE_11: AFTER_11,
    BEFORE_12: AFTER_12,
    BEFORE_13: AFTER_13,
}


pytesseract.pytesseract.tesseract_cmd = (
    "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
)


def Get_Vehicle_ID(vehicle):
    if vehicle not in vehicles:
        raise ValueError(
            f"Vehicle image '{vehicle}' not found in the dictionary 'vehicles'."
        )

    image = cv2.imread(vehicles[vehicle])

    text = pytesseract.image_to_string(image, lang="ara_number")

    vehicle_type = Get_vehicle_type(vehicle)

    vehicle_ID = f"{text.strip()}_{vehicle_type}"

    return vehicle_ID
