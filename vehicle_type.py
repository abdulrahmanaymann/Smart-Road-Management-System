import warnings
import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"
warnings.filterwarnings("ignore", category=UserWarning, module="tensorflow")
warnings.filterwarnings("ignore", category=UserWarning, module="keras")

import matplotlib.pyplot as plt
import numpy as np
from tensorflow.keras.preprocessing import image  # type: ignore
from tensorflow.keras.applications.efficientnet import preprocess_input  # type: ignore
from keras.models import load_model  # type: ignore
import warnings

model = load_model("my_model.keras")


def predict_and_display(image_path, model, class_labels):

    img = image.load_img(image_path, target_size=(240, 240))
    img_array = image.img_to_array(img)
    img_array = np.expand_dims(img_array, axis=0)
    img_array = preprocess_input(img_array)

    prediction = model.predict(img_array)
    predicted_class_index = np.argmax(prediction)

    predicted_class_label = class_labels[predicted_class_index]

    return predicted_class_label


class_labels = ["Bus", "Car", "Minibus", "Motorcycle", "Truck"]


def Get_vehicle_type(image):
    predicted_class_label = predict_and_display(image, model, class_labels)
    return predicted_class_label
