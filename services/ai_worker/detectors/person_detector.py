from .base_detector import BaseDetector
import cv2
import numpy as np
import tflite_runtime.interpreter as tflite

class PersonDetector(BaseDetector):
    def load_model(self, model_name: str):
        self.interpreter = tflite.Interpreter(model_path=model_name)
        self.interpreter.allocate_tensors()
        self.input_details = self.interpreter.get_input_details()
        self.output_details = self.interpreter.get_output_details()

    def detect(self, frame):
        h, w = self.input_details[0]['shape'][1:3]
        img = cv2.resize(frame, (w, h))
        img = np.expand_dims(img, axis=0).astype(np.float32)
        self.interpreter.set_tensor(self.input_details[0]['index'], img)
        self.interpreter.invoke()
        output_data = self.interpreter.get_tensor(self.output_details[0]['index'])
        results = []
        for det in output_data:
            results.append({
                "xmin": det[0], "ymin": det[1],
                "xmax": det[2], "ymax": det[3],
                "confidence": det[4]
            })
        return results
