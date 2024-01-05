from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel


class FuzzyValidatorWidget(QWidget):
    def __init__(self):
        super().__init__()

        layout = QVBoxLayout(self)
        IN_DEVELOPMENT = QLabel("<center>...In Development...</center>")
        layout.addWidget(IN_DEVELOPMENT)
