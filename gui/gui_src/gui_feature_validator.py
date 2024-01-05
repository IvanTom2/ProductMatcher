from PyQt6.QtWidgets import QWidget, QVBoxLayout, QLabel


class FeatureValidatorWidget(QWidget):
    def __init__(self):
        super().__init__()

        layout = QVBoxLayout(self)
        IN_DEVELOPMENT = QLabel("<center>...In Development...</center>")
        layout.addWidget(IN_DEVELOPMENT)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AutosemWidget(AUTOSEM_CONFIG_PATH)
    window.show()

    sys.exit(app.exec())
