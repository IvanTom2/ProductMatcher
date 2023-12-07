import sys
import json
from PyQt6 import QtWidgets


class Settings:
    def __init__(self, data):
        self.data = data


class ConfigReader(QtWidgets.QWidget):
    def __init__(self, parent=None):
        QtWidgets.QWidget.__init__(self, parent)

        self.settings = None
        self._init_ui()

    def _init_ui(self):
        # Создаем интерфейс
        layout = QtWidgets.QVBoxLayout()

        label = QtWidgets.QLabel("Setting 1:")
        self.setting1_edit = QtWidgets.QLineEdit()

        label2 = QtWidgets.QLabel("Setting 2:")
        self.setting2_edit = QtWidgets.QLineEdit()

        save_button = QtWidgets.QPushButton("Save Settings")
        save_button.clicked.connect(self.save_settings)

        layout.addWidget(label)
        layout.addWidget(self.setting1_edit)
        layout.addWidget(label2)
        layout.addWidget(self.setting2_edit)
        layout.addWidget(save_button)

        self.setLayout(layout)

        # Загрузка настроек из JSON файла
        self.load_settings()

    def load_settings(self):
        try:
            with open("settings.json", "r") as file:
                data = json.load(file)
                self.settings = Settings(data)
                self.setting1_edit.setText(str(self.settings.data.get("setting1", "")))
                self.setting2_edit.setText(str(self.settings.data.get("setting2", "")))
        except FileNotFoundError:
            # Если файл не найден, создаем пустые настройки
            self.settings = Settings({})

    def save_settings(self):
        # Обновление настроек из интерфейса
        self.settings.data["setting1"] = self.setting1_edit.text()
        self.settings.data["setting2"] = self.setting2_edit.text()

        # Сохранение настроек в JSON файл
        with open("settings.json", "w") as file:
            json.dump(self.settings.data, file)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    ex = ConfigReader()
    ex.show()
    sys.exit(app.exec())
