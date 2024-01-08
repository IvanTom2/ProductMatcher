import os
import sys
import json
from pathlib import Path
from PyQt6.QtWidgets import (
    QTreeView,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QComboBox,
    QMenuBar,
    QFileDialog,
    QMainWindow,
    QTabWidget,
    QApplication,
)

GUI_DIR = Path(__file__).parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
sys.path.append(str(PROJECT_DIR))
sys.path.append(str(GUI_SRC))

from gui_src.gui_autosem import AutosemWidget, AUTOSEM_CONFIG_PATH
from gui.gui_src.gui_feature_v import FeatureValidatorWidget
from gui.gui_src.gui_fuzzy_v import FuzzyValidatorWidget


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()

        main_window = QWidget(self)
        self.setCentralWidget(main_window)

        tab_widget = self._set_tables(main_window)

        main_layout = QVBoxLayout(main_window)
        main_layout.addWidget(tab_widget)

        self._set_size_position()

    def _set_tables(self, main_window: QWidget):
        tab_widget = QTabWidget(main_window)

        autosem_tab = AutosemWidget(AUTOSEM_CONFIG_PATH)
        feature_validator_tab = FeatureValidatorWidget()
        jakkar_validator_tab = FuzzyValidatorWidget()

        tab_widget.addTab(autosem_tab, "Auto-Semantic")
        tab_widget.addTab(feature_validator_tab, "Feature-Validator")
        tab_widget.addTab(jakkar_validator_tab, "Fuzzy-Validator")

        return tab_widget

    def _set_size_position(self):
        screen_size = self.screen().availableSize()

        width = int(screen_size.width() * 0.5)
        height = int(screen_size.height() * 0.5)
        self.resize(width, height)

        center_x = (screen_size.width() - self.width()) // 2
        center_y = (screen_size.height() - self.height()) // 2

        self.move(center_x, center_y)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
