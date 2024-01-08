import sys
import pandas as pd
from pathlib import Path
from PyQt6.QtWidgets import (
    QVBoxLayout,
    QApplication,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QLineEdit,
)

GUI_DIR = Path(__file__).parent.parent
GUI_SRC = GUI_DIR / "gui_src"
PROJECT_DIR = GUI_DIR.parent
CONFIG_PATH = PROJECT_DIR / "config" / "measures_config" / "setups"

sys.path.append(str(PROJECT_DIR))

from gui_common import CommonGUI


class FuzzyValidatorWidget(CommonGUI):
    def __init__(self):
        super().__init__()

        main_layout = QVBoxLayout(self)

        self.workfile_lay = self._setup_workfile_layout(main_layout)
        self.config_lay = self._setup_config_layout(main_layout)
        self.table_lay = self._setup_table_view(main_layout)
