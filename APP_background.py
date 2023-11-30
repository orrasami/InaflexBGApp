# pyinstaller --onefile --noconsole --icon="static\favicon.ico" APP_background.py
import pymysql.cursors
import cx_Oracle
from PyQt5.QtWidgets import QMainWindow, QApplication
from janelas.janela_inicial import *
import json
import sys
from modulo_inicial import Background


class AppPrincipal(QMainWindow, Ui_Form):
    def __init__(self, parent=None):
        super().__init__(parent)
        super().setupUi(self)
        Background()


if __name__ == "__main__":
    qt = QApplication(sys.argv)

    # Testa conexão com BD Workflow
    bd_worflow_ok = ''
    bd_worflow = pymysql.connect(
            host='mysql.inaflex-app.kinghost.net',
            user='inaflexapp',
            password='zt4cr3',
            db='inaflexapp',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    cursor = bd_worflow.cursor()
    try:
        cursor.execute("SELECT VERSION()")
        results = cursor.fetchone()
        ver = results['VERSION()']
        if ver is None:
            bd_worflow_ok = 'Falha'
        else:
            bd_worflow_ok = 'OK'
    except:
        pass

    # Testa conexão com BD Oracle
    bd_oracle_ok = ''
    path_oracle_client = ''
    with open('setup.json', 'r') as file:
        d1_json = file.read()
        d1_json = json.loads(d1_json)

    for x, y in d1_json.items():
        path_oracle_client = y['oracle_client']
    try:
        cx_Oracle.init_oracle_client(lib_dir=path_oracle_client)
    except:
        bd_oracle_ok = 'Falha'
    try:
        bd_oracle = cx_Oracle.connect('CONSULTA/INAFLEX@INAFLEX')
        bd_oracle_ok = 'OK'
    except:
        bd_oracle_ok = 'Falha'
    try:
        conexao = bd_oracle.cursor()
        bd_oracle_ok = 'OK'
    except:
        bd_oracle_ok = 'Falha'

    # Janela Inicial
    widget_inicio = QtWidgets.QStackedWidget()

    janela_inicio = AppPrincipal()
    widget_inicio.addWidget(janela_inicio)
    widget_inicio.setFixedSize(400, 52)

    widget_inicio.show()

    qt.exec()
