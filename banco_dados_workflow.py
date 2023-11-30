import pymysql.cursors
from datetime import datetime, timedelta


class BDWorkflow:
    def __init__(self):
        self.conn = pymysql.connect(
            host='mysql.inaflex-app.kinghost.net',
            user='inaflexapp',
            password='zt4cr3',
            db='inaflexapp',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.conn.cursor()

    def aguardando_faturamento_db(self):
        consulta = f"SELECT orcamento, pedido, cliente, cnpj, data_acabamento, acao, observacao FROM pedidos " \
                   f"WHERE faturamento = '1' AND entregas = '0' AND finalizado = '0'"
        self.cursor.execute(consulta)
        self.conn.commit()
        resultados = self.cursor.fetchall()
        return resultados

    def aguardando_faturamento_sem_informacao_db(self):
        consulta = f"SELECT orcamento, pedido, cliente, cnpj, data_acabamento, acao, observacao FROM pedidos " \
                   f"WHERE faturamento = '1' AND entregas = '0' AND finalizado = '0' AND observacao IS NULL"
        self.cursor.execute(consulta)
        self.conn.commit()
        resultados = self.cursor.fetchall()
        return resultados

    def email_pedido_para_entrega_db(self):
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        consulta = f"SELECT pedido, cliente, cnpj, data_entrega FROM pedidos " \
                   f"WHERE entregas = '1' AND finalizado = '0' AND data_entrega <= '{tomorrow}'"
        self.cursor.execute(consulta)
        self.conn.commit()
        resultados = self.cursor.fetchall()
        return resultados

    def listar_usuarios(self):
        consulta = f"SELECT nomeUsuario, email, email_responsavel FROM usuarios " \
                   f"WHERE ativo = 1 AND email <> '0'"
        self.cursor.execute(consulta)
        self.conn.commit()
        resultados = self.cursor.fetchall()
        return resultados

    def listar_eventos_pendentes(self, usuario):
        consulta = f"SELECT id, tipoEvento, numOrc, numPed, logUltimo FROM eventos " \
                   f"WHERE estagio <> 99 AND estagio <> 90 AND ativo = 1 AND usuario = '{usuario}'"
        self.cursor.execute(consulta)
        self.conn.commit()
        resultados = self.cursor.fetchall()
        return resultados

    def listar_comentarios(self, eventoID):
        consulta = f"SELECT logUsuario, comentario, logData FROM comentario " \
                   f"WHERE eventoID = '{eventoID}'"
        self.cursor.execute(consulta)
        self.conn.commit()
        resultados = self.cursor.fetchall()
        return resultados
