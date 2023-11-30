import cx_Oracle
import json
from PyQt5.QtWidgets import QMessageBox
import ast

path_oracle_client = ''
with open('setup.json', 'r') as file:
    d1_json = file.read()
    d1_json = json.loads(d1_json)

for x, y in d1_json.items():
    path_oracle_client = y['oracle_client']


class BDBohm:
    def __init__(self):
        self.conn = cx_Oracle.connect('CONSULTA/INAFLEX@INAFLEX')
        self.conexao = self.conn.cursor()

    def fechar(self):
        self.conn.close()

    def select(self, consulta):
            respostas = self.conexao.execute(consulta)
            return respostas

    def powerbi_orcamentos(self):
        consulta = f"SELECT " \
                   f"O.DATA_PEDIDO, " \
                   f"O.CHAVE_CLIENTE, " \
                   f"V.NOMERED, " \
                   f"I.CHAVE_PEDIDO, " \
                   f"I.CHAVE_PRODUTO, " \
                   f"I.QUANTIDADE, " \
                   f"I.PRECO_TABELA, " \
                   f"I.PRECO_VENDA, " \
                   f"I.VALOR_TOTAL, " \
                   f"I.ANALISE_CUSTO_MEDIO, " \
                   f"I.STATUS " \
                   f"FROM INAFLEX.ORCAMENTOS_ITENS I, INAFLEX.ORCAMENTOS O, INAFLEX.VENDEDORES V " \
                   f"WHERE (I.CHAVE_PEDIDO = O.NUMPED) " \
                   f"AND (O.CHAVE_VENDEDOR = V.CODVENDEDOR) " \
                   f"AND (O.DATA_PEDIDO>=TO_DATE('01-01-2020','DD-MM-YYYY')) " \
                   f"ORDER BY O.DATA_PEDIDO"
        try:
            respostas = self.conexao.execute(consulta)
            return respostas
        except cx_Oracle.Error as e:
            print(f"Error executing query: {e}")
            return None

    def powerbi_orcamentos_c(self):
        consulta = f"SELECT " \
                   f"O.DATA_PEDIDO, " \
                   f"O.CHAVE_CLIENTE, " \
                   f"V.NOMERED, " \
                   f"I.CHAVE_PEDIDO, " \
                   f"C.CHAVE_PRODUTO, " \
                   f"C.QUANTIDADE AS QUANTIDADE, " \
                   f"C.PRECO_TABELA, " \
                   f"C.PRECO_VENDA, " \
                   f"C.VALOR_TOTAL, " \
                   f"C.ANALISE_CUSTO_MEDIO, " \
                   f"I.STATUS " \
                   f"FROM INAFLEX.ORCAMENTOS_ITENS_C C, INAFLEX.ORCAMENTOS_ITENS I, INAFLEX.ORCAMENTOS O, INAFLEX.VENDEDORES V " \
                   f"WHERE (C.CHAVE_ORCAMENTO_ITEM = I.CHAVE) " \
                   f"AND (I.CHAVE_PEDIDO = O.NUMPED) " \
                   f"AND (O.CHAVE_VENDEDOR = V.CODVENDEDOR) " \
                   f"AND (O.DATA_PEDIDO>=TO_DATE('01-01-2020','DD-MM-YYYY')) " \
                   f"ORDER BY O.DATA_PEDIDO"
        try:
            respostas = self.conexao.execute(consulta)
            return respostas
        except cx_Oracle.Error as e:
            print(f"Error executing query: {e}")
            return None

    def powerbi_pedidos(self):
        consulta = f"SELECT O.DATA_PEDIDO, O.CHAVE_CLIENTE, V.NOMERED, I.CHAVE_PEDIDO, I.CHAVE_PRODUTO, " \
                   f"I.QUANTIDADE, I.PRECO_TABELA, I.PRECO_VENDA, I.VALOR_TOTAL, I.ANALISE_CUSTO_MEDIO, I.STATUS " \
                   f"FROM INAFLEX.PEDIDOS_ITENS I, INAFLEX.PEDIDOS O, INAFLEX.VENDEDORES V " \
                   f"WHERE (I.CHAVE_PEDIDO = O.NUMPED) AND (O.CHAVE_VENDEDOR = V.CODVENDEDOR) " \
                   f"AND (O.DATA_PEDIDO>=TO_DATE('01-01-2020','DD-MM-YYYY'))"
        try:
            respostas = self.conexao.execute(consulta)
            return respostas
        except cx_Oracle.Error as e:
            print(f"Error executing query: {e}")
            return None

    def powerbi_clientes(self):
        consulta = f"SELECT C.CODCLI, C.NOME, C.NOMERED, C.CGC, T.DESCRICAO AS SEGMENTO, C.CIDADE, E.SIGLA, " \
                   f"C.PRIMEIRA_VENDA_DATA, C.ULTIMACOMPRA, C.ULTIMACONSULTA, C.STATUS " \
                   f"FROM INAFLEX.CLIENTES C, INAFLEX.CLIENTES_TIPOS T, INAFLEX.ESTADOS E " \
                   f"WHERE (C.UF = E.CHAVE) AND (T.CHAVE = C.CHAVE_TIPO)"
        try:
            respostas = self.conexao.execute(consulta)
            return respostas
        except cx_Oracle.Error as e:
            print(f"Error executing query: {e}")
            return None

    def powerbi_itens(self):
        consulta = f"SELECT P.CPROD, P.CODIGO, P.CODIGO_INTERNO, P.DESCRICAO, P.CUSTO_MATERIAIS, P.CUSTO_OPERACOES, " \
                   f"P.CUSTO_TOTAL, P.PRECO_OBJETIVO_VISTA, P.ORIGEM, P.QTDE_COMPONENTES, P.CHAVE_MARKUP, " \
                   f"L.LINHA, G.GRUPO, F.FAMILIA " \
                   f"FROM INAFLEX.PRODUTOS P, INAFLEX.LINHA_PRODUTOS L, INAFLEX.GRUPO_PRODUTOS G, " \
                   f"INAFLEX.FAMILIA_PRODUTOS F " \
                   f"WHERE (P.CHAVE_LINHA = L.CHAVE) AND (P.CHAVE_GRUPO = G.CHAVE) AND (P.CHAVE_FAMILIA= F.CHAVE)"
        try:
            respostas = self.conexao.execute(consulta)
            return respostas
        except cx_Oracle.Error as e:
            print(f"Error executing query: {e}")
            return None

    def powerbi_formulas(self):
        consulta = f"SELECT P.CHAVE, P.CODIGO, P.CHAVE_PRODUTO, M.CHAVE_MATERIAL, M.QUANTIDADE " \
                   f"FROM INAFLEX.PROCESSOS_MATERIAIS M, INAFLEX.PROCESSOS P " \
                   f"WHERE (P.CHAVE = M.CHAVE_PROCESSO) AND (P.PADRAO = 'SIM')"
        try:
            respostas = self.conexao.execute(consulta)
            return respostas
        except cx_Oracle.Error as e:
            print(f"Error executing query: {e}")
            return None
