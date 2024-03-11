from datetime import datetime
import json
import schedule
import time
from threading import Thread
import xlsxwriter
from banco_dados_workflow import BDWorkflow
from banco_dados_oracle import BDBohm
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Background:
    def __init__(self):
        self.relatorio = []
        daemon = Thread(target=self.background_task, args=())
        daemon.daemon = True
        daemon.start()

    def background_task(self):
        # schedule.every().day.at("18:00").do(self.tarefas)
        schedule.every(1).seconds.do(self.tarefas)
        while True:
            schedule.run_pending()
            time.sleep(1)

    def tarefas(self):
        self.relatorio.clear()
        # RELATORIOS

        self.email_pedido_no_faturamento_sem_informacao()
        self.email_pedido_no_faturamento()
        self.email_pedido_para_entrega()
        self.email_eventos_parado_vendas()

        respostas = BDBohm().powerbi_orcamentos()
        self.cria_relatorio_orcamentos(respostas, 0)
        self.insere_relatorio("select_orcamentos", datetime.today().strftime('%H:%M:%S %Y/%m/%d'))

        respostas = BDBohm().powerbi_pedidos()
        self.cria_relatorio_orcamentos(respostas, 1)
        self.insere_relatorio("select_pedidos", datetime.today().strftime('%H:%M:%S %Y/%m/%d'))

        respostas = BDBohm().powerbi_orcamentos_c()
        self.cria_relatorio_orcamentos(respostas, 2)
        self.insere_relatorio("select_orcamentos_c", datetime.today().strftime('%H:%M:%S %Y/%m/%d'))

        respostas = BDBohm().powerbi_clientes()
        self.cria_relatorio_clientes(respostas, 0)
        self.insere_relatorio("select_clientes", datetime.today().strftime('%H:%M:%S %Y/%m/%d'))

        respostas = BDBohm().powerbi_itens()
        self.cria_relatorio_itens(respostas, 0)
        self.insere_relatorio("select_itens", datetime.today().strftime('%H:%M:%S %Y/%m/%d'))

        respostas = BDBohm().powerbi_formulas()
        self.cria_relatorio_formulas(respostas, 0)
        self.insere_relatorio("select_itens", datetime.today().strftime('%H:%M:%S %Y/%m/%d'))

        self.log_demanda(self.relatorio)

    def insere_relatorio(self, relatorio, resultado):
        dic = {"relatorio": relatorio, "resultado": resultado}
        self.relatorio.append(dic)

    @staticmethod
    def log_demanda(relatorio):
        nome = datetime.today().strftime('%Y%m%d%H%M%S')
        path_download = r'C:\\Download\\'
        workbook = xlsxwriter.Workbook(f'{path_download}log_{nome}.xlsx')
        worksheet = workbook.add_worksheet('Relatorio')
        worksheet.write(0, 0, "Relatorio")
        worksheet.write(0, 1, "Resultado")
        row = 1
        i = 0
        while i < len(relatorio):
            worksheet.write(row, 0, relatorio[i]["relatorio"])
            worksheet.write(row, 1, relatorio[i]["resultado"])
            row += 1
            i += 1
        workbook.close()

    @staticmethod
    def cria_relatorio_orcamentos(select, tipo):
        if tipo == 0:
            nome = "ORCAMENTOS"
        elif tipo == 1:
            nome = "PEDIDOS"
        else:
            nome = "ORCAMENTOS_C"
        with open('setup.json', 'r') as file:
            d1_json = file.read()
            d1_json = json.loads(d1_json)

        for x, y in d1_json.items():
            path_download = y['vendas']
        workbook = xlsxwriter.Workbook(f'{path_download}{nome}.xlsx')
        worksheet = workbook.add_worksheet('Relatorio')
        worksheet.write(0, 0, "DATA_PEDIDO")
        worksheet.write(0, 1, "CHAVE_CLIENTE")
        worksheet.write(0, 2, "NOMERED")
        worksheet.write(0, 3, "CHAVE_PEDIDO")
        worksheet.write(0, 4, "CHAVE_PRODUTO")
        worksheet.write(0, 5, "QUANTIDADE")
        worksheet.write(0, 6, "PRECO_TABELA")
        worksheet.write(0, 7, "PRECO_VENDA")
        worksheet.write(0, 8, "VALOR_TOTAL")
        worksheet.write(0, 9, "ANALISE_CUSTO_MEDIO")
        worksheet.write(0, 10, "STATUS")
        row = 1
        for item in select:
            data = str(item[0])
            if data != "None":
                data_final = f'{data[8:10]}/{data[5:7]}/{data[0:4]}'
            else:
                data_final = ""
            worksheet.write(row, 0, data_final)
            worksheet.write(row, 1, item[1])
            worksheet.write(row, 2, item[2])
            worksheet.write(row, 3, item[3])
            worksheet.write(row, 4, item[4])
            worksheet.write(row, 5, item[5])
            worksheet.write(row, 6, item[6])
            worksheet.write(row, 7, item[7])
            worksheet.write(row, 8, item[8])
            worksheet.write(row, 9, item[9])
            worksheet.write(row, 10, item[10])
            row += 1
        workbook.close()

    @staticmethod
    def cria_relatorio_clientes(select, tipo):
        nome = "CLIENTES"
        with open('setup.json', 'r') as file:
            d1_json = file.read()
            d1_json = json.loads(d1_json)

        for x, y in d1_json.items():
            path_download = y['vendas']
        workbook = xlsxwriter.Workbook(f'{path_download}{nome}.xlsx')
        worksheet = workbook.add_worksheet('Relatorio')
        worksheet.write(0, 0, "CODCLI")
        worksheet.write(0, 1, "NOME")
        worksheet.write(0, 2, "NOMERED")
        worksheet.write(0, 3, "CGC")
        worksheet.write(0, 4, "SEGMENTO")
        worksheet.write(0, 5, "CIDADE")
        worksheet.write(0, 6, "SIGLA")
        worksheet.write(0, 7, "PRIMEIRA_VENDA_DATA")
        worksheet.write(0, 8, "ULTIMACOMPRA")
        worksheet.write(0, 9, "ULTIMACONSULTA")
        worksheet.write(0, 10, "STATUS")
        row = 1
        for item in select:
            worksheet.write(row, 0, item[0])
            worksheet.write(row, 1, item[1])
            worksheet.write(row, 2, item[2])
            worksheet.write(row, 3, item[3])
            worksheet.write(row, 4, item[4])
            worksheet.write(row, 5, item[5])
            worksheet.write(row, 6, item[6])
            worksheet.write(row, 7, item[7])
            worksheet.write(row, 8, item[8])
            worksheet.write(row, 9, item[9])
            worksheet.write(row, 10, item[10])
            row += 1
        workbook.close()

    @staticmethod
    def cria_relatorio_itens(select, tipo):
        nome = "ITENS"
        with open('setup.json', 'r') as file:
            d1_json = file.read()
            d1_json = json.loads(d1_json)

        for x, y in d1_json.items():
            path_download = y['vendas']
        workbook = xlsxwriter.Workbook(f'{path_download}{nome}.xlsx')
        worksheet = workbook.add_worksheet('Relatorio')
        worksheet.write(0, 0, "CPROD")
        worksheet.write(0, 1, "CODIGO")
        worksheet.write(0, 2, "CODIGO_INTERNO")
        worksheet.write(0, 3, "DESCRICAO")
        worksheet.write(0, 4, "CUSTO_MATERIAIS")
        worksheet.write(0, 5, "CUSTO_OPERACOES")
        worksheet.write(0, 6, "CUSTO_TOTAL")
        worksheet.write(0, 7, "PRECO_OBJETIVO_VISTA")
        worksheet.write(0, 8, "ORIGEM")
        worksheet.write(0, 9, "QTDE_COMPONENTES")
        worksheet.write(0, 10, "CHAVE_MARKUP")
        worksheet.write(0, 11, "LINHA")
        worksheet.write(0, 12, "GRUPO")
        worksheet.write(0, 13, "FAMILIA")
        row = 1
        for item in select:
            worksheet.write(row, 0, item[0])
            worksheet.write(row, 1, item[1])
            worksheet.write(row, 2, item[2])
            worksheet.write(row, 3, item[3])
            worksheet.write(row, 4, item[4])
            worksheet.write(row, 5, item[5])
            worksheet.write(row, 6, item[6])
            worksheet.write(row, 7, item[7])
            worksheet.write(row, 8, item[8])
            worksheet.write(row, 9, item[9])
            worksheet.write(row, 10, item[10])
            worksheet.write(row, 11, item[11])
            worksheet.write(row, 12, item[12])
            worksheet.write(row, 13, item[13])
            row += 1
        workbook.close()

    @staticmethod
    def cria_relatorio_formulas(select, tipo):
        nome = "FORMULAS"
        with open('setup.json', 'r') as file:
            d1_json = file.read()
            d1_json = json.loads(d1_json)

        for x, y in d1_json.items():
            path_download = y['vendas']
        workbook = xlsxwriter.Workbook(f'{path_download}{nome}.xlsx')
        worksheet = workbook.add_worksheet('Relatorio')
        worksheet.write(0, 0, "CHAVE")
        worksheet.write(0, 1, "CODIGO")
        worksheet.write(0, 2, "CHAVE_PRODUTO")
        worksheet.write(0, 3, "CHAVE_MATERIAL")
        worksheet.write(0, 4, "QUANTIDADE")
        row = 1
        for item in select:
            worksheet.write(row, 0, item[0])
            worksheet.write(row, 1, item[1])
            worksheet.write(row, 2, item[2])
            worksheet.write(row, 3, item[3])
            worksheet.write(row, 4, item[4])
            row += 1
        workbook.close()

    @staticmethod
    def email_pedido_no_faturamento():
        resultados = BDWorkflow().aguardando_faturamento_db()

        mail_body = ""

        for resultado in resultados:
            cliente = resultado['cliente']
            cliente = cliente[:6]
            if cliente != "RANDON":
                data = resultado['data_acabamento']
                data = data.strftime("%d/%m/%Y")
                mail_body += f"PEDIDO: {resultado['pedido']} |"
                mail_body += f" CLIENTE: {resultado['cliente']} |"
                mail_body += f" DESDE: {data} |"
                mail_body += f" AGUARDANDO: {resultado['acao']} |"
                mail_body += f" OBSERVAÇÃO: {resultado['observacao']}"
                mail_body += f"\n"
                mail_body += f"\n"

        username = "sami@inaflex.com.br"
        password = "Inf_Rav#M365_2023"
        mail_from = "sami@inaflex.com.br"
        mail_to = "felipe.orra@inaflex.com.br, producao@inaflex.com.br, sami@inaflex.com.br"
        # mail_to = "orrasami@yahoo.com.br, sami@inaflex.com.br"
        mail_subject = "Pedidos Aguardando Faturamento"

        mimemsg = MIMEMultipart()
        mimemsg['From'] = mail_from
        mimemsg['To'] = mail_to
        mimemsg['Subject'] = mail_subject
        mimemsg.attach(MIMEText(mail_body, 'plain'))
        connection = smtplib.SMTP(host='smtp.office365.com', port=587)
        connection.starttls()
        connection.login(username, password)
        connection.send_message(mimemsg)
        connection.quit()

    @staticmethod
    def email_pedido_no_faturamento_sem_informacao():
        resultados = BDWorkflow().aguardando_faturamento_sem_informacao_db()

        mail_body = ""

        for resultado in resultados:
            data = resultado['data_acabamento']
            data = data.strftime("%d/%m/%Y")
            mail_body += f"PEDIDO: {resultado['pedido']} |"
            mail_body += f" CLIENTE: {resultado['cliente']} |"
            mail_body += f" DESDE: {data} |"
            mail_body += f"\n"
            mail_body += f"\n"

        username = "sami@inaflex.com.br"
        password = "Inf_Rav#M365_2023"
        mail_from = "sami@inaflex.com.br"
        mail_to = "faturamento@inaflex.com.br, sami@inaflex.com.br"
        # mail_to = "orrasami@yahoo.com.br"
        mail_subject = "Pedidos Sem Informacao de Faturamento"

        mimemsg = MIMEMultipart()
        mimemsg['From'] = mail_from
        mimemsg['To'] = mail_to
        mimemsg['Subject'] = mail_subject
        mimemsg.attach(MIMEText(mail_body, 'plain'))
        connection = smtplib.SMTP(host='smtp.office365.com', port=587)
        connection.starttls()
        connection.login(username, password)
        connection.send_message(mimemsg)
        connection.quit()

    @staticmethod
    def email_pedido_para_entrega():
        resultados = BDWorkflow().email_pedido_para_entrega_db()

        mail_body = ""

        for resultado in resultados:
            data = resultado['data_entrega']
            data = data.strftime("%d/%m/%Y")
            mail_body += f"PEDIDO: {resultado['pedido']} |"
            mail_body += f" CLIENTE: {resultado['cliente']} |"
            mail_body += f" CNPJ: {resultado['cnpj']} |"
            mail_body += f" DATA DE ENTREGA: {data} |"
            mail_body += f"\n"
            mail_body += f"\n"

        username = "sami@inaflex.com.br"
        password = "Inf_Rav#M365_2023"
        mail_from = "sami@inaflex.com.br"
        mail_to = "faturamento@inaflex.com.br, sami@inaflex.com.br"
        # mail_to = "orrasami@yahoo.com.br"
        mail_subject = "Pedidos Pendente de Entrega"

        mimemsg = MIMEMultipart()
        mimemsg['From'] = mail_from
        mimemsg['To'] = mail_to
        mimemsg['Subject'] = mail_subject
        mimemsg.attach(MIMEText(mail_body, 'plain'))
        connection = smtplib.SMTP(host='smtp.office365.com', port=587)
        connection.starttls()
        connection.login(username, password)
        connection.send_message(mimemsg)
        connection.quit()

    @staticmethod
    def email_eventos_parado_vendas():
        resultados = BDWorkflow().email_eventos_parado_vendas_db()

        count = 0
        while count < 2:
            conteudo = False

            mail_body = "<h3>Eventos atrasados</h3><br>"
            mail_body += ('<table style="border:1px solid black; border-collapse: collapse;">'
                         ' <tr>'
                         '  <td style="border:1px solid black; padding: 10px;"># EVENTO</td>'
                         '  <td style="border:1px solid black; padding: 10px;">USUÁRIO</td>'
                         '  <td style="border:1px solid black; padding: 10px;">TIPO DE EVENTO</td>'
                         '  <td style="border:1px solid black; padding: 10px;">ULTIMA ATUALIZAÇÃO</td>'
                         ' </tr>')

            for resultado in resultados:
                if count == 0 or (resultado["tipoEvento"] == "GERAR PEDIDO" or resultado["tipoEvento"] == "FAZER ORCAMENTO"):
                    conteudo = True
                    data = resultado['logUltimo']
                    data = data.strftime("%d/%m/%Y")
                    mail_body += f'<tr>'
                    mail_body += f'<td style="border:1px solid black; padding: 10px;">{resultado["id"]}</td>'
                    mail_body += f'<td style="border:1px solid black; padding: 10px;">{resultado["usuario"]}</td>'
                    mail_body += f'<td style="border:1px solid black; padding: 10px;">{resultado["tipoEvento"]}</td>'
                    mail_body += f'<td style="border:1px solid black; padding: 10px;">{data}</td>'
                    mail_body += f'</tr>'

            mail_body += f'</table>'

            username = "sami@inaflex.com.br"
            password = "Inf_Rav#M365_2023"
            mail_from = "sami@inaflex.com.br"
            if count == 0:
                mail_to = "sami@inaflex.com.br"
            else:
                mail_to = "felipe.orra@inaflex.com.br"
            mail_subject = "Eventos parados a mais de dois dias"

            if conteudo:
                mimemsg = MIMEMultipart()
                mimemsg['From'] = mail_from
                mimemsg['To'] = mail_to
                mimemsg['Subject'] = mail_subject
                mimemsg.attach(MIMEText(mail_body, 'html'))
                connection = smtplib.SMTP(host='smtp.office365.com', port=587)
                connection.starttls()
                connection.login(username, password)
                connection.send_message(mimemsg)
                connection.quit()
                count += 1

    @staticmethod
    def email_eventos_pendentes():
        usuarios = BDWorkflow().listar_usuarios()
        lista_diretoria = ""
        lista_producao = ""
        lista_vendas = ""
        for info in usuarios:
            usuario = info['nomeUsuario']
            email = info['email']
            responsavel = info['email_responsavel']

            resultados = BDWorkflow().listar_eventos_pendentes(usuario)

            if resultados != ():
                if responsavel == '1':
                    lista_diretoria += f"--------------------------------------------------------------------\n"
                    lista_diretoria += f"--------------------------------------------------------------------\n"
                    lista_diretoria += "@ " + usuario + ":\n"
                    lista_diretoria += f"--------------------------------------------------------------------\n"
                    lista_diretoria += f"--------------------------------------------------------------------\n"
                    lista_diretoria += f"\n"
                if responsavel == '2':
                    lista_producao += f"--------------------------------------------------------------------\n"
                    lista_producao += f"--------------------------------------------------------------------\n"
                    lista_producao += "@ " + usuario + ":\n"
                    lista_producao += f"--------------------------------------------------------------------\n"
                    lista_producao += f"--------------------------------------------------------------------\n"
                    lista_producao += f"\n"
                if responsavel == '3':
                    lista_vendas += f"--------------------------------------------------------------------\n"
                    lista_vendas += f"--------------------------------------------------------------------\n"
                    lista_vendas += "@ " + usuario + ":\n"
                    lista_vendas += f"--------------------------------------------------------------------\n"
                    lista_vendas += f"--------------------------------------------------------------------\n"
                    lista_vendas += f"\n"

                mail_body = ""
                for resultado in resultados:
                    id = resultado['id']
                    mail_body += f"ID: {id} |"
                    mail_body += f" TIPO DE EVENTO: {resultado['tipoEvento']} |"
                    mail_body += f" PEDIDO: {resultado['numPed']} |"
                    mail_body += f" ORÇAMENTO: {resultado['numOrc']} |"
                    data = resultado['logUltimo']
                    data = data.strftime("%d/%m/%Y")
                    mail_body += f" ULTIMA ALTERAÇÃO: {data} |"
                    mail_body += f"\n"
                    mail_body += f"--------------------------------------------------------------------"
                    mail_body += f"\n"

                    comentarios = BDWorkflow().listar_comentarios(id)
                    for comentario in comentarios:
                        mail_body += f"#### USUARIO: {comentario['logUsuario']} |"
                        data = comentario['logData']
                        data = data.strftime("%d/%m/%Y")
                        mail_body += f" DATA: {data} |"
                        mail_body += f" COMENTÁRIO: {comentario['comentario']} |"
                        mail_body += f"\n"

                    mail_body += f"\n"
                    mail_body += f"\n"
                    if responsavel == '1':
                        lista_diretoria += mail_body
                    if responsavel == '2':
                        lista_producao += mail_body
                    if responsavel == '3':
                        lista_vendas += mail_body

                username = "sami@inaflex.com.br"
                password = "Inf_Rav#M365_2023"
                mail_from = "sami@inaflex.com.br"
                mail_to = email
                # mail_to = "sami@inaflex.com.br"
                mail_subject = usuario + " - Eventos pendentes"

                mimemsg = MIMEMultipart()
                mimemsg['From'] = mail_from
                mimemsg['To'] = mail_to
                mimemsg['Subject'] = mail_subject
                mimemsg.attach(MIMEText(mail_body, 'plain'))
                connection = smtplib.SMTP(host='smtp.office365.com', port=587)
                connection.starttls()
                connection.login(username, password)
                connection.send_message(mimemsg)
                connection.quit()

        if lista_diretoria != "":
            username = "sami@inaflex.com.br"
            password = "Inf_Rav#M365_2023"
            mail_from = "sami@inaflex.com.br"
            mail_to = "sami@inaflex.com.br"
            mail_subject = "Diretoria - Eventos pendentes"

            mimemsg = MIMEMultipart()
            mimemsg['From'] = mail_from
            mimemsg['To'] = mail_to
            mimemsg['Subject'] = mail_subject
            mimemsg.attach(MIMEText(lista_diretoria, 'plain'))
            connection = smtplib.SMTP(host='smtp.office365.com', port=587)
            connection.starttls()
            connection.login(username, password)
            connection.send_message(mimemsg)
            connection.quit()

        if lista_producao != "":
            username = "sami@inaflex.com.br"
            password = "Inf_Rav#M365_2023"
            mail_from = "sami@inaflex.com.br"
            mail_to = "producao@inaflex.com.br"
            # mail_to = "sami@inaflex.com.br"
            mail_subject = "Produção - Eventos pendentes"

            mimemsg = MIMEMultipart()
            mimemsg['From'] = mail_from
            mimemsg['To'] = mail_to
            mimemsg['Subject'] = mail_subject
            mimemsg.attach(MIMEText(lista_producao, 'plain'))
            connection = smtplib.SMTP(host='smtp.office365.com', port=587)
            connection.starttls()
            connection.login(username, password)
            connection.send_message(mimemsg)
            connection.quit()

        if lista_vendas != "":
            username = "sami@inaflex.com.br"
            password = "Inf_Rav#M365_2023"
            mail_from = "sami@inaflex.com.br"
            mail_to = "felipe.orra@inaflex.com.br"
            # mail_to = "sami@inaflex.com.br"
            mail_subject = "Vendas - Eventos pendentes"

            mimemsg = MIMEMultipart()
            mimemsg['From'] = mail_from
            mimemsg['To'] = mail_to
            mimemsg['Subject'] = mail_subject
            mimemsg.attach(MIMEText(lista_vendas, 'plain'))
            connection = smtplib.SMTP(host='smtp.office365.com', port=587)
            connection.starttls()
            connection.login(username, password)
            connection.send_message(mimemsg)
            connection.quit()



'''
here for another time u can use following:
schedule.every(10).minutes.do(RunMyCode)
schedule.every().hour.do(RunMyCode)
schedule.every().day.at(“10:30”).do(RunMyCode)
schedule.every(5).to(10).minutes.do(RunMyCode)
schedule.every().monday.do(RunMyCode)
schedule.every(4).seconds.do(RunMyCode)
schedule.every().wednesday.at(“13:15”)do(RunMyCode)
schedule.every().minutes.at(":17").do(RunMyCode)
'''