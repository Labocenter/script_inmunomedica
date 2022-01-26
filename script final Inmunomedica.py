import csv
import mysql.connector
import json
from datetime import datetime
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
import codecs


fecha = datetime.today().strftime('%Y-%m-%d')
fecha_consulta_sql = datetime.today().strftime('%Y-%m-%d')
fecha_actual = datetime.today()
token = []



# #HOSTPRODUCCION
host = "10.10.1.230"
database = "labo_center_infante"
DEBUG = False


# #HOST DESARROLLO
#host = "10.10.1.120"
#database = "alonso"
#DEBUG = True




# URL WEB SERVICE DE CONSULTA
url_token = 'http://inmunomedicaqa.flowmed.cl/api/auth/getAutenticacion'
url_ingreso = 'http://inmunomedicaqa.flowmed.cl/api/auth/crearAtencion'
crendenciales = {"username": "labocenterqa", "password": "labocenterqa"}


# MANEJO REGISTROS Y RES
lista_personas = []
pacientes_pendientes = []




# LECTURA Y VALIDACION DE TOKEN
try:

    # APERTURA DE ARCHIVO TOKEN

    with open("token_save.txt") as json_file:

        datos_token = json.load(json_file)

        if datos_token:

            print("TOKEN EXISTES Y READ OK")

            expiracion_token = datetime.strptime(str(datos_token.split("fechaExp")[1].split('"')[2]), "%Y-%m-%d %H:%M:%S")

            if fecha_actual > expiracion_token:

                print("SE DEBE CREAR TOKEN POR EXPIRACION")

                response_token = requests.get(url_token, params=crendenciales)

                if str(response_token.status_code) == "200":

                    print(response_token.status_code)

                    file = open("token_save.txt", "w")

                    json_dicti = json.dumps(response_token.text)

                    file.write(json_dicti)

                    file.close()

                    print("Open new token")

                    with open("token_save.txt") as json_file:
                        datos_token = json.load(json_file)

            else:
              
                print("TOKEN VIDA UTIL OK")
                response_token = requests.get(url_token, params=crendenciales)

                print("RESPUESTA TOKEN WS")
                print(response_token.text)


                s = str(response_token.text).split(",")[0].split(':')[1].replace('"','')
                print("***---***")
                print(s)

                if s == str(1):
                    print("****TOKEN EXPIRADO POR SERVER*******")
                    file = open("token_save.txt", "w")

                    json_dicti = json.dumps(response_token.text)

                    file.write(json_dicti)

                    file.close()
                    print("Open new token")

                    with open("token_save.txt") as json_file:
                        datos_token = json.load(json_file)

     


except:

  print("***ENTRO TRY - NO EXISTE TOKEN U ERROR**")

  file = open("token_save.txt", "w")

  response_token = requests.get(url_token, params=crendenciales)

  json_dicti = json.dumps(response_token.text)

  file.write(json_dicti)

  file.close()


class Atencion:

  def __init__(self, rut, genero, comuna, correo, nombre, nombre2, telefono, apellidoM, apellidoP, direccion, fecha_nac, prevision, nacionalidad, fechaAtencion, centroAtencion):

    self.rut = rut

    self.genero = genero

    self.comuna = comuna

    self.correo = correo

    self.nombre = nombre

    self.nombre2 = nombre2

    self.telefono = telefono

    self.apellidoM = apellidoM

    self.apellidoP = apellidoP

    self.direccion = direccion

    self.fecha_nac = fecha_nac

    self.prevision = prevision

    self.nacionalidad = nacionalidad

    self.fechaAtencion = fechaAtencion

    self.examenes = []

    self.centroAtencion = centroAtencion


class Examen:

  def __init__(self, examen, resultado):

    self.examen = examen

    self.resultado = resultado


def get_data_rec_id(id_convenio, fecha_consulta):

    mydb = mysql.connector.connect(

        host=host,

        user="developer",

        password="123456..",

        database=database

    )

    cursor = mydb.cursor()

    sql = "SELECT rec.REC_ID "

    sql += "FROM recepcion rec "

    sql += "JOIN paciente p ON rec.paciente_PAC_ID = p.PAC_ID "

    sql += "join recepcion_has_procedimiento rhp on rhp.recepcion_REC_ID = rec.REC_ID "

    sql += "WHERE p.convenio_con_id = '" + id_convenio + "' and rec.REC_HORA_RECEPCION like '%" + \
        fecha_consulta + "%'  and rhp.procedimiento_PRO_ID = '3baa5dd9-8fa4-11ea-b766-0050568a4f21' order by 1 asc"

    cursor.execute(sql)

    myresult = cursor.fetchall()

    return myresult


def examen_rec_id(codigo_rec):

    mydb = mysql.connector.connect(

        host=host,

        user="developer",

        password="123456..",

        database=database

    )

    cursor = mydb.cursor()

    sql = "SELECT  proc.PRO_CODIGO FROM recepcion_has_procedimiento rh join procedimiento proc on rh.procedimiento_PRO_ID = proc.PRO_ID "

    sql += "WHERE rh.recepcion_REC_ID= '" + codigo_rec + "'"

    cursor.execute(sql)

    myresult = cursor.fetchall()

    return myresult


def write_csv_procesados(lista_pacientes):

    with open('pacientes_proc.csv', 'w', newline='') as csvfile:

        spamwriter = csv.writer(csvfile, delimiter=';')

        for row in lista_pacientes:

            spamwriter.writerow(row)


def read_procesados(proc_csv):

    fecha = datetime.today().strftime('%Y-%m-%d')

    with open(proc_csv) as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')

        procedimientos = []

        for row in csv_reader:

            # print("var fecha:" +str(fecha +"#### Fecha row"+str(row[14])))

            if str(row[14]) == str(fecha):

                # print("Fecha ok"+str(row[14]))

                procedimientos.append(row)

    return procedimientos


def log_historico_completo(log_csv):

    with open(log_csv) as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')

        lista_log = []

        for row in csv_reader:

            lista_log.append(row)

    return lista_log


def read_log_rut(proc_csv):

    with open(proc_csv) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')

        id_convenio = "c797e356-c651-11ea-99b2-0050568a8768"

        fecha = datetime.today().strftime('%Y-%m-%d')

        procedimientos = []
        for row in csv_reader:
            procedimientos.append(str(row[1]))




    return procedimientos



def read_log_numero_rec(proc_csv):

    with open(proc_csv) as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=';')

        id_convenio = "c797e356-c651-11ea-99b2-0050568a8768"

        fecha = datetime.today().strftime('%Y-%m-%d')



        procedimientos = []

        for row in csv_reader:

            procedimientos.append(str(row[0]))





    return procedimientos


# update  alonso.recepcion set REC_HORA_RECEPCION = '2022-01-20 11:22:10'
# WHERE convenios_con_id = 'c797e356-c651-11ea-99b2-0050568a8768'




def get_pacientes(id_convenio,fecha_consulta):
    mydb = mysql.connector.connect(
        host=host,
        user="developer",
        password="123456..",
        database=database
    )
    cursor = mydb.cursor()
    
    sql = "select rec.REC_ID,"
    sql += " replace(p.PAC_RUT,'.','')AS rut,"
    sql += "CASE WHEN PAC_SEXO_C = 'M' THEN 'MASCULINO' WHEN PAC_SEXO_C = 'F' THEN 'FEMENINO' ELSE '' END AS genero,'Santiago' AS comuna, "
    sql += "case when PAC_CORREO not like '%@%' then 'sincorreo@sincorre.cl' else PAC_CORREO end as correo,SUBSTRING_INDEX(PAC_NOMBRES, ' ', 1) AS nombre "
    sql += ",case when PAC_NOMBRES not like '% %' then 'n/a'when SUBSTRING_INDEX( PAC_NOMBRES, ' ', -1) <> '' then SUBSTRING_INDEX(rtrim(PAC_NOMBRES), ' ', -1) when SUBSTRING_INDEX( PAC_NOMBRES, ' ', -1) = SUBSTRING_INDEX( PAC_NOMBRES, ' ', -1) then 'sin nombre' else 'sin nombre' end as nombre2 "
    sql += ", ltrim(substring(replace(PAC_TEL_CELULAR,'-',''),7,10)) as telefono,PAC_AP_MATERNO as apellidoM,PAC_AP_PATERNO as apellidoP ,'sin direccion' AS direccion ,PAC_FECHA_NACIMIENTO AS fecha_nac,pre.PRE_DESCIPCION AS prevision,upper(n.NAC_DESCRIPCION) AS nacionalidad,DATE_FORMAT(rec.REC_HORA_RECEPCION, '%Y-%m-%d') AS fechaAtencion "
    sql += ",'SANTIAGO - JOSE MANUEL INFANTE 146 - LABOCENTER' as centroAtencion"
    sql += " FROM recepcion rec join paciente p on rec.paciente_PAC_ID = p.PAC_ID  JOIN prevision pre ON p.PREVISION_PRE_ID = pre.PRE_ID JOIN nacionalidad n ON n.NAC_ID = p.NACIONALIDAD_NAC_ID "
    sql += "join recepcion_has_procedimiento rhp on rhp.recepcion_REC_ID = rec.REC_ID "
    sql += "WHERE p.convenio_con_id = '" + id_convenio + "'and rhp.procedimiento_PRO_ID = '3baa5dd9-8fa4-11ea-b766-0050568a4f21' and rec.REC_HORA_RECEPCION like '%" + fecha_consulta + "%' order by 1 asc"
   

    cursor.execute(sql)
    myresult = cursor.fetchall()
    return myresult




def main():
 
    # CONVENIO INMUNOMEDICA

    id_convenio = "c797e356-c651-11ea-99b2-0050568a8768"

    ls_pac = get_pacientes(id_convenio,fecha_consulta_sql);



    # Data pendiente de procesar

    data_pendiente = get_data_rec_id(id_convenio,fecha_consulta_sql)




    # LOG LISTA CON LOS PACIENTES PROCESADOS

    log_historico = log_historico_completo('pacientes_proc.csv')

    lista_log = read_log_numero_rec('pacientes_proc.csv')

    leer_log_rut = read_log_rut('pacientes_proc.csv')

    log_general = read_procesados('pacientes_proc.csv')




    for pend in ls_pac:

      if str(pend[0]).replace("('","").replace("',)","") not in lista_log:

        p1 = Atencion(pend[1],pend[2],pend[3],pend[4],pend[5],pend[6],pend[7],pend[8],pend[9],pend[10],str(pend[11]),pend[12],pend[13],str(pend[14]),pend[15])

        examen = examen_rec_id(str(pend[0]).replace("('","").replace("',)",""))



        for item in examen:

          ex = Examen(str(item).replace("('","").replace("',)","").replace("P000040-01","PCR"),"")

          p1.examenes.append(json.dumps(ex.__dict__))



        # AGREGA PACIENTE QUE NO ESTA EN LISTA LOG

        lista_personas.append(json.dumps(p1.__dict__))





    # print (str(lista_personas).replace("\\","").replace("'","").replace('"{','{').replace('}"','}'))



    datos_post = '{"datos":'+str(lista_personas).replace("\\","").replace("'","").replace('["{"examen"','[{"examen"').replace('""}"','""}') +'}'



    if DEBUG == True:
        print("Ambiente desarrollo...")
        datos_post = '{"datos":[]}'
    
    validador_datos_vacios = False
    ##VALIDA SI ARRAY A ENVIAR ESTA VACIO
    if datos_post == '{"datos":[]}':
        print("DATOS ENVIO VACIO - NADA QUE ENVIAR")

    else:
        print("##################################################################")


        codigo_send_token = str(datos_token.split("access_token")[1].split(':')[1].split('"')[1])

    

        auth_token = str(codigo_send_token)

        hed = {'Authorization': 'Bearer ' + codigo_send_token}


        envio = json.loads(datos_post)



        response_ws = requests.post(url_ingreso, json=envio, headers=hed)

        print(response_ws.text)


        variable = response_ws.text.split("</pre>")[1]

        json_dicti = json.loads(variable)



        rut_obs = []

        rut_nuevo_log = []

        validador_env_email = False



        for i in json_dicti["data"]["errores:"]["estado"]["errores:"]:

            if i['codigo'] == 1 or i['codigo'] == 3:

                rut_annotation =str(i['mensaje']).lstrip().split(":")[1]
                validador_env_email = True
                rut_obs.append(rut_annotation.lstrip())

        for index, item in enumerate(lista_personas):

            if item.split(",")[0].replace('{"rut": "','').replace('"','') not in rut_obs:


                rut_nuevo_log.append(item.split(",")[0].replace('{"rut": "','').replace('"',''))

        for l in rut_nuevo_log:

            if l not in rut_obs and l not in leer_log_rut:

                for x in ls_pac:

                    if l == x[1]:
                        log_historico.append(x)



        write_csv_procesados(log_historico)



    #######################################################################################################

    # ENVIO DE INFORMACION POR EMAIL



        def send_email(email_recipient,

                       email_subject,

                       email_message,

                       attachment_location = ''):



            email_sender = 'noreply@labocenter.cl'



            msg = MIMEMultipart()

            msg['From'] = email_sender

            msg['To'] = email_recipient

            msg['Subject'] = email_subject



            msg.attach(MIMEText(email_message, 'html'))



            if attachment_location != '':

                filename = os.path.basename(attachment_location)

                attachment = open(attachment_location, "rb")

                part = MIMEBase('application', 'octet-stream')

                part.set_payload(attachment.read())

                encoders.encode_base64(part)

                part.add_header('Content-Disposition',

                                "attachment; filename= %s" % filename)

                msg.attach(part)



            try:

                server = smtplib.SMTP('smtp.office365.com', 587)

                server.ehlo()

                server.starttls()

                server.login('noreply@labocenter.cl', 'Labocenter.2022')

                text = msg.as_string()

                server.sendmail(email_sender, email_recipient, text)

                print('email sent')

                server.quit()

            except:

                print("SMPT server connection error")

            return True





        table = "<table>\n"



        # Create the table's column headers

        header = ['Rut']

        table += "  <tr>\n"

        for column in header:

            table += "    <th>{0}</th>\n".format(column.strip())

        table += "  </tr>\n"



        # Create the table's row data

        for line in rut_obs:

            row = line.split(",")

            table += "  <tr>\n"

            for column in row:

                table += "    <td>{0}</td>\n".format(column.strip())

            table += "  </tr>\n"



        table += "</table>"



        cuerpo_correo="<h3>Pacientes con observaciones</h3>"

        message = """

        <h3>Pacientes pendientes:</h3>

        <b>Listado:</b>

        <br>

        """+table

        f=codecs.open("firma.html", 'r')

        message = message + str(f.read())



        if validador_env_email == True:
            print(" ")

##            ##QA LABOCENTER
##            send_email("alonso.painevilo@labocenter.cl",'PRUEBA QA INFORMATICA',message,'')

            #PRODUCCION
            send_email("soporte@secall.cl",'PACIENTES CON OBSERVACIONES',message,'')
            send_email("andres.giraudo@inmunomedica.cl",'PACIENTES CON OBSERVACIONES',message,'')
            send_email("barbajose@gmail.com",'PACIENTES CON OBSERVACIONES',message,'')

            




if __name__ == "__main__":
    main()
