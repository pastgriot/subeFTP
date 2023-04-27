import os
import ftplib
import socket
import time
import logging
import configparser


class enviaFTP():

    def __init__(self, ftp_hostname=None, ftp_user=None, ftp_passwd=None, base_path=None, dest_path='Enviados', file_prefix =()):
        '''
        Args:
        ft_hostname: dirección ip del servidor FTP.
        ftp_user: nombre del usuario FTP.
        ftp_passwd: clave asociada al usuario.
        base_path: direccion de la donde se alojan los archivos que serpan enviados, si este
                    parametro queda vacio, el fichero donde se encuentra el script será considerada la raiz
        dest_path: Nombre y direccion de la carpeta donde se alojarán los archivos exitosamente enviados, por defecto
                    se creará la carpeta "Enviados" en el fichero raiz.
        file_prefix: tupla que contiene la o las cadenas string con los prefijos a ser procesados. 
        '''
        self.ftp_hostname = ftp_hostname
        self.ftp_user = ftp_user
        self.ftp_passwd = ftp_passwd
        self.dest_path = dest_path
        self.file_prefix = file_prefix

        if not base_path:
            self.base_path = os.getcwd()
        else:
            self.base_path = base_path

        #asumo carpeta raiz en el servidor
        self.ftp_server_dir = '/';
        #configuro un log de actividades
        fmt = '%(asctime)s | %(levelname)s | %(message)s'
        logging.basicConfig(level=logging.DEBUG, 
                    format=fmt, 
                    handlers=[logging.StreamHandler(), 
                              logging.FileHandler("ftp.log")])
        

        #inicio las carpetas para procesar y guardar
        self.processPath = os.path.join(self.base_path, base_path)
        self.enviadosPath = os.path.join(self.base_path, self.dest_path)

        #si no existen, las creo
        if not os.path.isdir(self.processPath):
            logging.info(f'Carpeta {self.processPath} no encontrada')
            os.mkdir(self.processPath)
            logging.info(f'Carpeta {self.processPath} creada')
        
        dir_path = os.path.join(self.base_path, self.dest_path)  
        if not os.path.isdir(dir_path):
            logging.info(f'Carpeta {dir_path} no encontrada')
            os.mkdir(dir_path)
            logging.info(f'Carpeta {dir_path} creada')
    

    def busca_y_envia(self):
        '''busca las coincidencias entre los archivos que contengan los
        prefijos configurados y envia los archivos via FTP.
        '''
        try:
            #reviso si hay algo para enviar
            file_list = os.listdir(self.processPath)
            if not file_list:
                logging.info("No existen archivos para enviar, esperando...")

            #reviso si hay algo, envío
            else:
                #abro la sesión con el servidor FTP
                with ftplib.FTP(host=self.ftp_hostname, user=self.ftp_user, passwd=self.ftp_passwd, timeout=3) as ftp:
                    ftp.cwd(self.ftp_server_dir)
                    logging.info(f"Accediendo al directorio {self.ftp_server_dir} en el servidor FTP")
                    #recorro todos los archivos en la carpeta de proceso
                    file_list = os.listdir(self.processPath)
                    for i in file_list:   
                        if i.startswith(self.file_prefix):
                            try:
                                with open(os.path.join(self.processPath, i), 'rb') as file:
                                    logging.info(f"Subiendo {i}...")
                                    #envío el archivo al servidor
                                    ftp.storbinary(f'STOR {i}', file)
                                #si todo resultó bien, muevo el archivo enviado a la carpeta de enviados
                                os.rename(os.path.join(self.processPath, i), os.path.join(self.enviadosPath,i))
                            except Exception as e:
                                logging.error(f'No se pudo enviar el archivo {i}: {str(e)}')
                                continue
        #logeo errores                      
        except socket.gaierror:
            logging.critical('IP de FTP inválida')

        except ftplib.error_perm:
            logging.critical('Error de autentificacion en FTP Server, revise clave y usuario')

        except FileNotFoundError:
            logging.critical(f'{file_list} Dirección del archivo incorrecta')

        except socket.timeout as e:
            logging.critical(f'Error al conectar con servidor FTP: {self.ftp_hostname} - {e}')


if __name__ == '__main__':
    

    config = configparser.ConfigParser()
    config.read('config.ini')

    hostname = config['FTP_SERVER']['hostname']
    user = config['FTP_SERVER']['usuario']
    passwd = config['FTP_SERVER']['passwd']

    base_path = config['ARCHIVOS']['directorio_raiz']
    dest_path = config['ARCHIVOS']['directorio_enviados']
    sleep_time = int(config['ARCHIVOS']['frecuencia_de_envio'])
    file_prefix = tuple(i for i in str.split(config['ARCHIVOS']['prefijo'], ',') if i != '')

    enviaftp = enviaFTP(ftp_hostname = hostname, ftp_user = user, ftp_passwd = passwd, base_path = '', dest_path = dest_path, file_prefix=file_prefix)
    
    while True:
        enviaftp.busca_y_envia()
        time.sleep(sleep_time)

