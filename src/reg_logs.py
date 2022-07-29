import logging as log

# Configuración de log
log.basicConfig(level=log.INFO,
                filename='debug.log',
                filemode= 'w',
                format="[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
                )