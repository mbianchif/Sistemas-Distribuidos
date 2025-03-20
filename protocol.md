# Protocolo ej5

### Descripción

El protocolo está implementado en los respectivos archivos protocol.go y protocol.py para el cliente y servidor dentro de sus directorios common.

El cliente implementa el lado de escritura y el servidor el de lectura, ya que gracias a que el protocolo de transporte es TCP no necesitamos un ida y vuelta en ambas partes, podemos siempre suponer la recepción exitosa de los mensajes.

Quise hacer algo sencillo pero a la vez elegante, por lo que implementé una capa de abstracción como wrapper al socket tcp en ambas partes. Del lado del cliente implementé una estructura que modela la apuesta, mientras que del lado del servidor aproveché la ya implementada.

+--------+---------+  
|  SIZE  | PAYLOAD |  
+--------+---------+  

### Campos

- SIZE: 4 bytes dedicados a establecer el largo del payload en bytes.
- PAYLOAD: Los datos de la apuesta, estos se almacenan en formato utf-8 y están guardados siguiendo el orden definido por la clase Bet dentro del archvo utils.py y tienen un caracter especial como separador.

