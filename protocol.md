# Protocolo ej7

### Descripción

El protocolo está implementado en los respectivos archivos protocol.go y protocol.py para el cliente y servidor dentro de sus directorios common.

El cliente implementa el lado de escritura y el servidor el de lectura, ya que gracias a que el protocolo de transporte es TCP no necesitamos un ida y vuelta en ambas partes, podemos siempre suponer la recepción exitosa de los mensajes.

Quise hacer algo sencillo pero a la vez elegante, por lo que implementé una capa de abstracción como wrapper al socket tcp en ambas partes. Del lado del cliente implementé una estructura que modela la apuesta, mientras que del lado del servidor aproveché la ya implementada.

#### Mensaje

Hay 3 tipos de mensajes, siendo estos el de apuestas, el de confirmación y el de los DNIs.

Los mensajes de apuesta y confirmación, llevan un byte por delante que aclara que tipo de mensaje es. Esto es porque son los mensaje que envía el cliente y en particular el mensaje de confirmación no se sabe a priori cuando podría llegar, por lo que tiene que poder ser leído desde el mismo método que el mensaje de apuestas.

#### Mensaje de Apuesta

```terminal
+------------+-------+--------+-----+-----+-------+--------+
| BATCHCOUNT | SIZE1 | BATCH1 | ... | ... | SIZEN | BATCHN |
+------------+-------+--------+-----+-----+-------+--------+
```

- BATCHCOUNT: 4 bytes que establecen la cantidad de baches del mensaje. 
- SIZEi: 4 bytes dedicados a establecer el largo del bache en bytes.
- BATCHi: Datos del i-ésimo bache.

#### Batch

```terminal
+-------------------+
| BET1 ; ... ; BETN |
+-------------------+
```

- BETi: Datos de la i-ésima apuesta.

Las apuestas están separadas por un delimitador, elegí separarlas por `;`.

#### Bet

```terminal
+---------------------------------------------------+
| Agency , Name , Surname , Id , Birthdate , Number |
+---------------------------------------------------+
```

Los campos de cada apuesta están separadas por un delimitador, elegí separarlos por `,`.

#### Mensaje de Confirmación

Este mensaje es vacío, solo con saber el tipo de mensaje es suficiente, por lo que no tiene datos.

#### Mensaje de DNIs

Decidí parsear los DNI y enviarlos como enteros, concluyendo en la siguiente estructura. Un detalle es que el enunciado pide enviar todos los dni, era suficiente pasar la cantidad de ganadores para poder loggearla.

```terminal
+-------+------+-----+------+
| COUNT | DNI1 | ... | DNIN |
+-------+------+-----+------+
```

- COUNT: 4 bytes que almacenan la cantidad de dnis en el mensaje.
- DNIi: 4 bytes con el número de documento.

