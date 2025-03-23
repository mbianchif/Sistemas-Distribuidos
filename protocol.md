# Protocolo ej6

### Descripción

El protocolo está implementado en los respectivos archivos protocol.go y protocol.py para el cliente y servidor dentro de sus directorios common.

El cliente implementa el lado de escritura y el servidor el de lectura, ya que gracias a que el protocolo de transporte es TCP no necesitamos un ida y vuelta en ambas partes, podemos siempre suponer la recepción exitosa de los mensajes.

Quise hacer algo sencillo pero a la vez elegante, por lo que implementé una capa de abstracción como wrapper al socket tcp en ambas partes. Del lado del cliente implementé una estructura que modela la apuesta, mientras que del lado del servidor aproveché la ya implementada.

#### Mensaje

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
