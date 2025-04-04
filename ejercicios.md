# Parte 1

## Ejercicio 1

Se implementó un generador de archivo compose para correr con docker. Este archivo es `generar-compose.sh` el cual se ejecuta de la siguiente manera

```sh
sh generar-compose.sh <nombre-de-archivo-output> <n-clientes>
```

## Ejercicio 2

En este ejercicio se amplió la funcionalidad del archivo previamente mencionado para facilitar la configuración del servidor y los clientes utilizando _volumes_ de docker para no tener que copiar estos archivos dentro de la imágen y poder cambiarla sin tener que buildearla otra vez.

## Ejercicio 3

Se implementó un validador del servidor, que para este entonces es un echo-server. Este programa `validar-echo-server.sh`, busca la coniguración del servidor para obtener su _ip_ y _puerto_ y levanta un contenedor de docker corriendo la imágen de `subfuzion/netcat` que se conecta a la _network_ del servidor para hacer una consulta y verificar que el servidor esté efectivamente levantado y listo para recibir consultas.

Esta verificación la hace enviando un string y validando que reciba el mismo string enviado. Para ambos casos sea de exito o falla imprime un mensaje acorde.

```sh
sh validar-echo-server.sh
```

## Ejercicio 4

En esta parte se amplió la funcionalidad del servidor y los clientes de forma que ahora puedan hacer un _graceful-shutdown_ manejando la señal de terminación _SIGTERM_.

# Parte 2

Para esta parte se modificó el completo funcionamiento del servidor y los clientes para simular la _Loteria Nacional_. Para esto se implementó un protocolo de comunicación que fue evolucionando a medida avanzaron los ejercicios.

El protocolo está implementado en los archivos `common/protocol.py` y `common/protocol.go` para el servidor y el cliente respectivamente.

## Ejercicio 5

Para este ejercicio y el siguiente el cliente implementa el lado de escritura y el servidor el de lectura, ya que no es necesario _todavía_ que el servidor envie mensajes a los clientes.


### Protocolo

#### Mensaje

```terminal
+------+-----+
| SIZE | BET |
+------+-----+
```

* `SIZE`: 4 bytes que establecen el largo del payload en bytes.
* `BET`: Los datos de la apuesta.

#### BET

```terminal
+---------------------------------------------------+
| Agency , Name , Surname , Id , Birthdate , Number |
+---------------------------------------------------+
```

La apuesta es representada por sus campos en el mismo orden en el que están definidos en la clase `Bet` dentro de `common/utils.py` de forma que sea sencillo de deserializar. Los campos se guardan en formato string y están serparados por una `,`.

### Ejecución

Para la ejecución del mismo es suficiente con correr el siguiente comando

```sh
make docker-compose-up
```

El archivo `docker-compose-dev.yaml` está configurado para correr con un solo cliente, en caso de querer correr con más habra que correr el comando del `Ejercicio 1` para más clientes.

Para ver los logs del sistema se puede correr el comando

```sh
make docker-compose-logs
```

## Ejercicio 6

Se implementó el envío de mensajes por baches. Para esto se utilizaron los _volumes_ de docker para evitar que los archivos de datos formen parte de la imágen de docker y se implementó el siguiente protocolo.

### Protocolo

#### Mensaje

```terminal
+------------+-------+--------+-----+-----+-------+--------+
| BATCHCOUNT | SIZE1 | BATCH1 | ... | ... | SIZEN | BATCHN |
+------------+-------+--------+-----+-----+-------+--------+
```

* `BATCHCOUNT`: 4 bytes que establecen la cantidad de baches del mensaje. 
* `SIZEi`: 4 bytes que establecen el largo del bache en bytes.
* `BATCHi`: Datos del i-ésimo bache.

#### Batch

```terminal
+-------------------+
| BET1 ; ... ; BETN |
+-------------------+
```

* `BETi`: Datos de la i-ésima apuesta.

Las apuestas están separadas por un delimitador, elegí separarlas por `;`.

#### Bet

```terminal
+---------------------------------------------------+
| Agency , Name , Surname , Id , Birthdate , Number |
+---------------------------------------------------+
```

Los campos de cada apuesta están separadas por un delimitador, elegí separarlos por `,`.

### Ejecución

Para la ejecución es igual que el ejercicio 5, el archivo de compose está configurado con un único cliente, en caso de querer agregar más se puede utilizar el archivo `generar-compose.yaml` para agregar más.

## Ejercicio 7

Se implementó un protocolo para que el cliente sea capaz de enviar distintos tipos de mensajes. Para esto se agrego una variable de entorno `NCLIENTS` que le permite al servidor saber de antemano cuantos clientes se van a conectar para enviar las apuestas.

## Protocolo

#### Mensaje

Hay 3 tipos de mensajes, siendo estos el de apuestas, el de confirmación y el de los DNIs.

Los mensajes de apuesta y confirmación, llevan un byte por delante que aclara que tipo de mensaje es. Esto es porque son los mensaje que envía el cliente y en particular el mensaje de confirmación no se sabe a priori cuando podría llegar, por lo que tiene que poder ser leído desde el mismo método que el mensaje de apuestas.

#### Mensaje de Apuesta

```terminal
+------------+-------+--------+-----+-----+-------+--------+
| BATCHCOUNT | SIZE1 | BATCH1 | ... | ... | SIZEN | BATCHN |
+------------+-------+--------+-----+-----+-------+--------+
```

* `BATCHCOUNT`: 4 bytes que establecen la cantidad de baches del mensaje. 
* `SIZEi`: 4 bytes dedicados a establecer el largo del bache en bytes.
* `BATCHi`: Datos del i-ésimo bache.

#### Batch

```terminal
+-------------------+
| BET1 ; ... ; BETN |
+-------------------+
```

* `BETi`: Datos de la i-ésima apuesta.

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

* `COUNT`: 4 bytes que almacenan la cantidad de dnis en el mensaje.
* `DNIi`: 4 bytes con el número de documento.

### Ejecución

Para ejecutar el ejemplo, es otra vez igual, utilizando el mismo comando de las otras veces. El archivo `docker-compose-dev.yaml` está configurado con 5 clientes, en caso de querer cambiarlo se puede utilizar el archivo generador el primer ejercicio.

# Parte 3

## Ejercicio 8

Se modificó la implementación del servidor para que pueda responder en paralelo a las consultas de los clientes. Para ello se utilizaron las siguientes primitivas de sincronización.

### Sync

Utilicé el módulo `multiprocessing` para evitar el GIL de python.

Por cada nueva conexión, el servidor crea un nuevo `Process` al que le pasa por argumento un `Lock` y una `Barrier`.

Una vez el servidor inició la cantidad esperada de clientes, este cierra su socket listener y queda esperando a que sus subprocesos terminen y así cerrar los sockets de conexión con los clientes.

Dentro del método `Server._handle_client_connection`, se reciben mensajes hasta recibir la confirmación del cliente de que no va a enviar más mensajes de apuestas, entonces entra en `Server._send_winners`.

Aquí se hace uso del `Lock` y la `Barrier`, donde la barrera sirve para sincronizar a todos los clientes a esperar a que todos confirmen haber enviado a todas sus apuestas. Una vez se rompe la barrera, los subprocesos compiten por el lock del archivo de apuestas del servidor para calcular sus ganadores y ser enviados al respectivo cliente.

Para pasar los tests se hizo uso de un `sleep` al final de la ejecución del servidor, no es utilizado para sincronizar procesos.

### Ejecución

Para ejecutar el ejemplo, es igual que los otros ejercicios. El archivo `docker-compose-dev.yaml` está configurado con 5 clientes.
