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

