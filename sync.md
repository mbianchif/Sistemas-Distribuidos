# Sync ej8

### Descripción

Utilicé el módulo `multiprocessing` para evitar el GIL de python.

Por cada nueva conexión, el servidor crea un nuevo `Process` al que le pasa por argumento un `Lock` y una `Barrier`.

Una vez el servidor inició la cantidad esperada de clientes, este cierra su socket listener y queda esperando a que sus subprocesos terminen y así cerrar los sockets de conexión con los clientes.

Dentro del método `Server._handle_client_connection`, se reciben mensajes hasta recibir la confirmación del cliente de que no va a enviar más mensajes de apuestas, entonces entre a `Server._send_winners`.

Acá se hace uso del `Lock` y la `Barrier`, donde la barrera sirve para sincronizar a todos los clientes a esperar a que todos confirmen haber enviado a todas sus apuestas. Una vez se rompe la barrera, los subprocesos compiten por el lock del archivo de apuestas del servidor para calcular sus ganadores y ser enviados al respectivo cliente.
