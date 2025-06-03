# TODO

## Fault-Tolerance
- Agregar Publisher Confirms
- Hacer el explotador aleatorio
- Hacer tolerante al Join
- Hacer tolerante el Gateway
- Hacer los Health-Checkers
- Agregar el mensaje de Cleanse (borra el estado de todos los clientes), se envia apenas se levanta el gateway

## Requisitos para la proxima entrega
- Agregar el Clean a los nodos stateful para el persistor
- Fijarse de ponerle una cota a los mensajes de bache de rabbit, no pedir todos, sino hasta cierto tope
- Agregar en el gateway el FLUSH cuando el cliente termina de enviar todos los archivos
- chequear que todos los recursos se cierren correctamente
- agregar la documentacion necesaria al informe/diagramas

## Cosas que estarian bien
- sacar el failed to recover
- hacer que los mensajes de los workers esten acotados por tamaño
- hacer que cada clientHandler en gateway tenga su propio mailer, asi no hay que sincronizar nada, deberia ser mas rapido
