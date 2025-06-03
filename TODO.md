# TODO

## Fault-Tolerance
- (Pesado) Hacer tolerante al Join
- (Facil)  Hacer el explotador aleatorio
- (Facil)  Agregar Publisher Confirms
- (Pesado) Hacer tolerante el Gateway
- (Pesado) Hacer los Health-Checkers
- (Facil)  Agregar el mensaje de Cleanse (borra el estado de todos los clientes), se envia apenas se levanta el gateway

## Requisitos para la proxima entrega
- (Mediano) Agregar el Clean a los nodos stateful para el persistor
- (Facil)   Fijarse de ponerle una cota a los mensajes de bache de rabbit, no pedir todos, sino hasta cierto tope
- (Facil)   Agregar en el gateway el FLUSH cuando el cliente termina de enviar todos los archivos
- chequear que todos los recursos se cierren correctamente
- agregar la documentacion necesaria al informe/diagramas

## Cosas que estarian bien
- sacar el failed to recover
- hacer que los mensajes de los workers esten acotados por tamaño
- hacer que cada clientHandler en gateway tenga su propio mailer, asi no hay que sincronizar nada, deberia ser mas rapido
