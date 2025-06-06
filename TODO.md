# TODO

## Fault-Tolerance
- (Mediano) Hacer tolerante al Join (falta solo el recover)
- (Facil)   Agregar el mensaje de Purge (borra el estado de todos los clientes), se envia apenas se levanta el gateway
- (Pesado)  Cambiar el mailer del Gateway por una entidad mejor
- (Mediano) Hacer tolerante el Gateway
- (Pesado)  Hacer los Health-Checkers

## Requisitos para la proxima entrega
- (Facil)   Agregar Close a todos los nodos
- (Mediano) Agregar el clean a los nodos stateful para el persistor

## Cosas importantes
- chequear que todos los recursos se cierren correctamente
- agregar la documentacion necesaria al informe/diagramas

## Cosas que estarian bien
- hacer que los mensajes de los workers esten acotados por tamaño
