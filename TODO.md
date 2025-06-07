# TODO

## Fault-Tolerance
- (Mediano) Hacer tolerante al Join (falta solo el recover)
- (Mediano) Agregar el mensaje de Purge (borra el estado de todos los clientes), se envia apenas se levanta el gateway
- (Pesado)  Hacer los Health-Checkers

## Requisitos para la proxima entrega
- (Facil)   Agregar Close a todos los nodos
- (Mediano) Agregar el clean a los nodos stateful para el persistor
- (Pesado)  Cambiar el mailer del Gateway por una entidad mejor

## Cosas importantes
- chequear que todos los recursos se cierren correctamente
- agregar la documentacion necesaria al informe/diagramas
- Sacar SHARDS de los join

## Cosas que estarian bien
- hacer que los mensajes de los workers esten acotados por tamaño
