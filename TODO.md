# TODO

## Fault-Tolerance
- (Mediano) Terminar de probar el Purge
- (Pesado)  Hacer los Health-Checkers

## Requisitos para la proxima entrega
- (IMPORTANTE) Agregar la documentacion necesaria al informe/diagramas
- (Mediano)    Agregar Close a todos los nodos
- (Mediano)    Agregar el clean a los nodos stateful para el persistor
- (Pesado)     Cambiar el mailer del Gateway por una entidad mejor

## Cosas que estarian bien
- (Mediano) Cambiar la imlementación del dump y recover del mailer a que use Persistor
- (Mediano) shardear los archivos del join para tener menos archivos, deberia ser mucho mas rapida la parte de izquierda
- (Pesado)  Hacer que los mensajes de los workers esten acotados por tamaño
