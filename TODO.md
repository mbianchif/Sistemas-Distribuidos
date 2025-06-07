# TODO

## Fault-Tolerance
- (Mediano) Agregar el mensaje de Purge (borra el estado de todos los clientes), se envia apenas se levanta el gateway
- (Pesado)  Hacer los Health-Checkers

## Requisitos para la proxima entrega
- (IMPORTANTE)  Agregar la documentacion necesaria al informe/diagramas
- (Mediano)     Agregar Close a todos los nodos
- (Mediano)     Agregar el clean a los nodos stateful para el persistor
- (Pesado)      Cambiar el mailer del Gateway por una entidad mejor

## Cosas que estarian bien
- (Super Facil) Sacar SHARDS de los join
- (Mediano)     Cambiar la imlementación del dump y recover del mailer a que use Persistor
- (Pesado)      Hacer que los mensajes de los workers esten acotados por tamaño
