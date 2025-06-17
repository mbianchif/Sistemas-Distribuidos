# TODO

## Fault-Tolerance
- Chequear por que al momento de revivir un container, este no parece estar leyendo las colas de entrada

## Requisitos para la proxima entrega
- (IMPORTANTE) Agregar la documentacion necesaria al informe/diagramas

## Cosas que estarian bien
- (Mediano) Chequear los accesos a las variables publicas y privadas, que las clases no expongan cosas al dope
- (Mediano) Cambiar la imlementación del dump y recover del mailer a que use Persistor
- (Mediano) shardear los archivos del join para tener menos archivos, deberia ser mucho mas rapida la parte de izquierda
- (Pesado)  Hacer que los mensajes de los workers esten acotados por tamaño
