# Protocolo

## Descripción

Todos los checkers usan UDP, tienen 3 sockets:
- Uno para esperar los keep-alives del checker anterior y contestar
- Otro para tirar keep-alives al checker siguiente
- Otro para tirar keep-alives a nodos del pipeline

Cada checker tiene una lista de los nodos a los que tiene que enviar los keep-alives

## Caída de un Checker

Un checker detecta la caída del checker siguiente, lo levanta usando docker in docker

## Caída de Nodo de Pipeline

Un checker detecta que se cayó un checker, lo levanta usando docker in docker
