# Protocolo

## Descripción

Todos los checkers usan UDP, tienen 2 sockets:
- Uno para chequear los nodos del pipeline y pedirle al lider que levante a un nodo
- Otro para tirar keep-alives entre checkers

Cada checker tiene una lista de nodos caídos detectados, arranca por esos siempre.

## Caída de Nodo de Pipeline

Un checker detecta una caída y se la envía al lider, el lider remata al nodo y lo levanta

## Caída de un Checker

Un chequer detecta que se cayó un checker, el líder eventualmente se da cuenta y lo levanta. Si se cae
el lider, se corre un algoritmo de token ring para elegir un nuevo lider, y es este nuevo lider quien
levanta al anterior caido.

## Caída del Lider mientras levantaba un Nodo de Pipeline

Se muere el lider mientras levantaba un nodo, lo detectan los demás, eligen otro lider, ese levanta al anterior
y eventualmente se vuelve a detectar la caída.
