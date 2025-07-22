# Distribuidos1-Grupo13 - Movies Analysis

## Instrucciones para correr el trabajo:

- Descargar los dataset corriendo el script
```sh
sh download-dataset.sh
```

- Configurar las replicas del sistema y cantidad de clientes en ./configs/compose/config.json

- Generar el compose del sistema
```sh
python3 generate_compose.py
```

- Levantar nodos workers y gateway
```bash
docker compose up --build
```

- Levantar health checkers
```sh
docker compose -f compose.checkers.yaml up --build
```

- Levantar clientes
```sh
docker compose -f compose.clients.yaml up --build
```

- Para matar nodos se puede utilizar
```
docker kill --signal=SIGKILL <nombre-container>
```
