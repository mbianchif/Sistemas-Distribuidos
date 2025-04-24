# Distribuidos1-Grupo13 - Movies Analysis

## Instrucciones para correr el trabajo:

- Descargar los dataset corriendo el script
  ```bash
  sh download-dataset.sh
  ```
- Generar el compose del sistema
  ```bash
  python3 generar_compose.py
  ```
- Correr el compose para levantar el programa
  ```bash
  docker compose up --build
- Correr el compose para levantar el cliente
  ```sh
  docker compose -f client-compose.yaml up --build
  ```
