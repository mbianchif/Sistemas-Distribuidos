# Health Checker

Monitorea activamente el estado de las otras entidades del sistema.

## 🚀 Funcionalidad

- **Protocolo de capa de transporte** a través de UDP.

## 🔐 Configuración

**Variables de entorno**:
El health checker obtiene la configuración desde las variables de entorno definidas en el archivo `config`.

Las variables esperadas son:

- `HEALTH_CHECKER_PORT`: El puerto usado para enviar y recibir keep alives. (Debe ser consistente en todas las entidades).
- `DEFAULT_SLEEP_DURATION`: Duración en segundos del tiempo que duerme el checker entre iteraciones.
- `REVIVE_SLEEP_DURATION`: Duración en segundos del tiempo que duerme el checker entre iteraciones si es que encuentra por lo menos un nodo caído.
- `STARTING_KEEP_ALIVE_WAIT_DURATION`: Duración en segundos base que el checker espera hasta recibir un mensaje antes de reintentar.
- `STARTUP_GRACE_DURATION`: Duración en segundos que espera un checker desde que inicializa su modulo de ack hasta que inicia su modulo de monitoreo.
- `KEEP_ALIVE_RETRIES`: Cantidad de veces que el monitor va a reintentar enviar keep alives.
- `REVIVE_RETRIES`: Cantidad de veces que el monitor va a intentar de resucitar a un nodo dado que esta acción falle.
- `ID`: El id del checker, debe ser único.
- `N`: Cantidad de checkers activos en el sistema.
- `HOST_NAME`: El nombre del container sin su id.
- `WATCH_NODES`: Una lista de nombres de containers a monitorear.
