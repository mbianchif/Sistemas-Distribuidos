import json
import sys
import math
import random
from pathlib import Path

PIPELINE_COMPOSE_FILE_NAME = "compose.yaml"
CLIENTS_COMPOSE_FILE_NAME = "compose.clients.yaml"
CHECKERS_COMPOSE_FILE_NAME = "compose.checkers.yaml"

# unscalable
GATEWAY = 1
TOP_10_COUNT = 1
TOP_5_BUDGET = 1
MINMAX_RATING = 1


def generate_pipeline_compose(
    sanitize_movies,
    sanitize_credits,
    sanitize_ratings,
    filter_production_countries_length,
    filter_production_countries_argentina_spain,
    filter_production_countries_argentina,
    filter_release_date_since_2000,
    filter_release_date_upto_2010,
    explode_production_countries,
    explode_cast,
    groupby_sentiment_mean_rate_revenue_budget,
    groupby_country_sum_budget,
    groupby_actor_count,
    groupby_id_title_mean_rating,
    divider,
    sentiment,
    sink_1,
    sink_2,
    sink_3,
    sink_4,
    sink_5,
    join_id_movieid,
    join_id_id,
) -> str:
    docker_compose = "name: moviesanalyzer"
    docker_compose += f"""
services:
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:management
    networks:
      - my-network
    ports:
      - 15672:15672
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 50s
    deploy:
      resources:
        limits:
          memory: 1g

  gateway:
    container_name: gateway
    build:
      dockerfile: build/gateway.Dockerfile
    networks:
      - my-network
    ports:
      - 9090:9090
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/gateway/.env
    environment:
      - ID=0
      - INPUT_COPIES={sink_1},{sink_2},{sink_3},{sink_4},{sink_5}
      - OUTPUT_COPIES={sanitize_movies},{sanitize_credits},{sanitize_ratings}
"""

    docker_compose += "\n# ======================= Sanitizers =======================\n"
    for i in range(sanitize_movies):
        docker_compose += f"""
  sanitize-movies-{i}:
    container_name: sanitize-movies-{i}
    build:
      dockerfile: build/sanitize.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sanitize/.env.movies
    environment:
      - ID={i}
      - INPUT_COPIES={GATEWAY}
      - OUTPUT_COPIES={divider},{filter_production_countries_length},{filter_release_date_since_2000}
"""

    for i in range(sanitize_credits):
        docker_compose += f"""
  sanitize-credits-{i}:
    container_name: sanitize-credits-{i}
    build:
      dockerfile: build/sanitize.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sanitize/.env.credits
    environment:
      - ID={i}
      - INPUT_COPIES={GATEWAY}
      - OUTPUT_COPIES={join_id_id}
"""

    for i in range(sanitize_ratings):
        docker_compose += f"""
  sanitize-ratings-{i}:
    container_name: sanitize-ratings-{i}
    build:
      dockerfile: build/sanitize.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sanitize/.env.ratings
    environment:
      - ID={i}
      - INPUT_COPIES={GATEWAY}
      - OUTPUT_COPIES={join_id_movieid}
"""

    docker_compose += "\n# ======================= Filters =======================\n"
    for i in range(filter_production_countries_length):
        docker_compose += f"""
  filter-production_countries_length-{i}:
    container_name: filter-production_countries_length-{i}
    build:
      dockerfile: build/filter.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/filter/.env.production_countries_length
    environment:
      - ID={i}
      - INPUT_COPIES={sanitize_movies}
      - OUTPUT_COPIES={explode_production_countries}
"""

    for i in range(filter_release_date_since_2000):
        docker_compose += f"""
  filter-release_date_since_2000-{i}:
    container_name: filter-release_date_since_2000-{i}
    build:
      dockerfile: build/filter.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/filter/.env.release_date_since_2000
    environment:
      - ID={i}
      - INPUT_COPIES={sanitize_movies}
      - OUTPUT_COPIES={filter_production_countries_argentina_spain},{filter_production_countries_argentina}
"""

    for i in range(filter_release_date_upto_2010):
        docker_compose += f"""
  filter-release_date_upto_2010-{i}:
    container_name: filter-release_date_upto_2010-{i}
    build:
      dockerfile: build/filter.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/filter/.env.release_date_upto_2010
    environment:
      - ID={i}
      - INPUT_COPIES={filter_production_countries_argentina_spain}
      - OUTPUT_COPIES={sink_1}
"""

    for i in range(filter_production_countries_argentina):
        docker_compose += f"""
  filter-production_countries_argentina-{i}:
    container_name: filter-production_countries_argentina-{i}
    build:
      dockerfile: build/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/filter/.env.production_countries_argentina
    environment:
      - ID={i}
      - INPUT_COPIES={filter_release_date_since_2000}
      - OUTPUT_COPIES={join_id_id},{join_id_movieid}
"""

    for i in range(filter_production_countries_argentina_spain):
        docker_compose += f"""
  filter-production_countries_argentina_spain-{i}:
    container_name: filter-production_countries_argentina_spain-{i}
    build:
      dockerfile: build/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/filter/.env.production_countries_argentina_spain
    environment:
      - ID={i}
      - INPUT_COPIES={filter_release_date_since_2000}
      - OUTPUT_COPIES={filter_release_date_upto_2010}
"""

    docker_compose += "\n# ======================= Explodes =======================\n"
    for i in range(explode_cast):
        docker_compose += f"""
  explode-cast-{i}:
    container_name: explode-cast-{i}
    build:
      dockerfile: build/explode.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/explode/.env.cast
    environment:
      - ID={i}
      - INPUT_COPIES={join_id_id}
      - OUTPUT_COPIES={groupby_actor_count}
"""

    for i in range(explode_production_countries):
        docker_compose += f"""
  explode-production_countries-{i}:
    container_name: explode-production_countries-{i}
    build:
      dockerfile: build/explode.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/explode/.env.production_countries
    environment:
      - ID={i}
      - INPUT_COPIES={filter_production_countries_length}
      - OUTPUT_COPIES={groupby_country_sum_budget}
"""

    docker_compose += "\n# ======================= GroupBys =======================\n"
    for i in range(groupby_id_title_mean_rating):
        docker_compose += f"""
  groupby-id_title_mean_rating-{i}:
    container_name: groupby-id_title_mean_rating-{i}
    build:
      dockerfile: build/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/groupby/.env.id_title_mean_rating
    environment:
      - ID={i}
      - INPUT_COPIES={join_id_movieid}
      - OUTPUT_COPIES={MINMAX_RATING}
"""

    for i in range(groupby_sentiment_mean_rate_revenue_budget):
        docker_compose += f"""
  groupby-sentiment_mean_rate_revenue_budget-{i}:
    container_name: groupby-sentiment_mean_rate_revenue_budget-{i}
    build:
      dockerfile: build/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/groupby/.env.sentiment_mean_rate_revenue_budget
    environment:
      - ID={i}
      - INPUT_COPIES={sentiment}
      - OUTPUT_COPIES={sink_5}
"""

    for i in range(groupby_country_sum_budget):
        docker_compose += f"""
  groupby-country_sum_budget-{i}:
    container_name: groupby-country_sum_budget-{i}
    build:
      dockerfile: build/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/groupby/.env.country_sum_budget
    environment:
      - ID={i}
      - INPUT_COPIES={explode_production_countries}
      - OUTPUT_COPIES={TOP_5_BUDGET}
"""

    for i in range(groupby_actor_count):
        docker_compose += f"""
  groupby-actor_count-{i}:
    container_name: groupby-actor_count-{i}
    build:
      dockerfile: build/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/groupby/.env.actor_count
    environment:
      - ID={i}
      - INPUT_COPIES={explode_cast}
      - OUTPUT_COPIES={TOP_10_COUNT}
"""

    docker_compose += "\n# ======================= Dividers =======================\n"
    for i in range(divider):
        docker_compose += f"""
  divider-{i}:
    container_name: divider-{i}
    build:
      dockerfile: build/divider.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/divider/.env.revenue_budget
    environment:
      - ID={i}
      - INPUT_COPIES={sanitize_movies}
      - OUTPUT_COPIES={sentiment}
"""

    docker_compose += "\n# ======================= Sentiments =======================\n"
    for i in range(sentiment):
        docker_compose += f"""
  sentiment-{i}:
    container_name: sentiment-{i}
    build:
      dockerfile: build/sentiment.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - my-network
    env_file:
      - configs/workers/.env
      - configs/workers/sentiment/.env.overview
    environment:
      - ID={i}
      - INPUT_COPIES={divider}
      - OUTPUT_COPIES={groupby_sentiment_mean_rate_revenue_budget}
"""

    docker_compose += "\n# ======================= Tops =======================\n"
    for i in range(TOP_10_COUNT):
        docker_compose += f"""
  top-10_count-{i}:
    container_name: top-10_count-{i}
    build:
      dockerfile: build/top.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/top/.env.10_count
    environment:
      - ID={i}
      - INPUT_COPIES={groupby_actor_count}
      - OUTPUT_COPIES={sink_4}
"""

    for i in range(TOP_5_BUDGET):
        docker_compose += f"""
  top-5_budget-{i}:
    container_name: top-5_budget-{i}
    build:
      dockerfile: build/top.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/top/.env.5_budget
    environment:
      - ID={i}
      - INPUT_COPIES={groupby_country_sum_budget}
      - OUTPUT_COPIES={sink_2}
"""

    docker_compose += "\n# ======================= MinMax =======================\n"
    for i in range(MINMAX_RATING):
        docker_compose += f"""
  minmax-rating-{i}:
    container_name: minmax-rating-{i}
    build:
      dockerfile: build/minmax.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/minmax/.env.rating
    environment:
      - ID={i}
      - INPUT_COPIES={groupby_id_title_mean_rating}
      - OUTPUT_COPIES={sink_3}
"""

    docker_compose += "\n# ======================= Joins =======================\n"
    for i in range(join_id_movieid):
        docker_compose += f"""
  join-id_movieid-{i}:
    container_name: join-id_movieid-{i}
    build:
      dockerfile: build/join.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/join/.env.id_movieId
    environment:
      - ID={i}
      - INPUT_COPIES={filter_production_countries_argentina},{sanitize_ratings}
      - OUTPUT_COPIES={groupby_id_title_mean_rating}
"""

    for i in range(join_id_id):
        docker_compose += f"""
  join-id_id-{i}:
    container_name: join-id_id-{i}
    build:
      dockerfile: build/join.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/join/.env.id_id
    environment:
      - ID={i}
      - INPUT_COPIES={filter_production_countries_argentina},{sanitize_credits}
      - OUTPUT_COPIES={explode_cast}
"""

    docker_compose += "\n# ======================= Sinks =======================\n"
    for i in range(sink_1):
        docker_compose += f"""
  sink-1-{i}:
    container_name: sink-1-{i}
    build:
      dockerfile: build/sink.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sink/.env.1
    environment:
      - ID={i}
      - INPUT_COPIES={filter_release_date_upto_2010}
      - OUTPUT_COPIES={GATEWAY}
"""

    for i in range(sink_2):
        docker_compose += f"""
  sink-2-{i}:
    container_name: sink-2-{i}
    build:
      dockerfile: build/sink.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sink/.env.2
    environment:
      - ID={i}
      - INPUT_COPIES={TOP_5_BUDGET}
      - OUTPUT_COPIES={GATEWAY}
"""

    for i in range(sink_3):
        docker_compose += f"""
  sink-3-{i}:
    container_name: sink-3-{i}
    build:
      dockerfile: build/sink.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sink/.env.3
    environment:
      - ID={i}
      - INPUT_COPIES={MINMAX_RATING}
      - OUTPUT_COPIES={GATEWAY}
"""

    for i in range(sink_4):
        docker_compose += f"""
  sink-4-{i}:
    container_name: sink-4-{i}
    build:
      dockerfile: build/sink.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sink/.env.4
    environment:
      - ID={i}
      - INPUT_COPIES={TOP_10_COUNT}
      - OUTPUT_COPIES={GATEWAY}
"""

    for i in range(sink_5):
        docker_compose += f"""
  sink-5-{i}:
    container_name: sink-5-{i}
    build:
      dockerfile: build/sink.Dockerfile
    networks:
      - my-network
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - configs/workers/.env
      - configs/workers/sink/.env.5
    environment:
      - ID={i}
      - INPUT_COPIES={groupby_sentiment_mean_rate_revenue_budget}
      - OUTPUT_COPIES={GATEWAY}
"""

    docker_compose += """
networks:
  my-network:
    name: moviesanalyzer_net
"""

    return docker_compose


def generate_clients_compose(client) -> str:
    docker_compose = f"""name: clients
services:"""

    for i in range(client):
        docker_compose += f"""
  client-{i}:
    container_name: client-{i}
    build:
      dockerfile: build/client.Dockerfile
    volumes:
      - .data:/data
    networks:
      - my-network
    env_file: configs/client/.env
"""
    docker_compose += """
networks:
  my-network:
    name: moviesanalyzer_net
    external: true
"""
    return docker_compose


def chunks(arr, r):
    for i in range(0, len(arr), r):
        bot = i
        top = min(i + r, len(arr))
        yield arr[bot:top]


def generate_checkers_compose(config: dict[str, int]):
    nodes = [
        "gateway-0",
        "top-10_count-0",
        "top-5_budget-0",
        "minmax-rating-0",
    ]

    for kind, replicas in config.items():
        if kind == "clients":
            continue

        name = kind.replace("_", "-", 1)
        for i in range(replicas):
            nodes.append(f"{name}-{i}")

    random.shuffle(nodes)
    ncheckers = math.ceil(len(nodes) / 10)
    partition_size = math.ceil(len(nodes) / ncheckers)
    watch_nodes_chunks = list(chunks(nodes, partition_size))

    docker_compose = f"""name: checkers
services:"""
    for i in range(ncheckers):
        docker_compose += f"""
  health-checker-{i}:
    container_name: healt-checker-{i}
    build:
      dockerfile: build/checker.Dockerfile
    networks:
      - my-network
    env_file: configs/checker/.env
    environment:
      - ID={i}
      - N={ncheckers}
      - CHECKER_COMPOSE_PATH={CHECKERS_COMPOSE_FILE_NAME}
      - HOST_FMT=health-checker-%d
      - WATCH_NODES={",".join(watch_nodes_chunks[i])}
"""
    docker_compose += """
networks:
  my-network:
    name: moviesanalyzer_net
    external: true
"""
    return docker_compose


if __name__ == "__main__":
    # pipeline
    config_path = Path("configs/compose/config.json")

    if not config_path.exists():
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)

    with open(config_path, "r") as config_file:
        config = json.load(config_file)
        clients = config.pop("clients", None)

    try:
        pipeline_docker_compose = generate_pipeline_compose(**config)
    except KeyError as e:
        print(f"Missing configuration key: {e}")
        sys.exit(1)

    with open(PIPELINE_COMPOSE_FILE_NAME, "w") as f:
        f.write(pipeline_docker_compose)

    print(f"Pipeline compose file saved to {PIPELINE_COMPOSE_FILE_NAME}")

    # clients
    docker_compose = generate_clients_compose(clients)
    with open(CLIENTS_COMPOSE_FILE_NAME, "w") as f:
        f.write(docker_compose)

    print(f"Clients compose file saved to {CLIENTS_COMPOSE_FILE_NAME}")

    # checkers
    docker_compose = generate_checkers_compose(config)
    with open(CHECKERS_COMPOSE_FILE_NAME, "w") as f:
        f.write(docker_compose)

    print(f"Checkers compose file saves to {CHECKERS_COMPOSE_FILE_NAME}")
