import json
import sys
from pathlib import Path


def generate_docker_compose(
    gateway,
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
    groupby_id_actor_count,
    groupby_id_title_mean_rating,
    divider,
    sentiment,
    sink_1,
    sink_2,
    sink_3,
    sink_4,
    sink_5,
    minmax_rating,
    top_10_count,
    top_5_budget,
    join_id_movieid,
    join_id_id
):
    docker_compose = """services:
  client:
    container_name: client
    build: client
    env_file: client/.env
    volumes:
      - .data:/data
    depends_on:
      - gateway
      
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:management
    ports:
      - 15672:15672
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 50s

  gateway:
    container_name: gateway
    build: gateway
    ports:
      - 9090:9090
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - envs/gateway/.env.gateway
"""
    for i in range(sanitize_movies):
        docker_compose += f"""
  #### Sanitizers ####
  sanitize-movies-{i}:
    container_name: sanitize-movies-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sanitize.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sanitize/.env.movies
    environment:
      - ID={i}
      - OUTPUT_COPIES={divider},{filter_production_countries_length},{filter_release_date_since_2000}
"""

    for i in range(sanitize_credits):
        docker_compose += f"""
  sanitize-credits-{i}:
    container_name: sanitize-credits-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sanitize.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sanitize/.env.credits
    environment:
      - ID={i}
      - OUTPUT_COPIES={join_id_id}
"""

    for i in range(sanitize_ratings):
        docker_compose += f"""
  sanitize-ratings-{i}:
    container_name: sanitize-ratings-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sanitize.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sanitize/.env.ratings
    environment:
      - ID={i}
      - OUTPUT_COPIES={join_id_movieid}
"""

    for i in range(filter_production_countries_length):
        docker_compose += f"""
  #### Filters ####
  filter-production_countries_length-{i}:
    container_name: filter-production_countries_length-{i}
    build:
      context: workers
      dockerfile: dockerfiles/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/filter/.env.production_countries_length
    environment:
      - ID={i}
      - OUTPUT_COPIES={explode_production_countries}
"""

    for i in range(filter_release_date_since_2000):
        docker_compose += f"""
  filter-release_date_since_2000-{i}:
    container_name: filter-release_date_since_2000-{i}
    build:
      context: workers
      dockerfile: dockerfiles/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/filter/.env.release_date_since_2000
    environment:
      - ID={i}
      - OUTPUT_COPIES={join_id_id},{filter_production_countries_argentina_spain},{filter_production_countries_argentina}
"""

    for i in range(filter_release_date_upto_2010):
        docker_compose += f"""
  filter-release_date_upto_2010-{i}:
    container_name: filter-release_date_upto_2010-{i}
    build:
      context: workers
      dockerfile: dockerfiles/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/filter/.env.release_date_upto_2010
    environment:
      - ID={i}
      - OUTPUT_COPIES={sink_1}
"""

    for i in range(filter_production_countries_argentina):
        docker_compose += f"""
  filter-production_countries_argentina-{i}:
    container_name: filter-production_countries_argentina-{i}
    build:
      context: workers
      dockerfile: dockerfiles/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/filter/.env.production_countries_argentina
    environment:
      - ID={i}
      - OUTPUT_COPIES={join_id_movieid}
"""

    for i in range(filter_production_countries_argentina_spain):
        docker_compose += f"""
  filter-production_countries_argentina_spain-{i}:
    container_name: filter-production_countries_argentina_spain-{i}
    build:
      context: workers
      dockerfile: dockerfiles/filter.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/filter/.env.production_countries_argentina_spain
    environment:
      - ID={i}
      - OUTPUT_COPIES={filter_release_date_upto_2010}
"""
    for i in range(explode_cast):
        docker_compose += f"""
  #### Exploders ####
  explode-cast-{i}:
    container_name: explode-cast-{i}
    build:
      context: workers
      dockerfile: dockerfiles/explode.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/explode/.env.cast
    environment:
      - ID={i}
      - OUTPUT_COPIES={groupby_id_actor_count}
"""
    for i in range(explode_production_countries):
        docker_compose += f"""
  explode-production_countries-{i}:
    container_name: explode-production_countries-{i}
    build:
      context: workers
      dockerfile: dockerfiles/explode.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/explode/.env.production_countries
    environment:
      - ID={i}
      - OUTPUT_COPIES={groupby_country_sum_budget}
"""
    for i in range(groupby_id_title_mean_rating):
        docker_compose += f"""
  #### Group by ####
  groupby-id_title_mean_rating-{i}:
    container_name: groupby-id_title_mean_rating-{i}
    build:
      context: workers
      dockerfile: dockerfiles/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/groupby/.env.id_title_mean_rating
    environment:
      - ID={i}
      - OUTPUT_COPIES={minmax_rating}
"""
    for i in range(groupby_sentiment_mean_rate_revenue_budget):
        docker_compose += f"""
  groupby-sentiment_mean_rate_revenue_budget-{i}:
    container_name: groupby-sentiment_mean_rate_revenue_budget-{i}
    build:
      context: workers
      dockerfile: dockerfiles/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/groupby/.env.sentiment_mean_rate_revenue_budget
    environment:
      - ID={i}
      - OUTPUT_COPIES={sink_5}
"""
    for i in range(groupby_country_sum_budget):
        docker_compose += f"""
  groupby-country_sum_budget-{i}:
    container_name: groupby-country_sum_budget-{i}
    build:
      context: workers
      dockerfile: dockerfiles/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/groupby/.env.country_sum_budget
    environment:
      - ID={i}
      - OUTPUT_COPIES={top_5_budget}
"""
    for i in range(groupby_id_actor_count):
        docker_compose += f"""
  groupby-id_actor_count-{i}:
    container_name: groupby-id_actor_count-{i}
    build:
      context: workers
      dockerfile: dockerfiles/groupby.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/groupby/.env.id_actor_count
    environment:
      - ID={i}
      - OUTPUT_COPIES={top_10_count}
"""
    for i in range(divider):
        docker_compose += f"""
  #### Divider ####
  divider-{i}:
    container_name: divider-{i}
    build:
      context: workers
      dockerfile: dockerfiles/divider.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/divider/.env.revenue_budget
    environment:
      - ID={i}
      - OUTPUT_COPIES={sentiment}
"""
    for i in range(sentiment):
        docker_compose += f"""
  #### Sentiment ####
  sentiment-{i}:
    container_name: sentiment-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sentiment.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sentiment/.env.overview
    environment:
      - ID={i}
      - OUTPUT_COPIES={groupby_sentiment_mean_rate_revenue_budget}
"""
    for i in range(top_10_count):
        docker_compose += f"""
  #### Tops #### 
  top-10_count-{i}:
    container_name: top-10_count-{i}
    build:
      context: workers
      dockerfile: dockerfiles/top.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/top/.env.10_count
    environment:
      - ID={i}
      - OUTPUT_COPIES={sink_4}
"""
    for i in range(top_5_budget):
        docker_compose += f"""
  top-5_budget-{i}:
    container_name: top-5_budget-{i}
    build:
      context: workers
      dockerfile: dockerfiles/top.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/top/.env.5_budget
    environment:
      - ID={i}
      - OUTPUT_COPIES={sink_2}
"""
    for i in range(minmax_rating):
        docker_compose += f"""
  #### Minmax ####
  minmax-rating-{i}:
    container_name: minmax-rating-{i}
    build:
      context: workers
      dockerfile: dockerfiles/minmax.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/minmax/.env.rating
    environment:
      - ID={i}
      - OUTPUT_COPIES={sink_3}
"""
    for i in range(join_id_movieid):
        docker_compose += f"""
  #### Joins ####
  join-id_movieid-{i}:
    container_name: join-id_movieid-{i}
    build:
      context: workers
      dockerfile: dockerfiles/join.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/join/.env.id_movieId
    environment:
      - ID={i}
      - OUTPUT_COPIES={groupby_id_title_mean_rating}
"""
    for i in range(join_id_id):
        docker_compose += f"""
  join-id_id-{i}:
    container_name: join-id_id-{i}
    build:
      context: workers
      dockerfile: dockerfiles/join.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/join/.env.id_id
    environment:
      - ID={i}
      - OUTPUT_COPIES={explode_cast}
"""
    for i in range(sink_1):
        docker_compose += f"""
  #### Sinks ####
  sink-1-{i}:
    container_name: sink-1-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sink.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sink/.env.1
    environment:
      - ID={i}
      - OUTPUT_COPIES={gateway}
"""
    for i in range(sink_2):
        docker_compose += f"""
  sink-2-{i}:
    container_name: sink-2-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sink.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sink/.env.2
    environment:
      - ID={i}
      - OUTPUT_COPIES={gateway}
"""
    for i in range(sink_3):
        docker_compose += f"""
  sink-3-{i}:
    container_name: sink-3-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sink.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sink/.env.3
    environment:
      - ID={i}
      - OUTPUT_COPIES={gateway}
"""
    for i in range(sink_4):
        docker_compose += f"""
  sink-4-{i}:
    container_name: sink-4-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sink.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sink/.env.4
    environment:
      - ID={i}
      - OUTPUT_COPIES={gateway}
"""
    for i in range(sink_5):
        docker_compose += f"""
  sink-5-{i}:
    container_name: sink-5-{i}
    build:
      context: workers
      dockerfile: dockerfiles/sink.Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    env_file:
      - workers/.env
      - envs/sink/.env.5
    environment:
      - ID={i}
      - OUTPUT_COPIES={gateway}
"""

    return docker_compose


if __name__ == "__main__":
    config_path = Path("generate_compose_config.json")

    if not config_path.exists():
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)

    with open(config_path, "r") as config_file:
        config = json.load(config_file)

    try:
        docker_compose = generate_docker_compose(**config)
    except KeyError as e:
        print(f"Missing configuration key: {e}")
        sys.exit(1)

    compose_name = "compose.yaml"

    with open(compose_name, "w") as f:
        f.write(docker_compose)

    print(f"Docker Compose configuration saved to {compose_name}")
