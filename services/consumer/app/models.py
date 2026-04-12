from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    broker: str
    topic: str
    group_id: str


class PostgresConfig(BaseModel):
    url: str


class PrometheusConfig(BaseModel):
    port: int = 8001


class Settings(BaseSettings):
    kafka: KafkaConfig
    postgres: PostgresConfig
    prometheus: PrometheusConfig

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="_"
    )

settings = Settings() # type: ignore