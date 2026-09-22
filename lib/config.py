from pathlib import Path


# Diretório raiz da configuração
BASE_DIR = Path("/srv/spark/scripts_spark/config")

CONNECTIONS_DIR = BASE_DIR / "connections"
SECRETS_DIR = BASE_DIR / "secrets"


def _load_env_file(file_path):
    """
    Carrega um arquivo no formato:
    
    CHAVE=VALOR
    
    Retorna um dicionário com as configurações.
    """
    if not file_path.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {file_path}"
        )

    config = {}

    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            # Ignora linhas vazias e comentários
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)

            config[key.strip()] = value.strip()

    return config


def get_postgres_config():
    """
    Retorna a configuração completa do PostgreSQL.
    """

    connection = _load_env_file(
        CONNECTIONS_DIR / "postgres.env"
    )

    secret = _load_env_file(
        SECRETS_DIR / "postgres.secret"
    )

    return {
        "host": connection["POSTGRES_HOST"],
        "port": int(connection["POSTGRES_PORT"]),
        "database": connection["POSTGRES_DATABASE"],
        "user": connection["POSTGRES_USER"],
        "password": secret["POSTGRES_PASSWORD"],
    }


def get_sqlserverprotheus_config():
    """
    Retorna a configuração completa do SQL Server Protheus.
    """

    connection = _load_env_file(
        CONNECTIONS_DIR / "sqlserverprotheus.env"
    )

    secret = _load_env_file(
        SECRETS_DIR / "sqlserverprotheus.secret"
    )

    return {
        "host": connection["SQLSERVER_HOST"],
        "port": int(connection["SQLSERVER_PORT"]),
        "database": connection["SQLSERVER_DATABASE"],
        "user": connection["SQLSERVER_USER"],
        "password": secret["SQLSERVER_PASSWORD"],
    }
