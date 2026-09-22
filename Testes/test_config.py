from lib.config import (
    get_postgres_config,
    get_sqlserverprotheus_config,
)


postgres = get_postgres_config()
sqlserver = get_sqlserverprotheus_config()


print("PostgreSQL")
print(f"Host: {postgres['host']}")
print(f"Porta: {postgres['port']}")
print(f"Database: {postgres['database']}")
print(f"Usuário: {postgres['user']}")
print("Senha: [PROTEGIDA]")


print("\nSQL Server Protheus")
print(f"Host: {sqlserver['host']}")
print(f"Porta: {sqlserver['port']}")
print(f"Database: {sqlserver['database']}")
print(f"Usuário: {sqlserver['user']}")
print("Senha: [PROTEGIDA]")
