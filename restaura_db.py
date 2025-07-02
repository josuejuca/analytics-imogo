import sqlite3

# Arquivo com o dump SQL
arquivo_sql = 'backup_analytics.sql'

# Nome do banco SQLite que será criado ou sobrescrito
banco_sqlite = 'analytics.db'

def restaurar_backup():
    # Lê o conteúdo do arquivo SQL
    with open(arquivo_sql, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    # Conecta/cria o banco
    conn = sqlite3.connect(banco_sqlite)
    cursor = conn.cursor()
    
    try:
        # Executa o script todo (criação de tabelas, inserts, etc)
        cursor.executescript(sql_script)
        conn.commit()
        print("Backup restaurado com sucesso!")
    except sqlite3.Error as e:
        print(f"Erro ao restaurar backup: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    restaurar_backup()
