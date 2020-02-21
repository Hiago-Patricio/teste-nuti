import psycopg2

def getConnection() -> object:
    try:
        connection = psycopg2.connect(user = "computabol",
                                    password = "computabol",
                                    host = "200.129.242.4",
                                    port = "35432",
                                    database = "futebol")
        # print('Connection successful')
        return connection
    except:
        # print('Connection failed')
        return None
    

def writeInFile(name: str, content: list) -> bool:
    with open(name, 'w') as file:
        file.writelines(content)
        return True
    return False


def insertInClassificacao(connection):
    query = '''
        SELECT id_campeonato, id_time, SUM(pontos) AS pontos
        FROM (

            -- Seleciona as partidas nas quais o time 1 venceu
            SELECT id_campeonato, id_time1 AS id_time, (COUNT(*) * 3) AS pontos
            FROM partida
            WHERE gols1 > gols2
            GROUP BY (id_campeonato, id_time)

            UNION ALL

            -- Seleciona as partidas nas quais o time 2 venceu
            SELECT id_campeonato, id_time2 AS id_time, (COUNT(*) * 3) AS pontos
            FROM partida
            WHERE gols2 > gols1
            GROUP BY (id_campeonato, id_time)

            UNION ALL

            -- Seleciona as partidas nas quais o time 1 empatou
            SELECT id_campeonato, id_time1 AS id_time, COUNT(*) AS pontos
            FROM partida
            WHERE gols1 = gols2
            GROUP BY (id_campeonato, id_time)

            UNION ALL

            -- Seleciona as partidas nas quais o time 2 empatou
            SELECT id_campeonato, id_time2 AS id_time, COUNT(*) AS pontos
            FROM partida
            WHERE gols1 = gols2
            GROUP BY (id_campeonato, id_time)
        ) s
        GROUP BY(id_campeonato, id_time)
        ORDER BY(pontos, id_campeonato, id_time)
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = list(cursor.fetchall())
        stmt = "INSERT INTO classificacao (id_campeonato, id_time, pontos) VALUES ({}, {}, {});\n"
        query_list = []
        for id_campeonato, id_time, pontos in result:
            query = stmt.format(id_campeonato, id_time, pontos)
            query_list.append(query)
        writeInFile('insert.sql', query_list)
    except:
        pass

if __name__ == '__main__':
    try:
        connection = getConnection()
        insertInClassificacao(connection)
    except:
        print('Failed')
    finally:
        if(connection):
            connection.close()
