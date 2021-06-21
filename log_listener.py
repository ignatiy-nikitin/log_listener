import psycopg2  # PostgreSQL adapter for Python
import asyncio
import configparser
import json

# Create a config.ini file having database credentials and the port number.
CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")

TCP_PORT = int(CONFIG["RSYSLOG"]["FORWARD_PORT"])

try:
    credentials = {
        "host": CONFIG["POSTGRESQL"]["DB_HOST"],
        "port": int(CONFIG["POSTGRESQL"]["DB_PORT"]),
        "database": CONFIG["POSTGRESQL"]["DB_NAME"],
        "user": CONFIG["POSTGRESQL"]["DB_USER"],
        "password": CONFIG["POSTGRESQL"]["DB_PASSWORD"]
    }
    CONN = psycopg2.connect(**credentials)
    # CURSOR = CONN.cursor()
except:
    print("Unable to connect to the database")
    quit()

# INSERT_QUERY = "INSERT INTO access_log (log_line, created_at) VALUES (%s, %s);"

INSERT_QUERY = "INSERT INTO accesslog \
    (ip, time, request, request_url, \
        method, request_header, request_body, \
            user_agent, user_id_got, user_id_set, \
                remote_user, status, response_header, response_body, \
                    body_bytes_sent, request_time, http_referer) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s);"


def execute_sql(params):
    try:
        CURSOR = CONN.cursor()
        CURSOR.execute(INSERT_QUERY, params)
        CURSOR.close()
        CONN.commit()
    except Exception as e:
        CONN.rollback()
        # raise e


class LogAnalyser:

    def __init__(self):
        pass

    def process(self, str_input):
        str_input = str_input.decode("utf-8", errors="ignore")
        # Add your processing steps here
        # ...
        try:
            # Extract created_at from the log string
            str_splits = str_input.split("{", 1)
            json_text = "{" + str_splits[1]
            data = json.loads(json_text)

            ip = data['ip']
            time = data['time']
            request = data['request']
            request_url = data['path']
            method = request.split()[0]
            request_header = data['req_header']
            request_body = data['request_body'] if data['request_body'] else '{}'
            user_agent = data['user_agent']
            user_id_got = data['user_id_got']
            user_id_set = data['user_id_set']
            remote_user = data['remote_user']
            status = int(data['status'])
            response_header = data['resp_header']
            response_body = data['response_body'] if data['response_body'] else '{}'
            body_bytes_sent = float(data['body_bytes_sent'])
            request_time = float(data['request_time'])
            http_referer = data['http_referrer']
            
            return ip, time, request, request_url, method, request_header, request_body, user_agent, user_id_got, user_id_set, remote_user, status, response_header, response_body, body_bytes_sent, request_time, http_referer
        except Exception as e:
            print(e)
        return None


@asyncio.coroutine
def handle_echo(reader, writer):
    log_filter = LogAnalyser()
    print('HERE')
    while True:
        line = yield from reader.readline()
        if not line:
            break
        params = log_filter.process(line)
        if params:
            execute_sql(params=params)


loop = asyncio.get_event_loop()
coro = asyncio.start_server(handle_echo, '127.0.0.1', TCP_PORT, loop=loop)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

# Close the server
print("Closing the server.")
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
CURSOR.close()
CONN.close()
