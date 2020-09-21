import datetime
import json
import os

from flask import Flask, request
from flask_mqtt import Mqtt

import psycopg2

########
# Conf #
########

HISTORY_LENGTH = 10

MQTT_URL = os.environ.get("MQTT_URL", "mqtt.svc.cave.avaruuskerho.fi")
DB_URL = os.environ.get("DB_URL", "postgres://postgres@localhost:5432")

############
# Conf end #
############

LOG_LEVELS = {
    1: "INFO",
    2: "NOTICE",
    4: "WARNING",
    8: "ERROR",
    16: "DEBUG"
}

# Dictionary that the activity gets stored in
activity = {}

app = Flask(__name__)
app.config['MQTT_BROKER_URL'] = MQTT_URL
app.config['MQTT_BROKER_PORT'] = 1883  # default port for non-tls connection
app.config['MQTT_USERNAME'] = None  # set the username here if you need authentication for the broker
app.config['MQTT_PASSWORD'] = None  # set the password here if the broker demands authentication
app.config['MQTT_KEEPALIVE'] = 5  # set the time interval for sending a ping to the broker to 5 seconds
app.config['MQTT_TLS_ENABLED'] = False  # set TLS to disabled for testing purposes

mqtt = Mqtt(app)

conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()


def add_activity(id: str) -> None:
    """
    Record activity from the given sensor with the current date/time

    :param id: ID of the sensor that registered activity
    """

    cursor.execute("INSERT INTO sensor_events (sensor, date) VALUES (%s, %s);",
                   (id, datetime.datetime.utcnow()))
    conn.commit()


@mqtt.on_connect()
def handle_connect(client, userdata, flags, rc) -> None:
    """
    Handle an event when the MQTT connection is made. Subscribe to topic in this event so that
    if the connection is lost and reconnects, the subscriptions get made again
    """
    print("Connected to MQTT")

    # Subscribe to all topics that begin with '/iot/cave/motion0/'
    mqtt.subscribe('/iot/cave/motion0/*')


@mqtt.on_message()
def handle_mqtt_message(client, userdata, message) -> None:
    """
    Handle an event where a message is published to one of the topics we are subscribed to.
    Since we're (only) subscribed to the motion events, this means that motion has been registered somewhere.

    :param message: An object containing information (including the topic and the payload) of the message
    """

    # The last part of the topic is the sensor ID, like /iot/cave/motion0/123456
    id = message.topic.split('/')[-1]

    add_activity(id)
    print("Logged activity on sensor {}".format(id))


@mqtt.on_log()
def handle_logging(client, userdata, level, buf) -> None:
    """
    Handle an event where the MQTT library wants to log a message. Ignore any DEBUG-level messages

    :param level: The level/severity of the message
    :param buf: Message contents
    """
    if LOG_LEVELS[level] != 'DEBUG':
        print(f"{LOG_LEVELS[level]}: {buf}")


def get_sensors():
    """
    Get the list of known sensors from the database

    :return: list of sensors
    """
    cursor.execute("SELECT DISTINCT sensor FROM sensor_events;")
    return [row[0] for row in cursor.fetchall()]


def get_history(sensor):
    """
    Get last $HISTORY_LENGTH values from the given sensor

    :param sensor: id of the sensor to query
    :return: list of timestamps
    """
    cursor.execute("SELECT date FROM sensor_events "
                   "WHERE sensor = '%s' "
                   "ORDER BY date DESC LIMIT %s;", (sensor, HISTORY_LENGTH))
    return [row[0].replace(tzinfo=datetime.timezone.utc).isoformat() for row in cursor.fetchall()]

def get_history_ranges():
    query = """
        WITH differences AS (
            SELECT 
                sensor, 
                alias,
                date, 
                LAG(date, 1) OVER ( 
                    PARTITION BY sensor 
                    ORDER BY date ASC) previous_date,
                LAG(date, 1) OVER ( 
                    PARTITION BY sensor 
                    ORDER BY date DESC) next_date 
            FROM sensor_events JOIN sensor_aliases ON sensor_events.sensor = sensor_aliases.id
            WHERE date BETWEEN NOW() - INTERVAL '48h' AND NOW()) 

        ( 
            SELECT 
                sensor, 
                alias,
                date, 
                'START' as type 
            FROM differences 
            WHERE date - previous_date > INTERVAL '1h' OR previous_date IS NULL
        ) UNION (
            SELECT 
                sensor, 
                alias,
                date, 
                'END' as type 
            FROM differences 
            WHERE next_date - date > INTERVAL '1h' OR next_date IS NULL
        )
        ORDER BY date ASC, type DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def get_alias(sensor):
    """
    Get the alias of a sensor, if known

    :param sensor: id of the sensor
    :return: sensor alias
    """
    cursor.execute("SELECT alias FROM sensor_aliases "
                   "WHERE id = '%s';", (sensor,))
    result = cursor.fetchone()
    return result[0] if result else None


@app.route("/")
def view_activity():
    """
    A Flask route that responds to requests on the URL '/'. Builds an JSON object from the stored data.
    """

    response = []

    for sensor in get_sensors():
        history = get_history(sensor)
        alias = get_alias(sensor)

        response.append({
            "id": sensor,
            "alias": alias,
            "history": history,
            "latest": history[0],
        })

    if 'pretty' in request.args.keys():
        return json.dumps(response, sort_keys=True, indent=4, separators=(',', ': '))
    elif 'prettyhtml' in request.args.keys():
        return json.dumps(response, sort_keys=True, indent=4, separators=(',', ': ')).replace('\n', '<br>')
    else:
        return json.dumps(response)

@app.route("/history/v1")
@app.route("/history")
def view_history():
    sensors = {}

    for sensor, alias, date, event in get_history_ranges():
        if sensor not in sensors.keys():
            sensors[sensor] = {'id': sensor, 'alias': alias, 'events': []}

        date = date.replace(tzinfo=datetime.timezone.utc).isoformat()
        sensors[sensor]['events'].append(
            {'date': date, 'event': event}
        )

    response = list(sensors.values())

    if 'pretty' in request.args.keys():
        return json.dumps(response, sort_keys=True, indent=4, separators=(',', ': '))
    elif 'prettyhtml' in request.args.keys():
        return json.dumps(response, sort_keys=True, indent=4, separators=(',', ': ')).replace('\n', '<br>')
    else:
        return json.dumps(response)


if __name__ == '__main__':
    # Finally start the app
    app.run(host='0.0.0.0')
