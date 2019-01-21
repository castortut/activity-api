# activity-api

Listens to messages published to an MQTT broker by motion sensors and provides an HTTP API to the data.


## Configuration

The main file app.py has two configuration variables near the top:

*HISTORY_LENGTH*

Determines how many values per sensor ID to keep in memory and return to clients.

*ALIASES*

Provides a mapping from the sensor IDs to more human friendly names for the sensors. 
The format is a dictionary with `id: alias`pairs


## TODO

- Log the values into a database instead of into memory in order to not loose data on restarts

- Make the aliases configurable in a database instead of hard coding and requiring a new deployment to change
