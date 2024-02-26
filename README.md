# System Knowledge Graphs (SKGs) to Automata 

## Description

Module developed within the [Auto-Twin](https://www.auto-twin-project.eu/) Horizon EU project 
to extract data from Neo4j SKGs for automata learning purposes and to store
learned automata into the SKG.

## Requirements

Follow instructions for the [promg-core](https://github.com/Ava-S/promg-core) project to create the SKG.

## Configuration

The default configuration file is available in the [`./resources/config/`](resources/config) folder.

The module currently assumes the Neo4j instance to be running on [localhost:7687](http://localhost:7687): 
if this is not the case, update the [`config.ini`](resources/config/config.ini) file accordingly.

## How to use

Available queries are implemented as methods of the [`Skg_Reader`](skg_main/skg_mgrs/skg_reader.py) class:

- *get_events()*: Returns all Event nodes.
- *get_events_by_date(start_t, end_t)*: Return Event nodes filtered by timestamp. If both start_t and end_t are given, it returns events such that the timestamp is within range [start_t, end_t]. If only one parameter is specified, it returns events such that the timestamp is greater than start_t or smaller than end_t. If no parameter is specified, it returns all events.
- *get_entities()*: Returns all Entity nodes.
- *get_sensors()*: Returns all Class nodes.

