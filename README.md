# ME-ERTS_RTOS-Lab_Project_rtos-distributed-temperature-monitoring-system
RTOS-based distributed temperature monitoring system using ESP32, ESP8266, TI CC3220SF, Raspberry Pi, MQTT, Node-RED, and Python logging


**Final Software & Compatibility Stack**

**Sensor Nodes (ESP32 / ESP8266)**
  Arduino IDE: 2.3.2
  ESP32 Arduino Core (Espressif): 2.0.17
  ESP8266 Arduino Core: 3.1.2
  
**Libraries**
  PubSubClient (MQTT): 2.8
  OneWire: 2.3.8
  DallasTemperature: 3.11.0
  
**Hardware Interface**
  Sensor: DS18B20
  Protocol: OneWire
  Logic Level: 3.3V
  ESP32 DS18B20 GPIO: GPIO 27
  ESP8266 DS18B20 GPIO: GPIO 2 (D4)
  
  --------------------------------------------------------------------------------------------------------------------------
**TI CC3220SF Gateway**
  Board: TI CC3220SF LaunchXL
  IDE: Code Composer Studio 12.8.1
  SDK: SimpleLink CC32XX SDK 7.10.00.13
  RTOS: TI-RTOS7
  Compiler: TI ARM Clang 3.2.2.LTS
  
**Gateway Functions**
  MQTT subscriber
  packet validation & decoding
  threshold monitoring
  RTOS multitasking
  software timer timeout supervision
  node state management
  LED actuator control
  
  -----------------------------------------------------------------------------------------------------------------------
**Raspberry Pi Edge Server**
  Board: Raspberry Pi 4B
  OS: Raspberry Pi OS 64-bit (Debian 12 Bookworm)
  
**Services**
  Mosquitto MQTT Broker: 2.0.x
  mosquitto-clients: 2.0.x
  Node-RED: 4.0.9
  node-red-dashboard: 3.6.5
  Python: 3.11.x
  paho-mqtt: 2.1.0
  
**Edge Functions**
  MQTT broker hosting
  Node-RED dashboard visualization
  Python packet decoding
  CSV logging (log.csv)
  
  ------------------------------------------------------------------------------------------------------------------------
**Communication Stack**
  Protocol: MQTT v3.1.1
  Transport: TCP/IP
  Broker Port: 1883
  Security Mode: Non-TLS (local network deployment)
  
**MQTT Topics**
  sensor/node1/data
  sensor/node2/data
  sensor/node3/data
  
  -----------------------------------------------------------------------------------------------------------------------
**Monitoring Platforms**
  Dashboard Browser: Google Chrome (latest stable)
  
  -----------------------------------------------------------------------------------------------------------------------
**Project Repository Suggested Structure**
  /ESP32_Node1_Code
  /ESP32_Node2_Code
  /ESP8266_Node3_Code
  /TI_CC3220SF_Gateway_Code
  /RaspberryPi_Python_Logger
  /NodeRED_Flows
  /Documentation
